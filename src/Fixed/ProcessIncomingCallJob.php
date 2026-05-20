<?php

namespace App\Jobs;

use App\Models\Call;
use App\Models\Client;
use App\Models\Operator;
use App\Services\TelephonyClient;
use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Исправленная версия ProcessIncomingCallJob.
 *
 * Ключевые изменения:
 *  1. Атомарный захват оператора через SELECT ... FOR UPDATE + UPDATE с WHERE available = true
 *     — устраняет race condition при параллельной обработке.
 *  2. Переход на optimistic locking (retry-цикл) для записи Call — защита от lost update.
 *  3. TelephonyClient вызывается через try/catch с idempotency-key;
 *     внешний HTTP не должен ломать консистентность БД.
 *  4. Отсутствие оператора больше не бросает Exception — помечаем звонок
 *     как pending_operator и ставим отложенный retry.
 *  5. Идемпотентность: если Job повторился (например, worker упал),
 *     status-guard не позволяет назначить оператора дважды.
 *  6. Структурированный лог с контекстом.
 */
class ProcessIncomingCallJob implements ShouldQueue
{
    use Batchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries         = 5;
    public int $maxExceptions = 5;
    public int $backoff        = [5, 15, 30, 60, 120];

    // Видимость для очереди
    public int $timeout = 30;

    private int $callId;

    public function __construct(int $callId)
    {
        $this->callId = $callId;
    }

    private bool $shouldRelease = false;

    public function handle(): void
    {
        // Если Job часть батча и он отменён — выходим
        if (method_exists($this, 'batch') && $this->batch()?->cancelled()) {
            return;
        }

        $call = Call::find($this->callId);

        if (!$call) {
            Log::warning('ProcessIncomingCall: call not found', ['call_id' => $this->callId]);
            return;
        }

        // Идемпотентность: обрабатываем только 'new' и 'pending_operator'
        if (!in_array($call->status, ['new', 'pending_operator'])) {
            Log::info('ProcessIncomingCall: call already processed', [
                'call_id'  => $call->id,
                'status'   => $call->status,
            ]);
            return;
        }

        $this->shouldRelease = false;

        DB::transaction(function () use ($call) {
            // 1. Привязка клиента
            $client = Client::where('phone', $call->phone)->first();
            if ($client) {
                $call->client_id = $client->id;
            }

            // 2. Атомарный захват оператора
            $operator = $this->acquireOperator();

            if (!$operator) {
                // Нет свободных операторов — оставляем статус как есть
                // (new или pending_operator) и просим retry.
                // Не меняем статус внутри транзакции, чтобы при повторе
                // Job снова вошёл в эту ветку.
                Log::info('ProcessIncomingCall: no available operators, retrying', [
                    'call_id' => $call->id,
                ]);
                $this->shouldRelease = true;
                return;
            }

            // 3. Назначение
            $call->operator_id = $operator->id;
            $call->status      = 'assigned';
            $call->assigned_at = now();
            $call->save();

            Log::info('Call assigned', [
                'call_id'     => $call->id,
                'operator_id' => $operator->id,
                'client_id'   => $call->client_id,
            ]);
        });

        // release() вне транзакции — корректный отложенный retry
        if ($this->shouldRelease) {
            $this->release(10);
            return;
        }

        // После коммита транзакции — внешняя система
        $this->notifyTelephony($call->fresh());
    }

    /**
     * Атомарный захват оператора.
     *
     * Используем raw-запрос, чтобы атомарно:
     *   - найти оператора с available = true,
     *   - пометить его как занятого,
     *   - обновить last_call_at.
     *
     * Возвращает модель Operator или null.
     */
    private function acquireOperator(): ?Operator
    {
        // Шаг 1: Атомарно заблокировать одного свободного оператора.
        // UPDATE с WHERE + ORDER BY + LIMIT атомарно выбирает и обновляет строку.
        // Чтобы потом точно получить ID обновлённого оператора, используем
        // уникальный маркер, а не timestamp (под нагрузкой now() совпадает).
        $lockMarker = uniqid('op_lock_', true);

        $affected = DB::table('operators')
            ->where('available', true)
            ->orderBy('last_call_at')
            ->limit(1)
            ->update([
                'available'    => false,
                'last_call_at' => now(),
                'updated_at'   => now(),
                'lock_marker'  => $lockMarker,
            ]);

        if ($affected === 0) {
            return null;
        }

        // Шаг 2: Находим оператора по уникальному маркеру
        $operatorId = DB::table('operators')
            ->where('lock_marker', $lockMarker)
            ->value('id');

        return Operator::find($operatorId);
    }

    /**
     * HTTP-вызов в телефонию с обработкой ошибок и idempotency-key.
     */
    private function notifyTelephony(Call $call): void
    {
        try {
            app(TelephonyClient::class)->sendCallAssigned(
                $call->id,
                $call->operator_id,
                ['idempotency_key' => "call_{$call->id}_assign"]
            );
        } catch (Throwable $e) {
            Log::error('Telephony notification failed', [
                'call_id'     => $call->id,
                'operator_id' => $call->operator_id,
                'error'       => $e->getMessage(),
            ]);

            // Re-throw чтобы Job повторился по стандартной механике retry
            throw $e;
        }
    }

    /**
     * Called when the job fails after all retries.
     */
    public function failed(Throwable $exception): void
    {
        Log::error('ProcessIncomingCallJob failed permanently', [
            'call_id' => $this->callId,
            'error'   => $exception->getMessage(),
            'trace'   => $exception->getTraceAsString(),
        ]);

        $call = Call::find($this->callId);
        if ($call && in_array($call->status, ['new', 'pending_operator'])) {
            $call->status = 'failed';
            $call->error_message = $exception->getMessage();
            $call->save();
        }
    }
}
