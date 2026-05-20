<?php

/**
 * Альтернативная версия Job с Redis-пулом операторов.
 * Для сценариев с высокой нагрузкой (50x рост).
 */

namespace App\Jobs;

use App\Models\Call;
use App\Models\Client;
use App\Models\Operator;
use App\Services\OperatorPool;
use App\Services\TelephonyClient;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Bus\Queueable;
use Throwable;

class ProcessIncomingCallRedisJob implements ShouldQueue
{
    use InteractsWithQueue, Queueable, SerializesModels;

    public int $tries       = 5;
    public int $timeout     = 30;
    public array $backoff   = [5, 15, 30, 60, 120];

    private int $callId;

    public function __construct(int $callId)
    {
        $this->callId = $callId;
    }

    public function handle(): void
    {
        $call = Call::find($this->callId);

        if (!$call) {
            Log::warning('Call not found', ['call_id' => $this->callId]);
            return;
        }

        if (!in_array($call->status, ['new', 'pending_operator'])) {
            return; // Идемпотентность
        }

        // 1. Привязка клиента
        $client = Client::where('phone', $call->phone)->first();

        // 2. Захват оператора через Redis
        $operatorId = app(OperatorPool::class)->acquire();

        if (!$operatorId) {
            // Не меняем статус — оставляем new для retry
            Log::info('No available operators, retrying', ['call_id' => $call->id]);
            $this->release(10);
            return;
        }

        // 3. Сохраняем в БД
        DB::transaction(function () use ($call, $client, $operatorId) {
            $call->client_id   = $client?->id;
            $call->operator_id = $operatorId;
            $call->status      = 'assigned';
            $call->assigned_at = now();
            $call->save();

            // Обновляем оператора в БД
            Operator::where('id', $operatorId)->update([
                'available'    => false,
                'last_call_at' => now(),
            ]);
        });

        Log::info('Call assigned', [
            'call_id'     => $call->id,
            'operator_id' => $operatorId,
            'client_id'   => $call->client_id,
        ]);

        // 4. Внешняя телефония
        try {
            app(TelephonyClient::class)->sendCallAssigned(
                $call->id,
                $operatorId,
                ['idempotency_key' => "call_{$call->id}_assign"]
            );
        } catch (Throwable $e) {
            Log::error('Telephony notification failed', [
                'call_id'     => $call->id,
                'operator_id' => $operatorId,
                'error'       => $e->getMessage(),
            ]);
            throw $e;
        }
    }

    public function failed(Throwable $exception): void
    {
        Log::error('ProcessIncomingCallRedisJob failed permanently', [
            'call_id' => $this->callId,
            'error'   => $exception->getMessage(),
        ]);

        $call = Call::find($this->callId);
        if ($call && in_array($call->status, ['new', 'pending_operator'])) {
            // Возвращаем оператора в Redis-пул
            if ($call->operator_id) {
                app(OperatorPool::class)->makeAvailable($call->operator_id);
            }
            $call->status = 'failed';
            $call->error_message = $exception->getMessage();
            $call->save();
        }
    }
}
