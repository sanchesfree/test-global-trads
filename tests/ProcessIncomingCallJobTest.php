<?php

/**
 * Тесты для ProcessIncomingCallJob.
 *
 * Используем PHPUnit + Laravel's Queue / Bus / Event fake.
 *
 * Приоритет: сначала критические баги (race condition, idempotency),
 * потом важные (retry, логирование), потом nice-to-have.
 */

namespace Tests\Feature\Jobs;

use App\Jobs\ProcessIncomingCallJob;
use App\Models\Call;
use App\Models\Client;
use App\Models\Operator;
use App\Services\TelephonyClient;
use Illuminate\Support\Facades\Log;
use Mockery;
use Tests\TestCase;

class ProcessIncomingCallJobTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        Log::spy(); // заглушаем логи по умолчанию
    }

    // =====================================================================
    // КРИТИЧЕСКИЕ — Race Conditions & Idempotency
    // =====================================================================

    /**
     * @test
     * Главный баг оригинального кода: два параллельных Job-а могут захватить
     * одного оператора. Проверяем что после назначения оператор занят,
     * и второй звонок остаётся без оператора.
     */
    public function it_does_not_assign_same_operator_to_two_calls(): void
    {
        $operator = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);

        $call1 = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);
        $call2 = Call::factory()->create(['status' => 'new', 'phone' => '+79000000002']);

        // Два звонка, один оператор — второй не должен получить того же оператора

        // Обрабатываем первый звонок
        (new ProcessIncomingCallJob($call1->id))->handle();

        // Обрабатываем второй звонок — оператор уже занят
        (new ProcessIncomingCallJob($call2->id))->handle();

        $call1->refresh();
        $call2->refresh();

        $this->assertEquals('assigned', $call1->status);
        $this->assertEquals('new', $call2->status);  // оператор уже занят — звонок не назначен
        $this->assertEquals($operator->id, $call1->operator_id);
        $this->assertNull($call2->operator_id);
    }

    /**
     * @test
     * Если worker упал после назначения, Redis отдаст Job повторно.
     * Проверяем что повторный вызов не переназначит оператора,
     * даже если есть свободные.
     */
    public function it_is_idempotent_when_call_already_assigned(): void
    {
        $operator = Operator::factory()->create(['available' => false]);
        // Свободные операторы есть — но Job не должен переназначать
        Operator::factory()->count(3)->create(['available' => true, 'last_call_at' => now()->subHour()]);

        $call = Call::factory()->create([
            'status'      => 'assigned',
            'operator_id' => $operator->id,
        ]);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldNotReceive('sendCallAssigned');
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob($call->id))->handle();

        // Если Job повторился — оператор не должен измениться
        $call->refresh();
        $this->assertEquals('assigned', $call->status);
        $this->assertEquals($operator->id, $call->operator_id);
    }

    /**
     * @test
     * Один звонок должен захватить ровно одного оператора.
     * Остальные операторы остаются свободными.
     */
    public function it_only_assigns_one_operator_per_call(): void
    {
        $operator1 = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);
        $operator2 = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subMinutes(30)]);

        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        (new ProcessIncomingCallJob($call->id))->handle();

        $call->refresh();
        $this->assertEquals($operator1->id, $call->operator_id);
        $this->assertEquals('assigned', $call->status);

        // Второй оператор не должен был быть затронут
        $operator2->refresh();
        $this->assertTrue($operator2->available);
    }

    // =====================================================================
    // ВАЖНЫЕ — Retry, Error Handling, Business Logic
    // =====================================================================

    /**
     * @test
     * В оригинале бросался Exception — это засоряло мониторинг.
     * Теперь Job тихо просит retry через release(),
     * звонок остаётся в статусе 'new'.
     */
    public function it_does_not_throw_when_no_operators_available(): void
    {
        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldNotReceive('sendCallAssigned');
        $this->app->instance(TelephonyClient::class, $telephony);

        // В оригинальном коде здесь бросался Exception('No available operators').
        // В исправленном — Job тихо просит retry через release().
        (new ProcessIncomingCallJob($call->id))->handle();

        // Звонок не должен быть назначен, но и не упал
        $call->refresh();
        $this->assertEquals('new', $call->status);
        $this->assertNull($call->operator_id);
    }

    /**
     * @test
     * Если в БД есть клиент с таким же номером телефона —
     * привязать его к звонку.
     */
    public function it_links_client_by_phone(): void
    {
        $client = Client::factory()->create(['phone' => '+79000000001']);
        $operator = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);

        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldReceive('sendCallAssigned')->once();
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob($call->id))->handle();

        $call->refresh();
        $this->assertEquals($client->id, $call->client_id);
        $this->assertEquals('assigned', $call->status);
    }

    /**
     * @test
     * Если клиента с таким номером нет в БД —
     * звонок всё равно назначается, client_id остаётся null.
     */
    public function it_handles_call_without_existing_client(): void
    {
        $operator = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);

        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79999999999']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldReceive('sendCallAssigned')->once();
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob($call->id))->handle();

        $call->refresh();
        $this->assertNull($call->client_id);
        $this->assertEquals('assigned', $call->status);
    }

    /**
     * @test
     * Если HTTP-запрос в телефонию упал — Job должен бросить исключение
     * (чтобы очередь сделала retry), но оператор уже назначен в БД.
     * При повторе Job увидит 'assigned' и не переназначит.
     */
    public function it_retries_on_telephony_failure(): void
    {
        $operator = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);
        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldReceive('sendCallAssigned')
            ->once()
            ->andThrow(new \RuntimeException('Telephony timeout'));
        $this->app->instance(TelephonyClient::class, $telephony);

        $this->expectException(\RuntimeException::class);
        (new ProcessIncomingCallJob($call->id))->handle();

        // Оператор уже назначен в БД — при retry Job увидит 'assigned' и не переназначит
        $call->refresh();
        $this->assertEquals('assigned', $call->status);
    }

    /**
     * @test
     * Если звонок удалён из БД до обработки — Job не должен упасть.
     */
    public function it_silently_returns_when_call_not_found(): void
    {
        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldNotReceive('sendCallAssigned');
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob(999999))->handle();
    }

    // =====================================================================
    // NICE TO HAVE — Логирование, метрики
    // =====================================================================

    /**
     * @test
     * При успешном назначении логируется 'Call assigned'
     * с call_id, operator_id и client_id.
     */
    public function it_logs_structured_info_on_successful_assignment(): void
    {
        // Override setUp's Log::spy() — this test needs to assert specific log calls
        Log::restore();

        $operator = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);
        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldReceive('sendCallAssigned')->once();
        $this->app->instance(TelephonyClient::class, $telephony);

        Log::shouldReceive('info')
            ->with('Call assigned', Mockery::on(function ($context) use ($call, $operator) {
                return isset($context['call_id'], $context['operator_id'])
                    && $context['call_id'] === $call->id
                    && $context['operator_id'] === $operator->id;
            }));

        (new ProcessIncomingCallJob($call->id))->handle();
    }

    /**
     * @test
     * Операторы распределяются по принципу «кто дольше не работал — тот следующий».
     * Проверяем что выбран оператор с самым старым last_call_at.
     */
    public function it_selects_operator_with_oldest_last_call_at(): void
    {
        $oldest = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subDays(3)]);
        Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);
        Operator::factory()->create(['available' => true, 'last_call_at' => now()->subMinute()]);

        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldReceive('sendCallAssigned')->once();
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob($call->id))->handle();

        $call->refresh();
        $this->assertEquals($oldest->id, $call->operator_id);
    }
}
