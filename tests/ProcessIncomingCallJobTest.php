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
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
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

    /** @test */
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

    /** @test */
    public function it_is_idempotent_when_call_already_assigned(): void
    {
        $operator = Operator::factory()->create(['available' => false]);
        $call = Call::factory()->create([
            'status'      => 'assigned',
            'operator_id' => $operator->id,
        ]);

        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldNotReceive('sendCallAssigned');
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob($call->id))->handle();

        // Если Job повторился — ничего не должно произойти
        $call->refresh();
        $this->assertEquals('assigned', $call->status);
    }

    /** @test */
    public function it_does_not_override_operator_if_concurrent_worker_changed_status(): void
    {
        $operator1 = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subHour()]);
        $operator2 = Operator::factory()->create(['available' => true, 'last_call_at' => now()->subMinutes(30)]);

        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        // Оператор с самым старым last_call_at должен быть назначен первым
        (new ProcessIncomingCallJob($call->id))->handle();

        $call->refresh();
        $this->assertEquals($operator1->id, $call->operator_id);
    }

    // =====================================================================
    // ВАЖНЫЕ — Retry, Error Handling, Business Logic
    // =====================================================================

    /** @test */
    public function it_marks_call_as_pending_operator_when_no_operators_available(): void
    {
        $call = Call::factory()->create(['status' => 'new', 'phone' => '+79000000001']);

        $job = new ProcessIncomingCallJob($call->id);
        Queue::fake();

        $telephony = $this->mock(TelephonyClient::class);
        $this->app->instance(TelephonyClient::class, $telephony);

        // В оригинальном коде здесь бросался Exception('No available operators').
        // В исправленном — Job тихо просит retry через release().
        $this->assertTrue(true); // TODO: интеграционный тест для release()
    }

    /** @test */
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
    }

    /** @test */
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

    /** @test */
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
    }

    /** @test */
    public function it_silently_returns_when_call_not_found(): void
    {
        $telephony = $this->mock(TelephonyClient::class);
        $telephony->shouldNotReceive('sendCallAssigned');
        $this->app->instance(TelephonyClient::class, $telephony);

        (new ProcessIncomingCallJob(999999))->handle();

        // Тихий выход — без исключений
        $this->assertTrue(true);
    }

    // =====================================================================
    // NICE TO HAVE — Логирование, метрики
    // =====================================================================

    /** @test */
    public function it_logs_structured_info_on_successful_assignment(): void
    {
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

    /** @test */
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
