<?php

/**
 * Захват оператора через Redis sorted set.
 *
 * Используется при высокой нагрузке (десятки/сотни RPS), когда
 * блокировки в БД на operators становятся bottleneck.
 *
 * Логика:
 *  1. Операторы при логине добавляются в sorted set 'operators:available'
 *     с score = last_call_at (или timestamp).
 *  2. ZPOPMIN атомарно извлекает оператора с наименьшим score.
 *  3. При завершении звонка оператор возвращается в sorted set.
 *
 * ZPOPMIN — одна Redis-команда, уже атомарная. Lua не нужен.
 *
 * Риски:
 *  - Требует синхронизации Redis ↔ БД (crash recovery).
 *  - Если Redis недоступен — нужен fallback на DB.
 */

namespace App\Services;

use Illuminate\Support\Facades\Redis;

class OperatorPool
{
    private const AVAILABLE_SET = 'operators:available';

    /**
     * Зарегистрировать оператора как доступного.
     */
    public function makeAvailable(int $operatorId, ?int $lastCallAt = null): void
    {
        $score = $lastCallAt ?? now()->timestamp;
        Redis::zadd(self::AVAILABLE_SET, $score, $operatorId);
    }

    /**
     * Захватить свободного оператора с наименьшим last_call_at.
     *
     * ZPOPMIN — атомарная команда: извлекает и удаляет элемент
     * с минимальным score из sorted set. Два параллельных клиента
     * не получат одного и того же оператора.
     *
     * @return int|null ID оператора или null если свободных нет
     */
    public function acquire(): ?int
    {
        $result = Redis::zpopmin(self::AVAILABLE_SET);

        if ($result === null || $result === false) {
            return null;
        }

        // phpredis возвращает ['id' => score] или [id => score]
        // Нам нужен только ID (ключ)
        $result = is_array($result) ? array_key_first($result) : $result;

        return (int) $result;
    }

    /**
     * Вернуть оператора в пул (если звонок не состоялся).
     */
    public function release(int $operatorId, int $lastCallAt = 0): void
    {
        $score = $lastCallAt ?: now()->timestamp;
        Redis::zadd(self::AVAILABLE_SET, $score, $operatorId);
    }

    /**
     * Количество доступных операторов.
     */
    public function availableCount(): int
    {
        return Redis::zcard(self::AVAILABLE_SET);
    }
}
