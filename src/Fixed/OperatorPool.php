<?php

/**
 * Альтернативная реализация захвата оператора через Redis+Lua.
 *
 * Используется при высокой нагрузке (десятки/сотни RPS), когда
 * SELECT-FOR-UPDATE на operators становится bottleneck.
 *
 * Логика:
 *  1. Операторы при логине добавляются в sorted set 'operators:available'
 *     с score = last_call_at (или timestamp).
 *  2. Lua-скрипт атомарно извлекает оператора с наименьшим score
 *     и удаляет его из sorted set.
 *  3. При завершении звонка оператор возвращается в sorted set.
 *
 * Преимущества:
 *  - Нет блокировок в БД.
 *  - O(log N) операций.
 *  - Полная атомарность через Lua.
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
     * Lua-скрипт: атомарно извлечь оператора с минимальным last_call_at.
     *
     * Возвращает operator_id или nil если нет свободных.
     */
    private const ACQUIRE_LUA = <<<'LUA'
        local result = redis.call('ZPOPMIN', KEYS[1])
        if #result == 0 then
            return false
        end
        return result[1]
    LUA;

    /**
     * Зарегистрировать оператора как доступного.
     */
    public function makeAvailable(int $operatorId, ?int $lastCallAt = null): void
    {
        $score = $lastCallAt ?? now()->timestamp;
        Redis::zadd(self::AVAILABLE_SET, $score, $operatorId);
    }

    /**
     * Атомарно захватить свободного оператора.
     *
     * @return int|null ID оператора или null
     */
    public function acquire(): ?int
    {
        $result = Redis::eval(self::ACQUIRE_LUA, 1, self::AVAILABLE_SET);
        return $result !== null ? (int) $result : null;
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
