# Code Review: ProcessIncomingCallJob

> Ревью задачи обработки входящего звонка в Laravel.
> Оригинальный код — [`src/Original/ProcessIncomingCallJob.php`](src/Original/ProcessIncomingCallJob.php).

---

## Допущения и предположения

Поскольку в задании не описан ряд аспектов, зафиксирую предположения:

| Область | Предположение |
|---|---|
| **Очередь** | Redis-драйвер, `queue:work` запущен в нескольких процессах |
| **БД** | MySQL / PostgreSQL с поддержкой `SELECT ... FOR UPDATE` |
| **Телефония** | HTTP API с неизвестными гарантиями (может быть медленной, может падать, может дублировать) |
| **Модель Call** | Статусы: `new` → `assigned` → `in_progress` → `completed`; возможен `pending_operator`, `failed` |
| **Модель Operator** | Флаг `available` + `last_call_at` для round-robin распределения |
| **Legacy** | В production уже могут быть данные с непоследовательными статусами |

### Риски из-за неопределённости

1. **Телефония может принимать повторные запросы как новые звонки** → нужен idempotency key, иначе оператору придёт два звонка.
2. **Redis может быть недоступен** → Job потеряется или зависнет; нужен DLQ и мониторинг.
3. **Поведение `save()` при конкурентной записи** — нет optimistic locking (`use PivotEvent`/version column), возможны lost updates.
4. **Нет чёткого SLA телефонии** — таймаут HTTP может быть 1 секунда, а может 30; это влияет на `$timeout` Job.
5. **Неизвестно, кто освобождает оператора** — если Job не освобождает operator при падении, он останется `available = false` навсегда.

---

## Найденные проблемы

### 🔴 Критические

#### 1. Race condition: два воркера захватывают одного оператора

```
Worker A: $operator = Operator::where('available', true)->first();  // Operator #1
Worker B: $operator = Operator::where('available', true)->first();  // Operator #1 (тот же!)
Worker A: $operator->available = false; $operator->save();
Worker B: $operator->available = false; $operator->save();          // перезапись
```

Оба воркера назначают **одного и того же** оператора на два разных звонка.

**Исправление:** Атомарный захват через `UPDATE ... WHERE available = true ORDER BY last_call_at LIMIT 1` внутри транзакции, либо `SELECT ... FOR UPDATE`.

См. [`src/Fixed/ProcessIncomingCallJob.php`](src/Fixed/ProcessIncomingCallJob.php) → метод `acquireOperator()`.

---

#### 2. Отсутствие транзакции

Чтение `Call`, обновление `Operator`, запись `Call` — три отдельные операции. Если процесс упадёт после `$operator->save()`, но до `$call->save()`:
- Оператор уже `available = false`
- Звонок остаётся `status = new` без оператора
- При повторе Job (retry) оператор не будет найден (он «занят»)

**Исправление:** Обернуть всю бизнес-логику в `DB::transaction()`.

---

#### 3. HTTP-вызов в телефонию без обработки ошибок

Если `sendCallAssigned()` бросает исключение:
- Job падает и retry-ится (OK).
- Но на повторе `$call->status === 'new'` — **нет, он уже `assigned`** после первого запуска (если save прошёл). Это может работать как идемпотентность, а может нет — зависит от того, успел ли `save()`.

Если `sendCallAssigned()` висит 30 секунд:
- Worker заблокирован.
- `$timeout` Job не задан — Laravel убьёт процесс только по `retry_after` в Redis (по умолчанию 90 сек), что может привести к дублю.

**Исправление:**
- Явный `$timeout = 30` на Job.
- HTTP-клиент с таймаутом 5-10 сек.
- try/catch + логирование.
- Idempotency key на повторный запрос.

---

#### 4. Отсутствие `failed()` — при исчерпании попыток звонок «зависает»

Если все 5 попыток провалились (нет операторов → Exception), звонок остаётся `status = new` навсегда. Никто не узнает о проблеме.

**Исправление:** Метод `failed()` переводит звонок в `status = 'failed'` и пишет лог/алерт.

---

### 🟡 Важные

#### 5. `throw new Exception('No available operators')` — это не ошибка, это бизнес-ситуация

Нет свободных операторов — нормальная ситуация в колл-центре. Exception:
- Увеличивает счётчик ошибок в мониторинге (шум).
- Срабатывает retry с backoff, но backoff не задан — повторит сразу.

**Исправление:** Не менять статус — оставить `new`, поставить отложенный retry (`$this->release(10)`), не бросать Exception.

---

#### 6. Нет `$backoff` — мгновенный retry

`$tries = 5`, но `$backoff` не задан. Все 5 попыток могут выполниться за секунды, что бессмысленно для ситуации «нет операторов» — они не появятся через 0.5 сек.

**Исправление:** `$backoff = [5, 15, 30, 60, 120]` — экспоненциальная задержка.

---

#### 7. Нет идемпотентности при повторе Job

Если worker упал после `$call->save()` (status = assigned), но до `Log::info`:
- Job повторится (Redis не получил ACK).
- `$call->status === 'new'` → false, Job выйдет — OK в этом конкретном месте.

Но если worker упал **после** `$operator->save()`, но **до** `$call->save()`:
- Оператор занят, звонок `new`.
- Retry → нет оператора → Exception → зависание.

**Исправление:** Транзакция (п.2) + атомарный захват (п.1) решают оба сценария.

---

#### 8. `$callId` — нет типа, нет валидации

`$callId` может быть `null`, строкой, массивом. Если передан `null`, `Call::find(null)` вернёт `null` — тихий пропуск без лога.

**Исправление:** Тип `int` в конструкторе + early return с warning-логом.

---

### 🟢 Было бы хорошо сделать

#### 9. Отсутствие мониторинга и метрик

Нет счётчиков:
- Время от `call.created_at` до `call.assigned_at` (SLA).
- Процент звонков без оператора.
- Доля failed jobs.

**Исправление:** Добавить `assigned_at` в модель Call, писать метрики в StatsD/Prometheus.

---

#### 10. Жёсткая привязка к `app()` для TelephonyClient

`app(TelephonyClient::class)` — разрешение через контейнер прямо в handle. Это затрудняет тестирование и нарушение DI.

**Исправление:** Inject через конструктор или метод.

---

## Первые тесты

Приоритет тестирования — от критических багов к остальным:

| # | Тест | Приоритет |
|---|---|---|
| 1 | Два параллельных звонка не получают одного оператора (race condition) | 🔴 |
| 2 | Повторный вызов Job не назначает оператора дважды (idempotency) | 🔴 |
| 3 | Звонок без оператора → release() с задержкой, retry без изменения статуса | 🟡 |
| 4 | Привязка клиента по номеру телефона | 🟡 |
| 5 | Звонок с несуществующим номером → клиент не привязан | 🟡 |
| 6 | HTTP-ошибка телефонии → retry | 🟡 |
| 7 | Call not found → тихий выход | 🟢 |
| 8 | Выбирается оператор с самым старым `last_call_at` | 🟢 |

Полные тесты: [`tests/ProcessIncomingCallJobTest.php`](tests/ProcessIncomingCallJobTest.php).

---

## Что я НЕ стал бы делать прямо сейчас

1. **Event-sourcing / CQRS** — оверкилл для текущей задачи. Достаточно транзакций.
2. **WebSocket / Real-time пуш оператору** — если телефония уже есть, пушить в неё достаточно.
3. **Микросервисы** — выносить Job в отдельный сервис пока рано; сначала убедимся, что один сервис справляется.
4. **Сложный routing-алгоритм операторов** (навыки, языки, приоритеты) — сначала фиксим базовую корректность.
5. **Оптимистичный locking на Call через `version`** — сейчас достаточно проверки `status === 'new'`; добавлю если появятся lost updates на проде.
6. **Блокировка `clients` таблицы** — привязка клиента идемпотентна и не вызывает race.

---

## Исправленный код

| Файл | Описание |
|---|---|
| [`src/Fixed/ProcessIncomingCallJob.php`](src/Fixed/ProcessIncomingCallJob.php) | Исправленная версия: транзакция, атомарный захват, backoff, failed(), idempotency |
| [`src/Fixed/OperatorPool.php`](src/Fixed/OperatorPool.php) | Redis+Lua пул операторов для высокой нагрузки |
| [`src/Fixed/ProcessIncomingCallRedisJob.php`](src/Fixed/ProcessIncomingCallRedisJob.php) | Вариант Job с Redis-пулом (для масштабирования) |

---

## Вопрос про масштабирование (×10–50)

### Ожидаемые bottleneck'и

| Компонент | Текущая проблема | При росте ×50 |
|---|---|---|
| **БД: operators** | `SELECT ... WHERE available ORDER BY last_call_at` с row lock — каждая транзакция блокирует строку | При 500 операторах и 100 RPS — сериализация на уровне таблицы. All workers ждут release lock |
| **БД: calls** | Каждый Job делает 2+ запроса к `calls` + 1 к `clients` | I/O растёт линейно; connection pool (default ~100) исчерпается |
| **Redis queue** | Один queue, один worker group | При росте depth очереди — consumer lag; также `retry_after` может убивать долгие jobs |
| **HTTP telephony** | Синхронный вызов в each Job | RPS в телефонию = RPS звонков. При 500 RPS и timeout 5 сек → 2500 одновременных соединений |
| **Логирование** | `Log::info` на каждый звонок | I/O на диск; при structured logging в ELK — буфер может не успевать |

### Простое увеличение workers: что даст и где перестанет

**Что даст:**
- До ~10–20 workers: пропускная способность растёт почти линейно, если БД на SSD и connection pool достаточный.

**Где перестанет помогать:**
- **~20–30 workers:** БД-соединения исчерпаны (default `max_connections` = 151 в MySQL). Каждый worker держит 1–2 соединения.
- **~30+ workers:** Lock contention на `operators` — workers проводят больше времени в ожидании lock, чем в полезной работе. Throughput выходит на плато, latency растёт.
- **~50+ workers:** OOM на сервере, scheduler thrashing.

### Лимиты по компонентам

#### Redis
- **Queue depth:** при 500 calls/sec и 5 sec обработки → 2500 jobs в очереди — не проблема для Redis.
- **Проблема:** `retry_after` (default 90 sec). Если Job обрабатывается 30+ секунд (медленный HTTP), Redis сочтёт его потерянным и отдаст другому worker → дублирование.
- **Решение:** Увеличить `retry_after` > `$timeout`, включить `$tries = 1` + ручной retry, или перейти на Redis 7+ с `XAUTOCLAIM`.

#### БД
- **Connections:** 50 workers × 2 connections = 100 → нужен `max_connections` ≥ 200 + connection pooling (PgBouncer / ProxySQL).
- **Lock contention на `operators`:** При атомарном `UPDATE ... WHERE available ORDER BY` — каждый UPDATE блокирует индекс. Решение: перейти на Redis-пул (см. ниже).
- **Writes:** При 500 RPS → 1500+ writes/sec (call + operator + client). Нормально для SSD, но нужно убедиться, что нет long-running transactions.

#### HTTP-интеграция с телефонией
- **Connection pool:** Laravel HTTP client (Guzzle) по умолчанию не переиспользует соединения между Jobs. При 500 RPS → 500 TCP-handshake/sec.
- **Timeout:** Если телефония отвечает 10 сек → 50 workers заблокированы на I/O.
- **Решение:**
  - Асинхронная отправка: Job кладёт событие в отдельную Redis-очередь → TelephonyNotifyJob с concurrency limit.
  - HTTP client с `Connection: keep-alive` и pool.
  - Circuit breaker: при 5+ ошибках подряд — пауза 30 сек + алерт.

#### Логирование
- **Volume:** 500 RPS × 2 log lines = 1000 lines/sec. Файл — OK; ELK/Splunk — может отставать.
- **Решение:** Buffer logging, async log shipper (Fluentd/Filebeat), метрики вместо логов где возможно.

### План масштабирования (поэтапно)

#### Фаза 1: Quick wins (1–2 недели)

- [ ] Connection pooling в БД (PgBouncer / ProxySQL)
- [ ] `$timeout`, `$backoff`, `retry_after` выровнены
- [ ] HTTP client с keep-alive и timeout 5 сек
- [ ] Отдельная очередь `calls` с приоритетом
- [ ] Мониторинг: queue depth, job duration, error rate

#### Фаза 2: Eliminate DB bottleneck (2–4 недели)

- [ ] **Redis-пул операторов** через sorted set + Lua (см. [`src/Fixed/OperatorPool.php`](src/Fixed/OperatorPool.php))
  - `ZPOPMIN` — O(log N), нет блокировок в БД
  - Восстановление при crash: periodic sync Redis → DB
- [ ] Read replicas для `Client::where('phone')` — не нужна strict consistency для поиска клиента
- [ ] Batch insert логов (или switch на async log driver)

#### Фаза 3: Decouple telephony (1–2 недели)

- [ ] Вынести `sendCallAssigned` в отдельную очередь `telephony`:
  ```
  ProcessIncomingCallJob → БД-запись → dispatch(TelephonyNotifyJob)
  ```
- [ ] Rate limiter на отправку в телефонию (если у них есть лимиты)
- [ ] Circuit breaker на TelephonyClient
- [ ] Retry с exponential backoff для HTTP-ошибок

#### Фаза 4: Horizontal scaling (по мере роста)

- [ ] Отдельные серверы под worker-ы (stateless — масштабируются горизонтально)
- [ ] Redis Cluster для очередей (если один Redis не справляется)
- [ ] Рассмотреть переход с Redis Queue на **RabbitMQ / Kafka** если:
  - Нужен strict ordering (calls одного клиента)
  - Нужен DLQ с подтверждением
  - Нужна replayability

#### Фаза 5: Observability (ongoing)

- [ ] Dashboards: Grafana + Prometheus
  - `call_assignment_duration_seconds` (histogram)
  - `operators_available_count` (gauge)
  - `job_failure_rate` (counter)
  - `telephony_request_duration_seconds` (histogram)
- [ ] Alerts:
  - Queue depth > 1000
  - No available operators > 5 минут
  - Telephony error rate > 5%
  - Failed calls > 0

### Сводная таблица масштабирования

| Метрика | Сейчас | ×10 | ×50 | Решение |
|---|---|---|---|---|
| Calls/sec | ~10 | ~100 | ~500 | — |
| Workers | 3–5 | 20–30 | 50–100 | Redis pool + conn pool |
| DB connections | ~10 | ~60 | ~200 | PgBouncer |
| Lock wait (operators) | ~0 | ~5ms | ~50ms | Redis pool |
| Telephony RPS | ~10 | ~100 | ~500 | Decouple + rate limit |
| Log volume | ~20/sec | ~200/sec | ~1000/sec | Async + metrics |
