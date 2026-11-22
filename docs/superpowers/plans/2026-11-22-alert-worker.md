# Alert Worker 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `stream:alert:event`의 양보 알림 요청·해제·경로 갱신·수신 확인 이벤트를 소비해서, 대상 차량이 연결된 Gateway 채널로 클라이언트 메시지를 발행하고 알림 상태(Redis)와 전송 이력(PostgreSQL)을 관리하는 Alert Worker를 만든다.

**Architecture:** Emergency Worker와 같은 형태의 단독 Node.js 워커(프레임워크 없음)로 만든다. Redis Stream 소비자 그룹 → `AlertEventRouter` → 서비스 3개(`YieldAlertService`, `RouteUpdateService`, `AcknowledgementService`) 순서로 처리하고, 서비스는 `domain/types.ts`에 정의한 포트 인터페이스(상태 저장소, 중복 억제, 세션 조회, 전달, 이력)에만 의존한다. 전달은 `ws:vehicle:{vehicleId}`로 Gateway를 찾은 뒤 `channel:gateway:{gatewayId}`에 PUBLISH한다. 유효기간 만료는 별도 주기 스케줄러가 처리한다.

**Tech Stack:** TypeScript 6.0.3, Node.js ≥ 26, ioredis 6.0.0, pg 8.16.3, Jest 30.5.2 + ts-jest 29.4.15 (Emergency Worker와 같은 버전으로 고정)

**Spec:**
- 설계서: `../상세설계서수정본_윰씨윰씨.pdf` — Alert Worker 상세 설계 58–64p, ERD `alert_delivery_log` 11–12p, Redis 키 13–14p, 이벤트 명세 96–108p
- 요구사항: `../상세요구사항명세서_윰씨윰씨.pdf` — F-ALT-01~05 (7p)
- 실제 상대 계약: `../Emergency-Worker/src/domain/contracts.ts` (생산자), `../client-core/src/contracts.ts` (최종 소비자)

**범위 결정 (사용자 확인, 2026-11-22):** Alert Worker만 구현한다. 현재 Gateway는 `ws:vehicle:*` 등록, `channel:gateway:*` 구독, ACK 스트림 발행을 하지 않으므로, 샌드박스 검증은 이 Gateway 역할을 흉내 내는 E2E 테스트로 한다. Gateway 수정과 Emergency Worker의 `evacs:stream:yield-alert` 이중 발행 정리는 후속 작업이다.

## Global Constraints

- 작업은 `/Users/chs/dev/Inha-Capstone/Alert-Worker` 안에서만 하고, 상위 `docker-compose.local.yml` 등 다른 저장소는 수정하지 않는다.
- 작업 브랜치는 `feat/alert-worker`이다(현재 저장소는 커밋이 없는 `main`).
- 코드 스타일은 Google TypeScript Style Guide를 따른다(2칸 들여쓰기, 작은따옴표, `I` 접두사 없는 인터페이스 이름). 테스트는 핵심 판단 로직과 서비스 간 주요 연동 위주로 간결하게 쓴다(설계서 4p).
- 입력 스트림 `stream:alert:event`, 엔트리 필드 `event`에 JSON envelope `{eventId, eventType, schemaVersion: 1, occurredAt, producer, payload}`가 들어 있다(Emergency Worker `parseStreamEvent`와 동일). 같은 엔트리의 `eventKey` 필드는 무시한다.
- 소비자 그룹 `alert-worker`, DLQ 스트림 `stream:alert-worker:dlq`
- Redis 키:
  - `ws:vehicle:{vehicleId}`: Gateway가 쓰는 JSON `{vehicleId, gatewayId, sessionId, connectedAt}`. 읽기만 한다.
  - `alert:{alertId}:state`: JSON 문자열
  - `alert:active:{dispatchId}:{vehicleId}`: alertId
  - `alert:expiry`: ZSET, score는 expiresAt(ms)
  - `alert:dedup:{dispatchId}:{vehicleId}`: 억제 지문, PX 10000
  - `route:delivered:{dispatchId}`: 마지막으로 전달한 routeVersion
- 전달 채널은 `channel:gateway:{gatewayId}`이고, 발행 프레임은 `{targetVehicleId, sessionId, message}` JSON이다.
- 재발송 억제 시간 10초(설계서 60p). 알림 상태 키는 expiresAt보다 5분 더 보존한다.
- `alertVersion`은 요청의 `decisionVersion`을 그대로 쓴다. Emergency Worker의 Gateway projection과 설계서 예시(decisionVersion 3 → alertVersion 3) 모두 이렇게 되어 있다.
- 클라이언트 메시지에는 설계서의 `eventType`과 client-core의 `type`을 둘 다 같은 값으로 넣는다.
- 시각은 모두 UTC ISO 8601(`toISOString()`)로 쓴다.
- 포트와 환경 변수: `HEALTH_PORT` 기본 18083, `REDIS_URL` 기본 `redis://localhost:6401`, `DATABASE_URL` 필수(샌드박스는 `postgres://evacs:evacs@127.0.0.1:5433/evacs`), `CONSUMER_NAME` 기본 hostname, `READ_COUNT` 기본 10, `BLOCK_MS` 기본 1000, `PENDING_MIN_IDLE_MS` 기본 30000, `MAX_DELIVERIES` 기본 5, `EXPIRY_SWEEP_MS` 기본 1000, `DEDUP_TTL_MS` 기본 10000, `ALERT_STREAM`/`DLQ_STREAM`/`CONSUMER_GROUP`은 위 기본값을 덮어쓸 수 있다.
- 통합 테스트는 샌드박스 Redis(호스트 `6401`)와 Postgres(호스트 `5433`)를 쓴다. 먼저 `docker compose -f ../docker-compose.local.yml up -d --wait redis postgres`로 띄우고, 테스트마다 무작위 접미사가 붙은 스트림·키 이름을 써서 실행 중인 다른 서비스와 충돌하지 않게 한다.

### 설계서와 다르게 정한 점 (근거)

| 설계서 | 이 계획 | 근거 |
|---|---|---|
| `target_vehicle_id UUID`, `dispatch_id UUID FK` | 둘 다 `VARCHAR(128)`, FK 없음 | SUMO 차량 ID가 `EVACS_CAR_000000` 형식이고, `dispatch` 테이블이 아직 없다. |
| 로그에 alert 식별자 없음 | `alert_id VARCHAR`, `alert_version INT`, `clear_reason VARCHAR` 추가 | Emergency Worker의 alertId는 (dispatch, vehicle) 조합마다 고정이라 해제 후 같은 ID로 다시 발행될 수 있다. 행은 "알림 에피소드"마다 하나씩 만든다. |
| 억제 기준이 권장 행동 | `recommendedAction|approachDirection` 지문 | F-ALT-04 "안내 내용이 달라지면 갱신". 접근 방향이 바뀌면 안내 문구도 바뀐다. |
| 해제 사유 4종 | 만료 시 `EXPIRED` 사유 추가 | 만료 스케줄러도 해제 메시지를 보낸다(64p). |

## Review Focus

1. **같은 스트림 엔트리 재전달(XAUTOCLAIM, 크래시 후 재시작):** 같은 alertId·같은 alertVersion이 다시 오면 메시지를 다시 보내지 않고 Ack만 한다. 이력 기록은 멱등 upsert로 보완한다. → Task 5 `redelivered request does not publish twice`
2. **늦게 도착한 낮은 버전:** decisionVersion이 현재보다 낮은 요청이나 ACK가 오면 상태를 되돌리지 않는다. → Task 5 `stale request is dropped`, Task 6 `ack for older alertVersion is ignored`
3. **Gateway 미구독(PUBLISH 수신자 0명):** 지금 샌드박스의 실제 상태다. SENT로 기록하면 관제 화면이 거짓말을 하게 되므로 FAILED로 기록하고 Ack한다. → Task 3 `publish reports zero receivers`, Task 5 `no gateway subscriber marks FAILED`
4. **해제 후 같은 alertId 재요청:** 새 에피소드(새 alert_log_id)로 다시 발송한다. 억제 기록에 막히면 안 된다. → Task 5 `re-request after clear starts new episode`
5. **만료 스케줄러와 ACK·해제가 경합:** 상태 전이는 Lua로 "기대 상태와 버전이 맞을 때만" 수행해서, 이미 CLEARED인 알림을 EXPIRED로 덮어쓰지 않게 한다. → Task 3 `transition rejects unexpected status`, Task 8 `sweep skips cleared alert`

---

## 파일 구조

```
package.json, tsconfig.json, tsconfig.build.json, jest.config.js, .gitignore, .dockerignore
Dockerfile.local                       # Emergency Worker와 같은 멀티스테이지(node:26-alpine), lua 복사 포함
deploy/compose.alert-worker.yml        # 상위 compose에 얹는 override (alert-worker 서비스)
migrations/001_alert_delivery_log.sql
README.md
src/
  main.ts                              # 설정 로드 → Worker.start, SIGTERM 처리
  worker.ts                            # 구성요소 조립, start/stop, readiness 집계
  config/config.ts                     # loadConfig(env)
  domain/contracts.ts                  # 상수, 이벤트 타입, parseStreamEvent
  domain/types.ts                      # AlertState, ClientMessage, 포트 인터페이스, DeliveryResult
  consumer/alert-event-subscriber.ts   # XREADGROUP / XAUTOCLAIM / XACK / DLQ
  consumer/alert-event-router.ts
  alert/alert-message-factory.ts
  alert/alert-deduplicator.ts
  alert/yield-alert-service.ts
  alert/acknowledgement-service.ts
  alert/alert-expiration-scheduler.ts
  route/route-update-service.ts
  delivery/session-locator.ts
  delivery/redis-pubsub-delivery-publisher.ts
  delivery/alert-deliverer.ts          # SessionLocator + DeliveryPublisher 조합 (설계서 deliver())
  store/alert-state-repository.ts      # Lua: save-if-newer, transition
  store/route-version-store.ts
  store/alert-delivery-log-repository.ts
  infra/redis.ts, infra/postgres.ts, infra/health-server.ts, infra/logger.ts
test/unit/**.test.ts                   # 순수 로직, 포트는 인메모리 fake
test/integration/**.test.ts            # 실제 Redis/PG
test/e2e/sandbox.e2e-spec.ts           # 워커 전체 + Gateway 흉내
test/support/fakes.ts, test/support/redis.ts
```

---

### Task 1: 스캐폴딩, 설정, 계약 파싱

**Files:**
- Create: `package.json`, `tsconfig.json`, `tsconfig.build.json`, `jest.config.js`, `.gitignore`, `.dockerignore`
- Create: `src/config/config.ts`, `src/domain/contracts.ts`, `src/domain/types.ts`
- Test: `test/unit/domain/contracts.test.ts`, `test/unit/config/config.test.ts`

**Interfaces:**
- Produces (`src/domain/contracts.ts`):
  - 상수: `ALERT_STREAM='stream:alert:event'`, `DLQ_STREAM='stream:alert-worker:dlq'`, `CONSUMER_GROUP='alert-worker'`, `EVENT_FIELD='event'`, `DEDUP_TTL_MS=10_000`, `STATE_RETENTION_MS=300_000`
  - 키 함수: `sessionKey(vehicleId)`, `alertStateKey(alertId)`, `activeAlertKey(dispatchId, vehicleId)`, `ALERT_EXPIRY_KEY='alert:expiry'`, `dedupKey(dispatchId, vehicleId)`, `routeDeliveredKey(dispatchId)`, `gatewayChannel(gatewayId)`
  - 열거형: `APPROACH_DIRECTIONS`, `RECOMMENDED_ACTIONS`, `YIELD_REASONS`, `CLEAR_REASONS`(+`'EXPIRED'`). 값은 Emergency Worker contracts.ts와 같다.
  - 페이로드 타입: `YieldAlertRequestedPayload`, `YieldAlertClearRequestedPayload`, `RouteUpdateRequestedPayload`(`congestedSegments: string[]` 포함), `YieldAlertAcknowledgedPayload {alertId, vehicleId, alertVersion, receivedAt}`
  - `type AlertStreamEvent = EventEnvelope<...>`: 위 4종의 판별 유니온
  - `class ContractError extends Error`
  - `parseStreamEvent(fields: readonly string[]): AlertStreamEvent`. 필수 필드·타입·enum을 검사한다. 생산자 쪽과 달리 **추가 필드는 허용**한다.
- Produces (`src/domain/types.ts`):
  - `type AlertStatus = 'PENDING'|'SENT'|'ACKNOWLEDGED'|'CLEARED'|'EXPIRED'|'FAILED'`
  - `interface AlertState {alertId; alertLogId; dispatchId; emergencyId; targetVehicleId; status: AlertStatus; alertVersion: number; recommendedAction; approachDirection; lastSentAt: string|null; acknowledgedAt: string|null; clearedAt: string|null; expiresAt: string}`
  - `type ClientMessage = YieldAlertIssuedMessage | YieldAlertClearedMessage | EmergencyRouteUpdatedMessage`: 설계서 106–107p 필드에 `type`(= `eventType`)을 더한 형태다. Cleared에는 `reason`이 들어간다.
  - `interface GatewayFrame {targetVehicleId: string; sessionId: string; message: ClientMessage}`
  - `interface SessionInfo {vehicleId; gatewayId; sessionId; connectedAt}`
  - `type DeliveryOutcome = 'SENT'|'NO_SESSION'|'NO_SUBSCRIBER'|'SUPPRESSED'|'STALE'|'DUPLICATE'|'EXPIRED'|'NO_ACTIVE_ALERT'`; `interface DeliveryResult {outcome: DeliveryOutcome; alertId?: string}`
  - 포트: `AlertStateStore`, `Deduplicator`, `SessionLocator`, `DeliveryPublisher {publish(gatewayId, frame: GatewayFrame): Promise<number /*receivers*/>}`, `AlertDeliveryLogStore`, `RouteVersionStore`. 각 메서드 시그니처는 해당 구현 Task의 Interfaces에 있다.
- Produces (`src/config/config.ts`): `interface WorkerConfig`, `loadConfig(env = process.env): WorkerConfig`. 잘못된 값은 한꺼번에 모아서 `ConfigError(issues)`로 던진다(Emergency Worker와 같은 방식).

- [ ] **Step 1:** `git switch -c feat/alert-worker` 후 `package.json`을 만든다. 의존성과 스크립트(`build`, `typecheck`, `test:unit`, `test:integration`, `test:e2e`)는 Emergency Worker와 같은 버전·형태로 하고, `npm install`을 실행한다. jest는 unit/integration/e2e 세 project로 나눈다(e2e testMatch `test/e2e/**/*.e2e-spec.ts`).
- [ ] **Step 2: 실패하는 테스트 작성** (`contracts.test.ts`)

```ts
const envelope = (eventType: string, payload: object) =>
  ['eventKey', 'k1', 'event', JSON.stringify({eventId: 'e1', eventType, schemaVersion: 1,
    occurredAt: '2026-10-12T08:00:00.000Z', producer: 'emergency-worker', payload})];
const requested = {alertId: 'ALT-001', emergencyId: 'EMG-001', dispatchId: 'DSP-001', emergencyVehicleId: 'AMB-001',
  targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
  recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD', decisionVersion: 3, expiresAt: '2026-10-12T08:00:20.000Z'};

test('parses YIELD_ALERT_REQUESTED from event field', () => {
  expect(parseStreamEvent(envelope('YIELD_ALERT_REQUESTED', requested)).payload).toEqual(requested);
});
test('accepts unknown extra payload fields', () => {
  expect(() => parseStreamEvent(envelope('YIELD_ALERT_REQUESTED', {...requested, extra: 1}))).not.toThrow();
});
test.each([
  ['missing event field', ['payload', '{}']],
  ['bad json', ['event', '{']],
  ['unknown approachDirection', envelope('YIELD_ALERT_REQUESTED', {...requested, approachDirection: 'FRONT'})],
  ['decisionVersion 0', envelope('YIELD_ALERT_REQUESTED', {...requested, decisionVersion: 0})],
  ['unsupported eventType', envelope('VEHICLE_STATE_UPDATED', {})],
  ['ack without alertVersion', envelope('YIELD_ALERT_ACKNOWLEDGED', {alertId: 'A', vehicleId: 'V', receivedAt: '2026-10-12T08:00:01.200Z'})],
])('rejects %s', (_, fields) => expect(() => parseStreamEvent(fields)).toThrow(ContractError));
test('parses ROUTE_UPDATE_REQUESTED with congestedSegments and CLEAR with each reason', ...);
```

`config.test.ts`: 기본값(위 Global Constraints 값)과 `DATABASE_URL` 누락·`BLOCK_MS=abc`가 함께 있을 때 `ConfigError.issues.length === 2`인지 검사한다.

- [ ] **Step 3:** `npx jest --selectProjects unit` 실행 → 모듈이 없어서 FAIL하는지 확인
- [ ] **Step 4:** `contracts.ts`, `types.ts`, `config.ts`를 구현한다. timestamp 검증은 Emergency Worker의 `timestamp()` 정규식을 그대로 쓴다.
- [ ] **Step 5:** `npx jest --selectProjects unit && npm run typecheck` → PASS
- [ ] **Step 6:** Commit `feat: scaffold alert worker with event contracts and config`

---

### Task 2: AlertMessageFactory

**Files:**
- Create: `src/alert/alert-message-factory.ts`
- Test: `test/unit/alert/alert-message-factory.test.ts`

**Interfaces:**
- Consumes: Task 1 payload 타입, `ClientMessage`
- Produces: `class AlertMessageFactory` 메서드
  - `createIssued(p: YieldAlertRequestedPayload): YieldAlertIssuedMessage`
  - `createCleared(alertId, targetVehicleId, reason: ClearReason): YieldAlertClearedMessage`. 만료 경로에서도 쓰므로 이벤트가 아니라 값을 받는다.
  - `createRouteUpdated(p: RouteUpdateRequestedPayload): EmergencyRouteUpdatedMessage`
  - `buildText(direction, action): string`

안내 문구는 `${방향} 긴급차량이 접근하고 있습니다. ${행동}` 형식이다.

| 방향 | 문구 | 행동 | 문구 |
|---|---|---|---|
| REAR | 후방에서 | YIELD_WHEN_SAFE | 안전한 상황에서 양보해 주세요. |
| REAR_LEFT | 좌측 후방에서 | MOVE_RIGHT_WHEN_SAFE | 안전할 때 우측으로 이동해 주세요. |
| REAR_RIGHT | 우측 후방에서 | CLEAR_INTERSECTION_FIRST | 교차로를 먼저 통과한 뒤 안전하게 양보해 주세요. |
| LEFT | 좌측에서 | | |
| RIGHT | 우측에서 | | |

- [ ] **Step 1: 실패하는 테스트**

```ts
test('issued message matches design example', () => {
  expect(new AlertMessageFactory().createIssued(requested)).toEqual({
    type: 'YIELD_ALERT_ISSUED', eventType: 'YIELD_ALERT_ISSUED', alertId: 'ALT-001', emergencyId: 'EMG-001',
    dispatchId: 'DSP-001', targetVehicleId: 'VEH-001', approachDirection: 'REAR', estimatedArrivalSeconds: 15,
    recommendedAction: 'YIELD_WHEN_SAFE', message: '후방에서 긴급차량이 접근하고 있습니다. 안전한 상황에서 양보해 주세요.',
    alertVersion: 3, expiresAt: '2026-10-12T08:00:20.000Z'});
});
test('every direction × action yields non-empty Korean text', ...);   // 5×3 조합
test('route updated carries waypoints, congestedSegments, estimatedTravelSeconds', ...);
test('cleared carries reason', ...);
```

- [ ] **Step 2:** 실행 → FAIL
- [ ] **Step 3:** 구현
- [ ] **Step 4:** `npx jest test/unit/alert/alert-message-factory.test.ts` → PASS
- [ ] **Step 5:** Commit `feat: add alert message factory`

---

### Task 3: Redis 어댑터 (상태 저장소, 중복 억제, 세션 조회, Pub/Sub 전달, 경로 버전)

**Files:**
- Create: `src/infra/redis.ts`, `src/store/alert-state-repository.ts`, `src/alert/alert-deduplicator.ts`, `src/delivery/session-locator.ts`, `src/delivery/redis-pubsub-delivery-publisher.ts`, `src/store/route-version-store.ts`
- Create: `test/support/redis.ts`. `uniqueRedis()`는 `keyPrefix`가 무작위인 ioredis 클라이언트와 정리 함수를 반환한다. 단, PUBLISH 채널명에는 keyPrefix가 적용되지 않으므로 채널용 gatewayId에도 무작위 값을 쓴다.
- Test: `test/integration/store/redis-adapters.test.ts`

**Interfaces:**
- `AlertStateRepository implements AlertStateStore`:
  - `findById(alertId): Promise<AlertState|null>`
  - `findActive(dispatchId, vehicleId): Promise<AlertState|null>`
  - `save(state): Promise<boolean>`: Lua로 원자적으로 처리한다. 저장된 상태가 없거나 `stored.alertVersion <= state.alertVersion`일 때만 쓰고, false면 더 높은 버전이 이미 있다는 뜻이다. 상태 키는 `PX = max(expiresAt - now, 0) + STATE_RETENTION_MS`로 쓰고, active 인덱스 키에도 같은 PX를 건다. `alert:expiry`에는 status가 SENT/ACKNOWLEDGED이면 ZADD, 아니면 ZREM한다.
  - `transition(alertId, from: AlertStatus[], to: AlertStatus, patch: Partial<AlertState>, expectedVersion?: number): Promise<AlertState|null>`: Lua로 처리한다. 현재 status가 `from`에 있고 (`expectedVersion`이 주어졌다면) 버전도 같을 때만 patch를 적용한다. 실패하면 null을 반환한다. to가 종료 상태(CLEARED/EXPIRED/FAILED)이면 ZREM하고 active 인덱스를 지운다.
  - `dueForExpiry(nowMs, limit): Promise<string[]>`
- `AlertDeduplicator implements Deduplicator`: `shouldSend(dispatchId, vehicleId, fingerprint): Promise<boolean>`, `remember(dispatchId, vehicleId, fingerprint): Promise<void>`(PX `dedupTtlMs`), `forget(dispatchId, vehicleId): Promise<void>`
- `RedisSessionLocator implements SessionLocator`: `findGateway(vehicleId): Promise<SessionInfo|null>`. 키가 없거나 JSON이 깨졌거나 gatewayId·sessionId가 비어 있으면 null을 반환한다.
- `RedisPubSubDeliveryPublisher implements DeliveryPublisher`: `publish(gatewayId, frame): Promise<number>`. `PUBLISH channel:gateway:{id}` 결과(수신자 수)를 그대로 반환한다.
- `RedisRouteVersionStore implements RouteVersionStore`: `isLatest(dispatchId, routeVersion): Promise<boolean>`(저장값 < routeVersion이면 true), `markDelivered(dispatchId, routeVersion): Promise<void>`(Lua set-if-greater, 키 TTL 24h)

- [ ] **Step 1: 실패하는 테스트**

```ts
test('save keeps higher version', async () => {
  expect(await repo.save(state({alertVersion: 4}))).toBe(true);
  expect(await repo.save(state({alertVersion: 3}))).toBe(false);
  expect((await repo.findById('ALT-001'))!.alertVersion).toBe(4);
});
test('findActive resolves through dispatch/vehicle index', ...);
test('transition rejects unexpected status', async () => {
  await repo.save(state({status: 'CLEARED'}));
  expect(await repo.transition('ALT-001', ['SENT', 'ACKNOWLEDGED'], 'EXPIRED', {})).toBeNull();
});
test('transition with expectedVersion mismatch returns null', ...);
test('terminal transition removes from expiry index and active index', ...);
test('dueForExpiry returns only alerts whose expiresAt <= now', ...);
test('dedup suppresses same fingerprint within ttl, allows different fingerprint, forget resets', ...);
test('session locator returns null for missing or malformed ws:vehicle', ...);
test('publish reports zero receivers', async () => {
  expect(await publisher.publish(gw, frame)).toBe(0);
});
test('publish delivers frame JSON to subscribed channel', ...);   // 별도 subscriber 연결로 수신 확인
test('route version store only advances', ...);
```

- [ ] **Step 2:** `docker compose -f ../docker-compose.local.yml up -d --wait redis` 후 `npx jest --selectProjects integration test/integration/store/redis-adapters.test.ts` → FAIL
- [ ] **Step 3:** 구현한다. Lua 스크립트는 `defineCommand`로 등록한다. JSON을 Lua에서 다룰 때는 `cjson`을 쓴다.
- [ ] **Step 4:** 같은 명령 → PASS
- [ ] **Step 5:** Commit `feat: add redis adapters for alert state, dedup, session, delivery`

---

### Task 4: alert_delivery_log 마이그레이션과 저장소

**Files:**
- Create: `migrations/001_alert_delivery_log.sql`, `src/infra/postgres.ts`, `src/store/alert-delivery-log-repository.ts`
- Test: `test/integration/store/alert-delivery-log-repository.test.ts`

**Interfaces:**
- 테이블 `alert_delivery_log`
  - `alert_log_id UUID PK`
  - `alert_id VARCHAR(128) NOT NULL`, `alert_version INT NOT NULL`
  - `dispatch_id VARCHAR(128) NOT NULL`, `target_vehicle_id VARCHAR(128) NOT NULL`
  - `selection_reason VARCHAR NOT NULL`, `estimated_arrival_seconds INT NULL`
  - `alert_status VARCHAR NOT NULL CHECK (alert_status IN (6종))`
  - `first_sent_at / acknowledged_at / cleared_at / expires_at TIMESTAMPTZ NULL`
  - `clear_reason VARCHAR NULL`
  - 인덱스: `(dispatch_id)`, `(alert_id)`
- `PgAlertDeliveryLogRepository implements AlertDeliveryLogStore`
  - `insert(log: AlertDeliveryLog): Promise<void>`: `ON CONFLICT (alert_log_id) DO NOTHING`이라 재시도해도 안전하다.
  - `updateStatus(alertLogId, status, at: string, extra?: {alertVersion?; estimatedArrivalSeconds?; expiresAt?; clearReason?}): Promise<void>`: status에 따라 `acknowledged_at` 또는 `cleared_at`을 채운다. CLEARED/EXPIRED/FAILED는 `cleared_at`이다. SENT로 갱신될 때(같은 에피소드 재발송)는 version·ETA·expires_at을 갱신한다.
  - `findByDispatchId(dispatchId): Promise<AlertDeliveryLog[]>`
  - `ping(): Promise<void>`

- [ ] **Step 1: 실패하는 테스트**: insert 두 번 → 행 1개, ACKNOWLEDGED 갱신 → `acknowledged_at` 설정, EXPIRED 갱신 → `cleared_at` 설정, `findByDispatchId` 정렬(first_sent_at 오름차순)
- [ ] **Step 2:** `docker compose -f ../docker-compose.local.yml up -d --wait postgres` 후 마이그레이션을 적용한다: `docker compose -f ../docker-compose.local.yml exec -T postgres psql -U evacs -d evacs -v ON_ERROR_STOP=1 < migrations/001_alert_delivery_log.sql`. 이어서 테스트를 실행하고 FAIL을 확인한다(테스트는 무작위 dispatch_id를 써서 공유 DB와 충돌하지 않게 한다).
- [ ] **Step 3:** 구현
- [ ] **Step 4:** PASS
- [ ] **Step 5:** Commit `feat: add alert_delivery_log migration and repository`

---

### Task 5: AlertDeliverer와 YieldAlertService (발송·갱신·해제)

**Files:**
- Create: `src/delivery/alert-deliverer.ts`, `src/alert/yield-alert-service.ts`
- Create: `test/support/fakes.ts`. Task 1 포트의 인메모리 구현이다. Fake publisher는 수신자 수를 설정할 수 있고 받은 프레임을 기록한다.
- Test: `test/unit/alert/yield-alert-service.test.ts`

**Interfaces:**
- `AlertDeliverer.deliver(vehicleId, message: ClientMessage): Promise<'SENT'|'NO_SESSION'|'NO_SUBSCRIBER'>`: 설계서의 `YieldAlertService.deliver()`를 분리한 것이다. RouteUpdateService와 스케줄러도 함께 쓴다.
- `YieldAlertService(deps: {states, dedup, deliverer, logs, factory, clock: () => Date})`
  - `handleRequested(p: YieldAlertRequestedPayload): Promise<DeliveryResult>`
  - `handleClearRequested(p: YieldAlertClearRequestedPayload): Promise<DeliveryResult>`
  - `isStale(p, state): boolean`

**handleRequested 판정 순서** (설계서 63p를 기반으로 하고, 멱등성 처리를 추가했다):
1. `expiresAt <= now`이면 `EXPIRED`를 반환한다(발송 안 함).
2. `findById(alertId)`로 현재 상태를 읽는다. 종료 상태(CLEARED/EXPIRED/FAILED)이면 "현재 없음"으로 본다.
3. 활성 상태가 있고 `p.decisionVersion < state.alertVersion`이면 `STALE`이다.
4. 활성 상태가 있고 버전이 같으면 `DUPLICATE`다. 재전달된 엔트리로 보고, `logs.insert`만 다시 호출(멱등)한 뒤 끝낸다.
5. 활성 상태가 있고 `dedup.shouldSend(fingerprint)`가 false면 `SUPPRESSED`다. 상태는 바꾸지 않는다.
6. `deliverer.deliver(...)`를 호출한다.
7. 결과에 따라 상태를 저장한다. SENT면 `status=SENT, lastSentAt=now`이고 dedup.remember를 호출한다. NO_SESSION·NO_SUBSCRIBER면 `status=FAILED`이다. alertLogId는 활성 상태가 있으면 그대로 쓰고, 없으면 `randomUUID()`로 새로 만든다.
8. 새 에피소드면 `logs.insert`, 기존 에피소드면 `logs.updateStatus(SENT, …)`를 호출한다. FAILED이면 상태 FAILED로 insert한다.
9. Redis·PG 예외는 그대로 던진다(호출자가 Ack하지 않으므로 재시도된다).

**handleClearRequested:** 활성 상태가 없으면 `NO_ACTIVE_ALERT`를 반환한다. 있으면 `createCleared`로 메시지를 만들어 전달한다(세션이 없어도 계속 진행한다). 이어서 `transition([SENT, ACKNOWLEDGED, PENDING], 'CLEARED', {clearedAt})`, `dedup.forget`, `logs.updateStatus(CLEARED, now, {clearReason})`를 수행한다.

- [ ] **Step 1: 실패하는 테스트**

```ts
test('first request publishes ISSUED to session gateway and records SENT', async () => {
  sessions.set('VEH-001', {gatewayId: 'gw-1', sessionId: 's-1', ...});
  expect((await svc.handleRequested(requested)).outcome).toBe('SENT');
  expect(publisher.frames).toEqual([{gatewayId: 'gw-1', frame: {targetVehicleId: 'VEH-001', sessionId: 's-1',
    message: expect.objectContaining({type: 'YIELD_ALERT_ISSUED', alertVersion: 3})}}]);
  expect(states.get('ALT-001')).toMatchObject({status: 'SENT', alertVersion: 3, lastSentAt: NOW});
  expect(logs.rows).toHaveLength(1);
});
test('no session marks FAILED and logs it', ...);
test('no gateway subscriber marks FAILED', ...);            // publisher receivers = 0
test('stale request is dropped', ...);                      // v4 저장 후 v3 → STALE, 프레임 0
test('redelivered request does not publish twice', ...);    // 같은 v3 두 번 → 두 번째 DUPLICATE, 프레임 1, logs.insert 2회(멱등)
test('same action and direction within dedup window is SUPPRESSED', ...);   // v3 후 v4 같은 지문
test('changed approachDirection re-sends as update in same episode', ...);  // v4 다른 방향 → SENT, 같은 alertLogId, logs.updateStatus
test('already expired request is not sent', ...);
test('clear sends CLEARED, transitions, forgets dedup', ...);
test('clear without active alert is NO_ACTIVE_ALERT', ...);
test('re-request after clear starts new episode', ...);     // clear 후 같은 alertId v5 → SENT, 새 alertLogId, 억제 안 됨
```

- [ ] **Step 2:** `npx jest test/unit/alert/yield-alert-service.test.ts` → FAIL
- [ ] **Step 3:** 구현
- [ ] **Step 4:** PASS
- [ ] **Step 5:** Commit `feat: add yield alert issue/update/clear flow`

---

### Task 6: AcknowledgementService와 RouteUpdateService

**Files:**
- Create: `src/alert/acknowledgement-service.ts`, `src/route/route-update-service.ts`
- Test: `test/unit/alert/acknowledgement-service.test.ts`, `test/unit/route/route-update-service.test.ts`

**Interfaces:**
- `AcknowledgementService(deps: {states, logs})`
  - `handleAck(p: YieldAlertAcknowledgedPayload): Promise<void>`: 상태가 있고, `p.vehicleId === state.targetVehicleId`이고, `isCurrentVersion(state, p.alertVersion)`이면 `transition(['SENT'], 'ACKNOWLEDGED', {acknowledgedAt: p.receivedAt}, p.alertVersion)`를 호출한다. 성공했을 때만 `logs.updateStatus(ACKNOWLEDGED, p.receivedAt)`를 호출한다. 이미 ACKNOWLEDGED이면 아무것도 하지 않는다(멱등).
  - `isCurrentVersion(state, alertVersion): boolean`
- `RouteUpdateService(deps: {routes: RouteVersionStore, deliverer, factory})`
  - `handleRouteUpdate(p): Promise<DeliveryResult>`: `isLatestVersion`이 false면 `STALE`이다. true면 전달하고, 결과가 SENT일 때만 `markDelivered`를 호출한다. 세션이 없으면 표시하지 않으므로 다음 버전이나 재요청 때 다시 전달할 수 있다.
  - `isLatestVersion(dispatchId, routeVersion): Promise<boolean>`

- [ ] **Step 1: 실패하는 테스트**: `ack for current version transitions to ACKNOWLEDGED with receivedAt`, `ack for older alertVersion is ignored`, `ack from different vehicle is ignored`, `ack for unknown alert is ignored`, `duplicate ack is idempotent (logs updated once)`, `route update sends EMERGENCY_ROUTE_UPDATED to emergency vehicle`, `older routeVersion is STALE`, `undelivered route does not advance version`
- [ ] **Step 2:** FAIL 확인
- [ ] **Step 3:** 구현
- [ ] **Step 4:** PASS
- [ ] **Step 5:** Commit `feat: add acknowledgement and route update services`

---

### Task 7: AlertEventRouter와 AlertEventSubscriber

**Files:**
- Create: `src/consumer/alert-event-router.ts`, `src/consumer/alert-event-subscriber.ts`, `src/infra/logger.ts`
- Test: `test/unit/consumer/alert-event-router.test.ts`, `test/integration/consumer/alert-event-subscriber.test.ts`

**Interfaces:**
- `AlertEventRouter(services)`: `route(event: AlertStreamEvent): Promise<void>`, `isSupported(eventType: string): boolean`. 이벤트 유형별 분기는 설계서 59–60p 표를 따른다.
- `AlertEventSubscriber(redis, config, router)`
  - `start()`: `XGROUP CREATE <stream> <group> $ MKSTREAM`을 실행한다(BUSYGROUP은 무시). 시작 지점이 `$`인 이유는 배포 이전의 오래된 알림을 다시 보내지 않기 위해서다. 블로킹 전용 연결은 `redis.duplicate()`로 따로 연다.
  - `onMessage(id, fields)`: 파싱이 실패하면(`ContractError`) DLQ에 `XADD`한 뒤 Ack한다. 라우팅 중 예외가 나면 Ack하지 않고 로그만 남긴다. 성공하면 Ack한다.
  - `acknowledge(entryId)`
  - `reclaimPending(idleMs)`: 루프 매 회차 시작에 `XAUTOCLAIM`을 실행한다. 회수한 엔트리는 `XPENDING <stream> <group> IDLE 0 <id> <id> 1`로 전달 횟수를 확인하고, `> maxDeliveries`이면 DLQ에 넣은 뒤 Ack한다.
  - `stop()`: 블로킹 연결을 끊고 루프를 기다린다.
  - `get ready(): boolean`
  - DLQ 엔트리 필드: `sourceId`, `reason`, `event`(원본 문자열)

- [ ] **Step 1: 실패하는 테스트**
  - router 단위: 4개 eventType 각각이 해당 서비스 메서드로 가는지, 지원하지 않는 유형에서 `isSupported`가 false인지
  - subscriber 통합(무작위 스트림명, 가짜 router):
    - `valid entry is routed then XACKed (pending count 0)`
    - `malformed entry goes to DLQ and is acked`
    - `router throw leaves entry pending, reclaimed after idle`(`PENDING_MIN_IDLE_MS=50`)
    - `entry exceeding maxDeliveries goes to DLQ`
    - `stop() returns within blockMs+1s`
- [ ] **Step 2:** FAIL 확인
- [ ] **Step 3:** 구현. 루프 구조(회수 → XREADGROUP → 순차 처리)는 `../Emergency-Worker/src/consumer/stream-consumer.ts`를 참고하되, 재시도 예산·H3 관련 부분은 가져오지 않는다.
- [ ] **Step 4:** PASS
- [ ] **Step 5:** Commit `feat: add alert stream subscriber and router`

---

### Task 8: AlertExpirationScheduler

**Files:**
- Create: `src/alert/alert-expiration-scheduler.ts`
- Test: `test/unit/alert/alert-expiration-scheduler.test.ts`

**Interfaces:**
- `AlertExpirationScheduler(deps: {states, deliverer, logs, factory, dedup, clock})`
  - `start(intervalMs): void`: `setInterval`로 돌린다. 이전 sweep이 끝나지 않았으면 이번 회차는 건너뛴다.
  - `sweepExpired(now: Date): Promise<number>`: 만료 처리한 건수를 반환한다. `dueForExpiry(now, 100)`의 각 alertId에 대해 `transition([SENT, ACKNOWLEDGED], 'EXPIRED', {clearedAt: now})`를 시도하고, 성공하면 `createCleared(..., 'EXPIRED')`를 전달하고 `dedup.forget`과 `logs.updateStatus(EXPIRED)`를 호출한다. 한 건에서 예외가 나도 나머지는 계속 처리한다.
  - `stop(): void`

- [ ] **Step 1: 실패하는 테스트**: `expired SENT alert becomes EXPIRED and client gets CLEARED(reason EXPIRED)`, `expired ACKNOWLEDGED alert also expires`, `not-yet-expired alert untouched`, `sweep skips cleared alert`(이미 CLEARED → 전달 0), `one failing alert does not stop others`
- [ ] **Step 2:** FAIL 확인
- [ ] **Step 3:** 구현
- [ ] **Step 4:** PASS
- [ ] **Step 5:** Commit `feat: add alert expiration scheduler`

---

### Task 9: 워커 조립, 헬스 서버, 컨테이너

**Files:**
- Create: `src/worker.ts`, `src/main.ts`, `src/infra/health-server.ts`, `Dockerfile.local`, `deploy/compose.alert-worker.yml`, `README.md`
- Test: `test/integration/worker.test.ts`

**Interfaces:**
- `class Worker`
  - `static create(config): Worker`: 모든 구성요소를 실제 구현으로 조립한다.
  - `start()`: PG ping → subscriber.start → scheduler.start → health server listen
  - `stop()`: 시작의 역순으로 정리하고 Redis·PG 연결을 닫는다.
  - `readiness(): Promise<{ready: boolean; checks: Record<string, boolean>}>`: 검사 항목은 redis ping, pg ping, subscriber.ready다.
- 헬스 서버: `GET /livez`는 항상 200이다. `GET /readyz`는 준비됐으면 200, 아니면 503이고, 본문은 checks JSON이다.
- `main.ts`: SIGTERM/SIGINT를 받으면 `stop()` 후 exit 0으로 끝낸다. `ConfigError`가 나면 이슈를 출력하고 exit 1로 끝낸다.
- `deploy/compose.alert-worker.yml`: 상위 compose에 `-f`로 얹어서 쓴다. 상대 경로는 첫 번째 파일 기준이므로 build context는 `./Alert-Worker`이다. 환경 변수는 `REDIS_URL: redis://redis:6379/0`, `DATABASE_URL: postgres://evacs:evacs@postgres:5432/evacs`, `HEALTH_PORT: '18083'`, `CONSUMER_NAME: alert-worker-local`이고, 포트는 `18083:18083`, depends_on은 redis·postgres healthy, healthcheck는 `/readyz`이다.
- README: 마이그레이션 적용, 로컬 실행, compose 실행 명령, 환경 변수 표, Gateway 연동 계약(채널·프레임·ACK 이벤트 형식)을 적는다. 이 계약은 Gateway 후속 작업의 입력이 된다.

- [ ] **Step 1: 실패하는 테스트** (`worker.test.ts`, 무작위 스트림명): `readyz is 200 after start and 503 after redis disconnect`, `stop() resolves and closes connections`
- [ ] **Step 2:** FAIL 확인
- [ ] **Step 3:** 구현
- [ ] **Step 4:** PASS. 이어서 `npm run build && docker build -f Dockerfile.local -t evacs-alert-worker .` → 빌드 성공
- [ ] **Step 5:** Commit `feat: wire alert worker with health server and container`

---

### Task 10: 샌드박스 E2E 검증

**Files:**
- Create: `test/e2e/sandbox.e2e-spec.ts`

**Interfaces:**
- Consumes: `Worker.create(loadConfig({...env, ALERT_STREAM: 무작위, CONSUMER_GROUP: 무작위}))`
- Gateway 흉내: `SET ws:vehicle:<V> {"vehicleId":V,"gatewayId":G,"sessionId":"s-1",...}`와 `SUBSCRIBE channel:gateway:<G>`. ACK는 `XADD <stream> * event <YIELD_ALERT_ACKNOWLEDGED envelope>`로 넣는다.

- [ ] **Step 1: 시나리오 테스트 작성** (각 단계에서 프레임 수신을 최대 3초까지 기다린다)
  1. REQUESTED v1 → `YIELD_ALERT_ISSUED` 프레임 수신, Redis 상태 SENT, PG 행 SENT
  2. ACK(alertVersion 1) → 상태 ACKNOWLEDGED, PG `acknowledged_at` = receivedAt
  3. 같은 내용 REQUESTED v2(10초 안) → 프레임 없음
  4. 방향이 바뀐 REQUESTED v3 → ISSUED 프레임(alertVersion 3), 같은 alert_log_id
  5. CLEAR(EMERGENCY_PASSED) → `YIELD_ALERT_CLEARED` 프레임, 상태 CLEARED, PG `cleared_at`
  6. 별도 차량에 expiresAt = now+1.5s로 요청 → 약 2.5초 안에 CLEARED(reason EXPIRED) 프레임, 상태 EXPIRED
  7. ROUTE_UPDATE v2, v1 순서로 보냄 → 프레임 1개(v2)
  8. 구독하지 않은 gatewayId로 세션 등록 → 상태 FAILED
  9. 첫 REQUESTED 프레임 수신까지의 지연을 출력한다(참고 지표. 설계서의 50ms는 클라이언트 쪽 기준이므로 단언하지 않는다).
- [ ] **Step 2:** `docker compose -f ../docker-compose.local.yml up -d --wait redis postgres` 후 `npx jest --selectProjects e2e` → PASS
- [ ] **Step 3: 실제 Emergency Worker 출력과 호환되는지 확인.** 전체 샌드박스(`../scripts/run-local.sh`)를 실행한 상태에서 `docker compose -f ../docker-compose.local.yml -f deploy/compose.alert-worker.yml up -d --build --wait alert-worker`를 실행하고, README의 Emergency Worker 절차에 따라 출동 이벤트를 넣는다. 그 뒤 다음을 확인한다.
  - `docker compose ... logs alert-worker`에 `ContractError`가 없다.
  - `XLEN stream:alert-worker:dlq`가 0이다.
  - `XPENDING stream:alert:event alert-worker`의 pending 수가 0이다.
  - 실제 Gateway는 `ws:vehicle:*`를 쓰지 않으므로 알림은 FAILED(NO_SESSION)로 기록되는 것이 정상이다. 실제 이벤트가 파싱·라우팅되었다는 증거로 보면 된다.
  - `curl -fsS http://127.0.0.1:18083/readyz` → 200
- [ ] **Step 4:** 전체 `npm test && npm run typecheck`가 PASS인지 확인한다.
- [ ] **Step 5:** Commit `test: add sandbox e2e for alert worker`

---

## 후속 작업 (이번 범위 밖, README에 기록)

- Gateway에 다음을 추가한다: 연결 시 `ws:vehicle:{id}` SET(연결 해제 시 sessionId가 같을 때만 DEL), `channel:gateway:{gatewayId}` SUBSCRIBE 후 `frame.message.type`을 기준으로 소켓 이벤트(`yield.alert` / `yield.alert.cleared` / route)로 emit, `yield.alert.ack`를 `stream:alert:event`에 `YIELD_ALERT_ACKNOWLEDGED` envelope로 XADD.
- 위 작업이 끝나면 Emergency Worker의 `evacs:stream:yield-alert` 이중 발행과 Gateway의 해당 스트림 소비를 제거한다(그 전까지는 같은 알림이 두 경로로 갈 수 있다).
