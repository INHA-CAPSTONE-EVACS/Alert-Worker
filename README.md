# EVACS Alert Worker

Emergency Worker가 판별한 양보 알림 요청을 받아, 대상 차량이 연결된 Gateway로 클라이언트 메시지를 전달하는
워커다. 알림의 현재 상태는 Redis에, 전송 이력은 PostgreSQL `alert_delivery_log`에 남긴다.
(상세설계서 58–64p, 이벤트 명세 96–108p, 요구사항 F-ALT-01~05)

```text
Emergency Worker ─XADD─▶ stream:alert:event ─▶ Alert Worker ─PUBLISH─▶ channel:gateway:{gatewayId} ─▶ Gateway ─▶ 차량
Gateway(차량 ACK) ─XADD─▶ stream:alert:event ─┘        │
                                                      ├─ Redis   alert:{alertId}:state, alert:expiry, …
                                                      └─ Postgres alert_delivery_log
```

## 처리 규칙

| 이벤트 | 처리 |
|---|---|
| `YIELD_ALERT_REQUESTED` | 낮은 `decisionVersion`, 같은 버전 재전달, 만료된 요청은 보내지 않는다. 10초 안에 같은 안내(권장 행동 + 접근 방향)가 반복되면 보내지 않지만, 그 요청은 `refresh`로 남긴다. 나머지는 PENDING으로 예약한 뒤 `YIELD_ALERT_ISSUED`를 보내고 SENT/FAILED로 확정한다. `alertVersion`은 `decisionVersion`과 같다. |
| `YIELD_ALERT_CLEAR_REQUESTED` | 활성 알림이 있으면 `YIELD_ALERT_CLEARED`를 보내고 CLEARED로 바꾼다. 차량이 오프라인이어도 알림은 종료한다. envelope `occurredAt`이 현재 결정(`decidedAt`)보다 오래된 해제는 이전 에피소드의 것이므로 무시한다. |
| `ROUTE_UPDATE_REQUESTED` | 이미 전달한 `routeVersion`보다 높을 때만 `EMERGENCY_ROUTE_UPDATED`를 긴급차량에 보낸다. |
| `YIELD_ALERT_ACKNOWLEDGED` | 차량과 `alertVersion`이 현재 알림과 일치할 때만 ACKNOWLEDGED로 바꾼다. |
| 만료 스케줄러 | 1초마다 실행한다. 억제된 더 새로운 결정(`refresh`)이 있으면 만료 2초 전에 그 내용으로 다시 발행한다. 없으면 `expiresAt`이 지난 SENT/ACKNOWLEDGED 알림을 EXPIRED로 바꾸고 `YIELD_ALERT_CLEARED(reason=EXPIRED)`를 보낸다. |

Gateway 세션(`ws:vehicle:{id}`)이 없거나 채널을 구독 중인 Gateway가 없으면(PUBLISH 수신자 0명) FAILED로 기록한다.
Redis 상태 전이는 항상 PostgreSQL 기록보다 먼저 일어난다. PostgreSQL 오류로 다시 처리되는 엔트리는 Redis에 이미
반영된 내용을 멱등하게 다시 기록하므로, 이력은 늦어질 수는 있어도 사라지지 않고 메시지가 두 번 나가지도 않는다.
종료된 이력 행(CLEARED/EXPIRED/FAILED)은 늦게 온 쓰기로 되살아나지 않는다. 클라이언트는 이미 받은 것보다 낮은
`alertVersion`을 무시해야 한다(여러 인스턴스로 운영할 때 필요하다).

형식이 잘못된 엔트리와 `MAX_DELIVERIES`를 넘긴 엔트리는 `stream:alert-worker:dlq`로 보낸다. 처리 중 예외가 나면
Ack하지 않으므로 `PENDING_MIN_IDLE_MS`가 지난 뒤 다시 처리된다.

## Gateway 연동 계약

현재 Gateway는 아래 계약을 아직 구현하지 않았다. Gateway 후속 작업의 입력으로 쓴다.

- **세션 등록:** 연결되면 `SET ws:vehicle:{vehicleId} {"vehicleId","gatewayId","sessionId","connectedAt"}`.
  끊기면 sessionId가 같을 때만 DEL한다.
- **전달 수신:** `SUBSCRIBE channel:gateway:{gatewayId}`. 메시지는 `{"targetVehicleId","sessionId","message"}` JSON이고,
  `message.type`은 `YIELD_ALERT_ISSUED` | `YIELD_ALERT_CLEARED` | `EMERGENCY_ROUTE_UPDATED`이다. `eventType`에도
  같은 값이 들어 있다. client-core 형식이다.
- **ACK 전달:** 클라이언트의 `yield.alert.ack`를 받으면
  `XADD stream:alert:event * event '{"eventId","eventType":"YIELD_ALERT_ACKNOWLEDGED","schemaVersion":1,"occurredAt","producer":"gateway","payload":{"alertId","vehicleId","alertVersion","receivedAt"}}'`
- 이 연동이 끝나면 Emergency Worker의 `evacs:stream:yield-alert` 이중 발행과 Gateway의 해당 스트림 소비를 제거한다.
  그 전까지는 같은 알림이 두 경로로 전달될 수 있다.

## 실행

```sh
# 0) 샌드박스 Redis(6401)와 Postgres(5433)
docker compose -f ../docker-compose.local.yml up -d --wait redis postgres

# 1) 마이그레이션 (최초 한 번)
docker compose -f ../docker-compose.local.yml exec -T postgres \
  psql -U evacs -d evacs -v ON_ERROR_STOP=1 < migrations/001_alert_delivery_log.sql

# 2) 호스트에서 실행
npm ci && npm run build
DATABASE_URL=postgres://evacs:evacs@127.0.0.1:5433/evacs npm start

# 또는 컨테이너로 (캡스톤 루트에서)
docker compose -f docker-compose.local.yml -f Alert-Worker/deploy/compose.alert-worker.yml up -d --build --wait alert-worker
curl -fsS http://127.0.0.1:18083/readyz
```

`/livez`는 프로세스 상태를, `/readyz`는 Redis·PostgreSQL(테이블 포함)·스트림 구독 상태를 반환한다.

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | PostgreSQL 접속 URL |
| `REDIS_URL` | `redis://localhost:6401` | |
| `HEALTH_PORT` | `18083` | |
| `ALERT_STREAM` | `stream:alert:event` | 입력 스트림 |
| `CONSUMER_GROUP` | `alert-worker` | 처음 생성할 때 `$`부터 읽는다 |
| `CONSUMER_NAME` | hostname | 인스턴스마다 달라야 한다 |
| `DLQ_STREAM` | `stream:alert-worker:dlq` | |
| `READ_COUNT` / `BLOCK_MS` | `10` / `1000` | XREADGROUP |
| `PENDING_MIN_IDLE_MS` | `30000` | 이 시간이 지난 미처리 엔트리를 다시 가져온다 |
| `MAX_DELIVERIES` | `5` | 넘기면 DLQ로 보낸다 |
| `EXPIRY_SWEEP_MS` | `1000` | 만료 검사 주기 |
| `DEDUP_TTL_MS` | `10000` | 재발송 억제 시간 |
| `LOG_LEVEL=debug`, `LOG_SILENT=true` | | 로그 출력 |

## 테스트

```sh
npm run test:unit
npm run test:integration   # 샌드박스 Redis/Postgres 필요, 무작위 키·스트림 이름 사용
npm run test:e2e           # 워커 전체 + Gateway 흉내
```

## 설계서와 다르게 정한 점

- `alert_delivery_log.target_vehicle_id`, `dispatch_id`는 VARCHAR이고 FK가 없다. SUMO 차량 ID는 UUID가 아니고,
  `dispatch` 테이블도 아직 없다.
- 로그 행은 알림 에피소드마다 하나다. alertId는 (출동, 차량)마다 고정이라, 해제된 뒤 다시 요청되면 새 행을 만든다.
  `alert_id`, `alert_version`, `clear_reason` 컬럼을 추가했다.
- 중복 억제는 권장 행동에 접근 방향까지 비교한다. 안내 내용이 달라지면 갱신해야 하기 때문이다(F-ALT-04).
