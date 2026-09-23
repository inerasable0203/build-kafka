# Build Kafka from Scratch

Kafka를 가져다 쓰는 대신, 작은 메시지 로그부터 시작해 분산 스트리밍 플랫폼의 핵심을 직접 구현하는 학습 프로젝트입니다.

> 목표는 Apache Kafka를 그대로 복제하는 것이 아니라, 기능을 하나씩 추가하면서 **왜 Kafka가 로그, offset, partition, replication이라는 구조를 택했는지** 이해하는 것입니다.

## 현재 상태

**Milestone 4 완료:** log segment와 sparse offset index

- 레코드를 append-only 방식으로 저장
- 0부터 증가하는 offset 발급
- 프로세스 재시작 후 로그와 다음 offset 복구
- CRC32 checksum으로 데이터 손상 감지
- TCP 위에서 동작하는 길이 prefix binary frame
- `PRODUCE`와 `FETCH` request/response
- correlation ID를 이용한 요청과 응답 연결
- 여러 client connection 동시 처리
- 잘못되거나 16 MiB를 초과한 frame 거부
- topic 생성과 영속적인 partition metadata
- topic별 여러 partition과 독립 offset
- 명시적 partition, key hash, null key round-robin 선택
- partition별 segment 분할과 sparse index를 이용한 offset 검색
- 재시작 시 segment 검증과 index 재구성

아직 consumer group, replication, retention은 없습니다.

## 빠른 실행

요구 사항: JDK 17 이상

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

첫 번째 터미널에서 broker를 실행합니다.

```bash
java -cp out MiniKafka broker data 9092
```

다른 터미널에서 메시지를 저장하고 읽습니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9092 orders 3
java -cp out MiniKafka describe-topic 127.0.0.1 9092 orders

java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-1 "ordered coffee"
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-1 "ordered tea"
java -cp out MiniKafka consume 127.0.0.1 9092 orders 1 0
```

출력 예시:

```text
created topic orders with 3 partitions
topic=orders partitions=3
partition=1 offset=0
partition=1 offset=1
0    customer-1    ordered coffee
1    customer-1    ordered tea
```

마지막 인자로 partition을 지정할 수도 있습니다.

```bash
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-3 "explicit partition" 2
```

partition을 생략하면 key가 있는 레코드는 key hash로 partition을 정합니다. null key(`-`)는 같은 producer 프로세스 안에서 round-robin으로 선택합니다. 이 CLI는 명령마다 새 프로세스이므로 여러 null-key 명령의 분산을 관찰하려면 이후 장기 실행 producer 또는 self-test를 사용해야 합니다.

## 현재 구조

```text
.
├── README.md
├── docs
│   └── milestones
│       ├── 01-persistent-log.md
│       ├── 02-tcp-broker.md
│       ├── 03-topics-partitions.md
│       └── 04-segments-index.md
└── src
    ├── Broker.java          # TCP 연결과 request 처리
    ├── MiniKafka.java       # CLI와 network client
    ├── PartitionLog.java    # append-only disk log
    └── Protocol.java        # binary frame과 자료형 encoding

data/                       # 실행 시 생성되며 Git에는 포함하지 않음
└── <topic>/
    ├── topic.meta          # partition 개수
    └── <partition>/
        ├── 0.log           # base offset이 0인 segment
        ├── 0.index         # (offset, 파일 내 위치) 쌍
        ├── 123.log         # 다음 segment의 base offset 예시
        └── 123.index
```

Milestone 1의 단일 파일을 저장소, protocol, broker, client의 네 역할로 분리했습니다. 각 클래스는 아직 interface나 별도 계층 없이 한 가지 구현만 가집니다.

## 레코드 저장 형식

각 레코드는 다음 순서로 파일 끝에 추가됩니다. 모든 숫자는 Java `DataOutputStream`의 big-endian 형식입니다.

| 필드 | 크기 | 설명 |
|---|---:|---|
| payload size | 4 bytes | 뒤따르는 payload의 길이 |
| offset | 8 bytes | partition 내부에서 증가하는 레코드 번호 |
| timestamp | 8 bytes | 생성 시각(epoch milliseconds) |
| key size | 4 bytes | null이면 `-1` |
| value size | 4 bytes | value 길이 |
| key | variable | 선택 사항 |
| value | variable | 메시지 본문 |
| CRC32 | 4 bytes | payload 손상 검사용 checksum |

현재 `fsync`를 매 append마다 호출합니다. 느리지만 첫 단계에서는 durability 동작을 명확하게 확인하기 위한 선택입니다. batching은 뒤 단계에서 측정 후 추가합니다.

## Segment와 index

partition 로그는 기본 1 MiB에 도달하면 다음 append 전에 새 segment를 엽니다. 한 레코드 때문에 실제 파일 크기가 기준을 조금 넘을 수 있습니다. 학습용으로 작은 크기를 실험하려면 broker 실행 시 마지막 인자를 지정합니다.

```bash
java -cp out MiniKafka broker data 9092 256
```

segment 파일 이름은 해당 파일의 첫 offset이며, 16개 레코드마다 `.index`에 `(offset, byte position)`을 기록합니다. fetch는 segment 목록과 index를 각각 이진 탐색한 뒤 최대 15개 레코드만 건너뛰고 요청한 위치부터 읽습니다. 응답 크기가 16 MiB를 넘기 전에 fetch를 끊으므로 뒤의 레코드는 다음 offset으로 다시 조회합니다.

Milestone 3 형식인 `<partition>.log`는 처음 열 때 `<partition>/0.log`로 옮깁니다. 재시작 시 index는 로그를 검사하며 다시 만들기 때문에 시작 시간은 전체 레코드 수에 비례합니다.

## TCP frame 형식

모든 request와 response 앞에는 뒤따르는 body 크기를 나타내는 4-byte 정수가 붙습니다.

```text
request  = frame size | correlation ID | API key | API payload
response = frame size | correlation ID | status  | response payload
```

현재 API는 `PRODUCE`, `FETCH`, `CREATE_TOPIC`, `METADATA`입니다. 이것은 학습용 protocol이며 실제 Apache Kafka wire protocol과 호환되지 않습니다. 자세한 필드 구성은 각 마일스톤 문서에 기록합니다.

## 마일스톤 문서

- [Milestone 1 — 영속 append-only log](docs/milestones/01-persistent-log.md)
- [Milestone 2 — TCP broker와 protocol](docs/milestones/02-tcp-broker.md)
- [Milestone 3 — topic과 partition](docs/milestones/03-topics-partitions.md)
- [Milestone 4 — log segment와 sparse index](docs/milestones/04-segments-index.md)

## 구현 순서

각 milestone은 앞 단계의 결과를 버리지 않고 확장합니다. 체크박스는 저장소의 실제 진행 상태를 나타냅니다.

### Milestone 1 — 영속 로그 `[완료]`

**배우는 것:** Kafka는 메시지를 삭제하며 전달하는 queue가 아니라, consumer가 offset으로 위치를 정하는 append-only log라는 점

- [x] binary record encoding/decoding
- [x] append와 순차 offset
- [x] 재시작 시 offset 복구
- [x] checksum 검증
- [x] 불완전한 tail record 제거
- [x] offset 기반 fetch
- [x] 재실행 가능한 self-test

**완료 조건:** 두 레코드를 쓴 뒤 프로세스를 다시 열어 offset 1부터 읽고, 다음 append가 offset 2를 받아야 합니다.

완성된 코드는 Git tag `milestone-1`에서 확인할 수 있습니다.

### Milestone 2 — TCP broker와 protocol `[완료]`

**배우는 것:** producer와 consumer가 저장소에 직접 접근하지 않고 broker에 request/response를 보내는 이유

- [x] long-running broker process
- [x] 길이 prefix가 있는 binary frame
- [x] `PRODUCE` request/response
- [x] `FETCH` request/response
- [x] correlation id로 요청과 응답 연결
- [x] 잘못된 frame과 너무 큰 request 거부
- [x] producer/consumer CLI를 TCP client로 변경

**완료 조건:** broker와 client를 서로 다른 프로세스로 실행하고, 여러 client가 메시지를 저장하고 읽을 수 있어야 합니다.

완성된 코드는 Git tag `milestone-2`에서 확인할 수 있습니다.

### Milestone 3 — topic과 partition `[완료]`

**배우는 것:** Kafka가 전체 순서 대신 partition 내부 순서만 보장하여 처리량을 확장하는 방식

- [x] topic metadata와 생성 명령
- [x] topic별 여러 partition
- [x] 명시적 partition 선택
- [x] key hash 기반 partition 선택
- [x] null key round-robin 선택
- [x] partition별 독립 offset

**완료 조건:** 같은 key는 항상 같은 partition으로 가고, 각 partition의 offset이 독립적으로 증가해야 합니다.

완성된 코드는 Git tag `milestone-3`에서 확인할 수 있습니다.

### Milestone 4 — log segment와 sparse index `[완료]`

**배우는 것:** 로그 전체를 매번 스캔하지 않고 큰 데이터를 관리하는 방법

- [x] 설정한 크기에서 active segment 교체
- [x] segment 파일명을 base offset으로 관리
- [x] offset → file position sparse index
- [x] binary search로 대상 segment/index 탐색
- [x] 재시작 시 segment와 index 검증 및 복구

**완료 조건:** 여러 segment가 생성된 뒤에도 임의 offset fetch가 정확해야 하며, 전체 로그 선형 탐색 없이 대상 위치를 찾아야 합니다.

### Milestone 5 — retention과 log compaction

**배우는 것:** consumer가 읽었는지와 무관하게 broker가 저장 공간을 관리하는 방식

- [ ] 보존 시간 기반 오래된 segment 삭제
- [ ] 보존 용량 기반 오래된 segment 삭제
- [ ] key별 최신 값만 남기는 compaction
- [ ] tombstone 레코드

**완료 조건:** active segment를 손상시키지 않고 정책에 해당하는 closed segment만 정리해야 합니다.

### Milestone 6 — consumer offset과 consumer group

**배우는 것:** queue의 경쟁 소비와 pub/sub을 consumer group 하나로 표현하는 방식

- [ ] group별 committed offset 저장
- [ ] offset commit/fetch API
- [ ] group membership과 heartbeat
- [ ] partition assignment
- [ ] consumer join/leave 시 rebalance

**완료 조건:** 같은 group의 consumer들은 partition을 나눠 처리하고, 다른 group은 동일한 레코드를 독립적으로 읽어야 합니다. 재시작한 consumer는 committed offset부터 이어서 읽어야 합니다.

### Milestone 7 — replication

**배우는 것:** partition leader와 follower가 동일한 순서의 로그를 유지하는 방식

- [ ] broker id와 cluster metadata
- [ ] partition별 leader/follower 역할
- [ ] follower fetch와 로그 복제
- [ ] leader end offset과 follower lag 추적
- [ ] ISR(in-sync replicas)의 단순화된 모델
- [ ] `acks=0`, `acks=1`, `acks=all`

**완료 조건:** replication factor 3에서 leader에 쓴 레코드가 follower에 같은 offset과 순서로 복제되어야 합니다.

### Milestone 8 — controller와 failover

**배우는 것:** broker 장애 시 누가 새 leader를 선택하고 metadata를 갱신하는지

- [ ] broker heartbeat와 failure detection
- [ ] 단일 controller 선출
- [ ] ISR 안에서 새 partition leader 선출
- [ ] producer/consumer metadata 갱신
- [ ] 재접속과 retry

**완료 조건:** leader broker를 종료한 뒤 follower가 leader가 되고, client가 metadata를 갱신해 produce/fetch를 계속해야 합니다.

### Milestone 9 — 처리량과 전달 보장

**배우는 것:** batching과 retry가 성능을 높이는 동시에 중복 가능성을 만드는 이유

- [ ] producer batching
- [ ] broker의 batch append/fetch
- [ ] configurable flush policy
- [ ] retry와 backoff
- [ ] idempotent producer의 단순화된 sequence number
- [ ] at-most-once / at-least-once 시나리오 테스트

**완료 조건:** 단건 처리보다 batch 처리의 throughput이 개선됨을 측정하고, retry에서 발생하는 중복을 테스트로 재현해야 합니다.

### Milestone 10 — 선택 과제

핵심 원리를 구현한 다음 필요한 주제만 선택합니다.

- [ ] 실제 Kafka protocol 일부 호환
- [ ] zero-copy 전송
- [ ] TLS와 인증/인가
- [ ] metrics와 운영 도구
- [ ] transaction과 exactly-once semantics
- [ ] Raft 기반 metadata quorum

이 단계들은 핵심 학습에 필수는 아니며, 앞 단계의 병목이나 관심사가 분명할 때 진행합니다.

## 구현 원칙

1. **한 번에 개념 하나만 추가한다.** 저장소, 네트워크, 분산 합의를 동시에 만들지 않습니다.
2. **각 단계는 실행 가능해야 한다.** 새 기능은 최소 하나의 재현 가능한 검증 명령과 함께 완료합니다.
3. **표준 라이브러리부터 사용한다.** 학습 대상 자체를 가리는 framework는 도입하지 않습니다.
4. **측정 전에는 최적화하지 않는다.** 우선 정확하게 만들고, 병목을 재현한 뒤 batching/index/zero-copy를 추가합니다.
5. **Kafka와 다른 선택을 기록한다.** 단순화를 숨기지 않고 현재 한계와 다음 확장 지점을 README에 남깁니다.

## 범위 밖

현재 목표는 학습용 구현입니다. 다음 용도로 사용하면 안 됩니다.

- production workload
- Apache Kafka 대체
- 실제 Kafka client와의 호환이 필요한 서비스
- 데이터 유실이 허용되지 않는 시스템

## 참고 자료

- [Apache Kafka Design](https://kafka.apache.org/41/design/design/)
- [Apache Kafka Protocol Guide](https://kafka.apache.org/41/design/protocol/)
- [Build Your Own Database from Scratch](https://build-your-own.org/database/) — append-only storage와 index를 공부할 때 참고
