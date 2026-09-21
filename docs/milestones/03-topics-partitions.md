# Milestone 3 — topic과 partition

하나의 topic을 여러 append-only log로 나누고 각 로그가 독립적인 offset을 갖도록 확장했습니다. producer는 레코드를 보낼 partition을 결정하고 consumer는 특정 topic-partition을 읽습니다.

## 학습 목표

- topic이 논리적인 레코드 이름이고 partition이 실제 순서 보장 단위인 이유
- partition을 늘리면 병렬 처리가 가능하지만 topic 전체 순서는 사라지는 이유
- 같은 key를 같은 partition에 보내 key별 순서를 유지하는 방법
- partition마다 offset 0부터 독립적으로 증가하는 이유

## 실행

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

터미널 1:

```bash
java -cp out MiniKafka broker data 9092
```

터미널 2:

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9092 orders 3
java -cp out MiniKafka describe-topic 127.0.0.1 9092 orders

java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-1 "ordered coffee"
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-1 "ordered tea"
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-2 "explicit partition" 2

java -cp out MiniKafka consume 127.0.0.1 9092 orders 1 0
java -cp out MiniKafka consume 127.0.0.1 9092 orders 2 0
```

## 저장 구조

```text
data/orders/
├── topic.meta    # 3
├── 0.log
├── 1.log
└── 2.log
```

`topic.meta`는 broker가 재시작되어도 partition 개수를 복구하기 위한 작은 metadata 파일입니다. 각 `<partition>.log`는 Milestone 1에서 만든 `PartitionLog` 형식을 그대로 사용합니다.

## Partition 선택

producer가 partition을 명시하지 않으면 다음 규칙을 적용합니다.

```text
key 있음  → floorMod(Arrays.hashCode(key), partition count)
key 없음  → producer 내부 counter를 이용한 round-robin
```

같은 byte key와 같은 partition 수를 사용하면 항상 같은 partition이 선택됩니다. 따라서 같은 key의 레코드는 한 partition 안에서 순서를 유지합니다.

CLI 명령은 실행할 때마다 새로운 JVM producer를 시작합니다. null key round-robin 상태도 매번 0부터 시작하므로, 여러 명령에 걸친 분산을 관찰하려면 self-test처럼 하나의 producer 프로세스에서 여러 번 전송해야 합니다.

## Protocol 변경

### CreateTopic API (`3`)

request에 topic과 partition count를 보내면 broker가 `topic.meta`를 생성합니다. 같은 topic을 다시 생성하거나 1–1000 범위를 벗어난 partition 수는 거부합니다.

### Metadata API (`4`)

producer가 topic의 partition 수를 조회할 때 사용합니다.

### Produce API (`1`)

Milestone 2의 request에 `partition: int32`가 추가되었습니다. response는 실제 partition과 해당 partition에서 발급된 offset을 반환합니다.

### Fetch API (`2`)

request에 `partition: int32`가 추가되었습니다. broker는 해당 partition log만 읽습니다.

## 검증 내용

`self-test`는 실제 TCP broker를 통해 다음을 검증합니다.

1. partition 3개인 topic 생성 및 metadata 조회
2. partition 0과 1의 첫 레코드가 모두 offset 0인지 확인
3. 같은 key의 두 레코드가 같은 partition에서 연속 offset을 받는지 확인
4. null key 세 개가 partition 0, 1, 2에 round-robin 되는지 확인
5. broker 종료 후 `topic.meta`와 partition log가 남는지 확인
6. broker를 다시 시작해 저장된 partition metadata를 읽는지 확인

## 현재 한계

- topic 삭제와 partition 수 변경은 지원하지 않습니다.
- metadata는 단일 broker의 로컬 파일에만 존재합니다.
- CLI는 레코드 하나마다 새 producer 프로세스를 시작합니다.
- partition log는 아직 하나의 무한히 커지는 파일입니다.

다음 단계에서는 log segment rotation과 sparse offset index를 구현합니다.
