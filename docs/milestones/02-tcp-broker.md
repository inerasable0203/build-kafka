# Milestone 2 — TCP broker와 protocol

producer와 consumer가 로그 파일을 직접 열지 않습니다. 장시간 실행되는 broker가 로그를 소유하고, client는 TCP request를 통해서만 데이터를 저장하고 읽습니다.

완성 시점의 코드는 Git tag `milestone-2`로 보존되어 있습니다.

```bash
git checkout milestone-2
```

## 학습 목표

- TCP가 byte stream이므로 message 경계를 직접 표시해야 하는 이유
- request와 response를 correlation ID로 연결하는 방법
- 신뢰할 수 없는 network input의 길이를 검증해야 하는 이유
- 여러 client가 동시에 접근할 때 하나의 partition log가 쓰기를 직렬화하는 방법

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
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-1 "ordered coffee"
java -cp out MiniKafka produce 127.0.0.1 9092 orders customer-2 "ordered tea"
java -cp out MiniKafka consume 127.0.0.1 9092 orders 0
```

## 흐름

```text
Producer CLI ─┐
              ├─ TCP ─→ Broker ─→ data/<topic>/0.log
Consumer CLI ─┘
```

broker는 연결마다 worker thread를 할당합니다. 같은 topic의 요청은 하나의 `PartitionLog` 인스턴스를 공유하며, `append()`와 `readFrom()`은 동기화되어 있습니다.

## Frame

모든 정수는 big-endian입니다.

```text
4 bytes frame size | frame body
```

`frame size`는 자기 자신을 제외한 body의 크기이며 최대 16 MiB입니다. 이 범위를 벗어나면 broker는 연결을 종료합니다.

### Produce request

| 필드 | 형식 |
|---|---|
| correlation ID | int32 |
| API key | int8, `1` |
| topic | int32 length + UTF-8 bytes |
| key | int32 length + bytes, null은 `-1` |
| value | int32 length + bytes |

성공 response는 correlation ID, status `0`, 저장된 offset을 반환합니다.

### Fetch request

| 필드 | 형식 |
|---|---|
| correlation ID | int32 |
| API key | int8, `2` |
| topic | int32 length + UTF-8 bytes |
| offset | int64 |

성공 response는 correlation ID, status `0`, record count와 레코드 목록을 반환합니다.

## 검증 내용

`self-test`는 임시 port의 실제 broker를 시작하고 다음을 검증합니다.

1. 서로 다른 TCP 연결에서 레코드 두 개를 produce
2. offset 1부터 fetch
3. response correlation ID 확인
4. 16 MiB를 초과한다고 선언한 frame의 연결 종료 확인
5. broker 종료 후 로그를 다시 열어 다음 offset이 2인지 확인

## 현재 한계

- 실제 Kafka wire protocol과 호환되지 않습니다.
- topic마다 partition 0 하나만 존재합니다.
- fetch는 요청한 offset 이후의 레코드를 한 번에 반환합니다.
- 인증, TLS, batching, replication은 없습니다.

다음 단계에서는 topic metadata와 여러 partition을 추가합니다.
