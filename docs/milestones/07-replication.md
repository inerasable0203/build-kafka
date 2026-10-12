# Milestone 7 — leader/follower 복제

이번 단계에서는 같은 파티션의 로그를 **세 broker에 같은 offset과 순서로 저장**합니다. broker 0이 모든 파티션의 leader이고 broker 1·2가 follower입니다. Producer는 leader에 쓰고, follower는 leader에게 아직 없는 레코드를 요청해서 자기 디스크에 추가합니다.

```text
producer ──PRODUCE──> broker 0 (leader)
                           ↑         ↑
                REPLICA_FETCH         REPLICA_FETCH
                           │         │
                broker 1 (follower)   broker 2 (follower)
```

`REPLICA_FETCH` 요청은 follower가 leader에게 보내며, leader가 레코드를 응답합니다.

## 준비와 broker 실행

저장소 루트에서 빌드하고 전체 self-test를 실행합니다. 테스트는 세 개의 TCP broker를 켜고 복제·재시작을 검증합니다.

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

각각 **별도 터미널**에서 아래 명령을 하나씩 실행합니다. 기존 실습 데이터가 있다면 `data/cluster-0` 같은 디렉터리 이름을 새 것으로 바꿔 주세요. 세 broker는 같은 `config/cluster.properties`를 읽되, 데이터 디렉터리는 공유하지 않습니다. `broker.id` 파일은 잘못된 broker ID로 기존 디렉터리를 열지 못하게 합니다.

```bash
# 터미널 1: leader, 127.0.0.1:9100
java -cp out MiniKafka cluster-broker config/cluster.properties 0 data/cluster-0
```

```bash
# 터미널 2: follower, 127.0.0.1:9101
java -cp out MiniKafka cluster-broker config/cluster.properties 1 data/cluster-1
```

```bash
# 터미널 3: follower, 127.0.0.1:9102
java -cp out MiniKafka cluster-broker config/cluster.properties 2 data/cluster-2
```

## 토픽과 복제 확인

네 번째 터미널에서 leader에 토픽을 만듭니다. 파티션은 두 개이며 **각 파티션이 세 broker에 복제**됩니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9100 replicated-cars 2
java -cp out MiniKafka replica-status 127.0.0.1 9100 replicated-cars 0
```

두 follower가 토픽을 발견하고 요청할 때까지 잠시 기다렸다가 `replica-status`를 다시 실행하세요. 처음에는 `end=-1`, `in-sync=false`일 수 있습니다. 준비되면 대략 아래처럼 보입니다.

```text
leader=0 end=0
follower=1 end=0 lag=0 in-sync=true
follower=2 end=0 lag=0 in-sync=true
```

이제 파티션 0에 레코드를 씁니다. 맨 끝의 `0`은 파티션 번호, `all`은 acks 설정입니다.

```bash
java -cp out MiniKafka produce 127.0.0.1 9100 replicated-cars car-1 "speed=50" 0 all
java -cp out MiniKafka replica-status 127.0.0.1 9100 replicated-cars 0
java -cp out MiniKafka consume 127.0.0.1 9100 replicated-cars 0 0
java -cp out MiniKafka consume 127.0.0.1 9101 replicated-cars 0 0
java -cp out MiniKafka consume 127.0.0.1 9102 replicated-cars 0 0
```

`acks=all` 명령이 성공했다면 세 `consume` 명령 모두 offset 0의 `car-1 / speed=50`을 보여야 합니다. 각 broker의 `data/cluster-<id>/replicated-cars/0/`에는 같은 레코드가 들어 있는 `.log`와 검색용 `.index`가 생깁니다. 1번 파티션도 독립된 offset과 로그를 가집니다.

## end offset, lag, 간이 ISR

`end`는 **다음에 기록할 offset**입니다. offset 0까지 저장했다면 `end=1`입니다. `lag = leader end - follower end`이므로 follower가 아직 offset 0을 받지 못했다면 leader의 `end=1`, follower의 `end=0`, `lag=1`입니다.

이 구현에서 `in-sync=true`는 follower가 현재 leader end까지 따라왔고 최근 5초 안에 진행 상태를 보고했다는 뜻입니다. 이것은 실습용 **간이 ISR 판정**입니다. 실제 Kafka의 ISR 관리, high watermark, min.insync.replicas를 구현한 것은 아닙니다.

## acks 차이 실습

```bash
java -cp out MiniKafka produce 127.0.0.1 9100 replicated-cars car-1 "speed=60" 0 1
java -cp out MiniKafka produce 127.0.0.1 9100 replicated-cars car-1 "speed=70" 0 0
java -cp out MiniKafka replica-status 127.0.0.1 9100 replicated-cars 0
```

| 값 | CLI가 기다리는 것 | 의미 |
|---|---|---|
| `0` | 응답을 기다리지 않음 | offset도 받지 못하며, 저장 실패를 확인하지 못할 수 있음 |
| `1` | leader의 디스크 기록 | follower 복제 전에 성공할 수 있음 |
| `all` | leader와 두 follower의 기록 | 쓰기 전 두 follower가 동기화돼 있어야 하며, 기록 뒤 최대 5초 기다림 |

이 프로젝트에서 `acks=all`은 **두 follower 모두**를 요구합니다. follower가 늦거나 종료되면 `not all replicas are in sync` 또는 `replication timeout after local append`가 날 수 있습니다. 후자의 경우 leader에 이미 레코드가 남아 있으므로, 무조건 재시도하면 중복이 생길 수 있습니다. `acks=0`에서 출력하는 `sent without ack`는 저장 성공 표시가 아닙니다.

장애를 보려면 터미널 3의 follower를 `Ctrl+C`로 종료하고, 새 토픽을 만든 뒤 아래 명령을 실행하세요. 새 토픽의 follower 상태가 없으므로 `all`은 거부되고 `1`은 leader에 기록됩니다. follower를 **같은 디렉터리**로 재시작하면 빠진 레코드를 따라잡습니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9100 offline-demo 1
java -cp out MiniKafka produce 127.0.0.1 9100 offline-demo car-2 "blocked" 0 all
java -cp out MiniKafka produce 127.0.0.1 9100 offline-demo car-2 "accepted" 0 1
# 터미널 3에서 broker 2를 다시 실행한 뒤:
java -cp out MiniKafka replica-status 127.0.0.1 9100 offline-demo 0
java -cp out MiniKafka consume 127.0.0.1 9102 offline-demo 0 0
```

## 현재 범위와 다음 단계

- 설정 파일은 세 broker의 주소를 고정하며 replication factor도 3으로 고정됩니다. 파티션별 leader 분산은 아직 없습니다.
- leader가 종료돼도 follower는 새 leader가 되지 않습니다. 선출·장애 전환·client 재접속은 Milestone 8에서 다룹니다.
- follower에서 일반 `FETCH`로 로컬 로그를 읽을 수 있지만, 아직 복제 중이면 최신 레코드가 보이지 않을 수 있습니다. 쓰기·토픽 생성·group 작업은 leader에 보내야 합니다.
- group의 committed offset은 leader에만 저장됩니다. cluster 모드에서는 데이터 유실·불일치 방지를 위해 retention과 compaction을 비활성화했습니다.
- 이 binary protocol은 학습용이며 Apache Kafka client와 호환되지 않습니다. 운영용 quorum, 인증, 고가용성을 제공하지 않습니다.

개념 비교에는 [Apache Kafka의 replication 설계](https://kafka.apache.org/41/design/design/)와 [producer `acks` 설명](https://kafka.apache.org/31/configuration/producer-configs/)을 참고할 수 있습니다. 이 구현의 고정 leader·전체 follower 확인 규칙은 학습을 위한 단순화입니다.
