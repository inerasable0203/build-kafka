# Milestone 6 — consumer offset과 consumer group

이번 단계에서는 **메시지가 저장된 위치**와 **consumer가 어디까지 처리했는지**를 분리합니다. 같은 토픽이라도 group마다 읽은 위치가 다릅니다. 같은 group의 consumer는 파티션을 나눠 갖고, 서로 다른 group은 같은 메시지를 각각 읽습니다.

## 준비

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

터미널 1에서 broker를 켭니다. `data/m6-demo`에 이전 실습 데이터가 있다면 새 디렉터리 이름을 사용하세요.

```bash
java -cp out MiniKafka broker data/m6-demo 9095
```

터미널 2에서 파티션이 3개인 토픽에 메시지를 한 개씩 넣습니다. 명령 끝의 `0`, `1`, `2`는 저장할 파티션입니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9095 vehicles 3
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-0 "speed=50" 0
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-1 "speed=60" 1
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-2 "speed=70" 2
```

## group별 읽기 위치 확인

`workers` group으로 한 번 읽습니다. 출력에는 파티션 0, 1, 2의 레코드가 나타납니다.

```bash
java -cp out MiniKafka consume-group 127.0.0.1 9095 workers vehicles 1
java -cp out MiniKafka group-offset 127.0.0.1 9095 workers vehicles 0
```

두 번째 명령은 `next offset=1`을 출력합니다. offset 0을 **처리했으므로 다음에는 offset 1부터** 읽는다는 뜻입니다. 같은 명령으로 다시 읽으면 이미 처리한 레코드는 나오지 않습니다.

```bash
java -cp out MiniKafka consume-group 127.0.0.1 9095 workers vehicles 1
```

다른 group은 읽은 위치가 독립적입니다. `auditors`는 offset 0부터 같은 메시지를 다시 볼 수 있습니다.

```bash
java -cp out MiniKafka consume-group 127.0.0.1 9095 auditors vehicles 1
```

## 두 consumer가 파티션 나눠 갖기

터미널 2와 3에서 아래 명령을 각각 실행하고 켜 둡니다. 마지막의 조회 횟수를 생략하면 1초마다 계속 확인하고 heartbeat를 보냅니다.

```bash
java -cp out MiniKafka consume-group 127.0.0.1 9095 live-workers vehicles
```

첫 consumer는 처음에 세 파티션을 모두 받습니다. 두 번째 consumer가 참여하면 generation이 바뀌고 파티션이 나뉩니다. 예를 들어 첫 consumer는 `[0, 2]`, 두 번째는 `[1]`을 맡습니다. 이후 다른 터미널에서 각 파티션에 새 메시지를 넣으면 해당 파티션 담당 consumer가 출력합니다. 둘 중 하나를 `Ctrl+C`로 종료하면 나머지가 파티션을 다시 받습니다.

```bash
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-0 "speed=55" 0
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-1 "speed=65" 1
java -cp out MiniKafka produce 127.0.0.1 9095 vehicles car-2 "speed=75" 2
```

## 내부 동작

1. `JOIN_GROUP`: broker가 member ID를 발급하고 파티션을 배정합니다.
2. `HEARTBEAT`: member가 살아 있음을 알리고 현재 generation·배정을 받습니다.
3. `FETCH`: 배정된 파티션의 committed offset부터 레코드를 읽습니다.
4. `COMMIT_OFFSET`: 출력한 마지막 레코드의 **다음 offset**을 저장합니다.
5. `LEAVE_GROUP` 또는 10초간 heartbeat 부재: generation을 올리고 파티션을 재배정합니다.

파티션은 member의 참여 순서대로 round-robin 배정됩니다. 같은 파티션을 두 member에게 동시에 배정하지 않습니다. 이전 generation 또는 배정되지 않은 파티션의 commit은 거부됩니다.

Committed offset은 `data/m6-demo/vehicles/.consumer-offsets/<group>/<partition>.offset`에 저장됩니다. broker를 재시작하면 member는 다시 join해야 하지만 이 파일 덕분에 마지막으로 저장한 위치부터 이어 읽습니다. 값이 아직 없으면 offset 0에서 시작합니다.

## 현재 한계

- 하나의 group은 토픽 하나에 가입합니다. 여러 토픽 구독은 아직 없습니다.
- member 정보는 메모리에만 있고, 만료 여부는 group 요청이 들어올 때 확인합니다. 백그라운드 타이머는 없습니다.
- CLI는 레코드를 화면에 출력한 뒤 offset을 commit합니다. 출력 후 commit 전에 종료되면 같은 레코드가 다시 보일 수 있습니다(최소 한 번 처리). 실제 업무 처리 성공 여부를 확인하는 로직은 아직 없습니다.
- retention으로 로그가 삭제되면 committed offset이 가리키던 레코드가 없을 수 있습니다. 그 경우 다음으로 남아 있는 레코드부터 읽습니다.
