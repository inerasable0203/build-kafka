# Milestone 5 — retention과 log compaction

이번 단계의 핵심은 **읽지 않은 consumer가 있어도 broker가 오래된 데이터를 정리할 수 있다**는 점입니다. retention은 닫힌 segment를 통째로 삭제하고, compaction은 같은 key의 이전 값을 닫힌 segment에서 제거합니다. 두 작업 모두 현재 쓰는 active segment는 건드리지 않습니다.

## 실행

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

터미널 1에서 실습용으로 segment 크기를 1바이트로 설정합니다. 레코드 하나를 쓴 뒤 다음 append에서 새 segment가 생기므로 파일별 변화를 쉽게 볼 수 있습니다.

```bash
java -cp out MiniKafka broker data/m5-demo 9093 1
```

터미널 2에서 같은 key를 갱신한 후 tombstone을 기록합니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9093 accounts 1
java -cp out MiniKafka produce 127.0.0.1 9093 accounts alice old 0
java -cp out MiniKafka produce 127.0.0.1 9093 accounts alice new 0
java -cp out MiniKafka delete 127.0.0.1 9093 accounts alice 0
java -cp out MiniKafka produce 127.0.0.1 9093 accounts bob current 0
java -cp out MiniKafka compact 127.0.0.1 9093 accounts 0
java -cp out MiniKafka consume 127.0.0.1 9093 accounts 0 0
ls data/m5-demo/accounts/0
```

`consume`에는 offset 2의 `alice <tombstone>`과 offset 3의 `bob current`가 보입니다. offset 0, 1은 사라지지만 **번호를 다시 매기지 않습니다.** offset 0에서 읽으면 다음으로 남아 있는 offset 2부터 반환합니다. tombstone은 값이 `null`인 레코드이며, key가 반드시 있어야 합니다. CLI의 `delete`는 이 레코드를 추가하는 명령이지 기존 레코드를 즉시 지우는 명령은 아닙니다.

## Retention 설정

```text
MiniKafka broker <data-dir> [port] [segment-bytes] [retention-ms] [retention-bytes]
```

예를 들어 다음은 256바이트마다 segment를 교체하고, 닫힌 segment가 60초 이상 지났거나 전체 로그가 4096바이트를 초과하면 오래된 닫힌 segment를 삭제합니다.

```bash
java -cp out MiniKafka broker data/m5-retention 9094 256 60000 4096
```

`-1`은 해당 제한을 끕니다. 용량만 시험하려면 `... 256 -1 4096`처럼 지정합니다. 시간은 **닫힌 `.log` 파일의 마지막 수정 시각**을 기준으로 합니다. 두 정책의 검사는 새 메시지를 추가할 때 수행합니다. 배경 타이머는 아직 없습니다. 용량 제한보다 active segment 하나가 크더라도 active segment는 삭제하지 않습니다.

Retention은 레코드 단위가 아니라 segment 단위로 작동합니다. 따라서 아직 읽지 않은 consumer의 데이터도 삭제될 수 있습니다. 삭제된 offset으로 fetch하면 남아 있는 다음 offset부터 읽습니다.

## Compaction의 동작

1. partition 전체를 읽어 key별 가장 최신 offset을 찾습니다.
2. 닫힌 segment에서 같은 key의 오래된 레코드를 제거합니다.
3. key가 없는 레코드와 key별 최신 레코드(tombstone 포함)는 남깁니다.
4. sparse index를 다시 만들고, 다음에 쓸 offset은 유지합니다.

현재 쓰는 active segment는 재작성하지 않으므로 그 안의 오래된 값은 다음에 segment가 닫히고 `compact`를 다시 실행할 때까지 남을 수 있습니다. `compact`는 학습용 수동 명령입니다. tombstone 만료나 주기적인 자동 compaction은 아직 구현하지 않았습니다.

## 재시작과 한계

Retention 또는 compaction 후에는 offset 사이에 빈칸이 생깁니다. 재시작 시 남은 segment를 검사해 index를 다시 만들고, 마지막 offset 다음부터 append합니다. 빈칸은 오류가 아닙니다.

이 구현의 retention 설정은 broker 실행 인자로 모든 partition에 공통 적용되며, 각 partition의 크기는 독립적으로 계산합니다. 토픽별 정책, consumer의 진행 상태를 고려한 삭제, tombstone 만료는 구현하지 않았습니다.
