# Milestone 1 — 영속 append-only log

첫 번째 단계에서는 네트워크 없이 Kafka의 가장 작은 핵심인 partition log를 구현했습니다.

완성 시점의 코드는 Git tag `milestone-1`로 보존되어 있습니다.

```bash
git checkout milestone-1
```

## 학습 목표

- 메시지를 소비 후 삭제하지 않고 파일 끝에 계속 추가하는 이유
- partition 내부 offset이 레코드의 위치를 나타내는 방법
- 프로세스 재시작 후 다음 offset을 복구하는 방법
- checksum과 불완전한 tail record 복구가 필요한 이유

## 실행

```bash
mkdir -p out
javac -d out src/MiniKafka.java
java -ea -cp out MiniKafka self-test

java -cp out MiniKafka produce data orders customer-1 "ordered coffee"
java -cp out MiniKafka produce data orders customer-2 "ordered tea"
java -cp out MiniKafka consume data orders 0
```

## 당시 구조

producer와 consumer가 `data/<topic>/0.log` 파일을 직접 열었습니다.

```text
CLI → Partition Log File
```

이 구조로 저장 원리는 볼 수 있지만, 저장 파일을 여러 client가 직접 공유해야 합니다. Milestone 2에서는 파일 접근을 broker 하나로 모으고 client는 TCP로 요청하도록 변경합니다.

## 완료 조건

- 두 레코드의 offset이 각각 0과 1이어야 합니다.
- 로그를 다시 연 뒤 offset 1부터 읽으면 두 번째 레코드만 나와야 합니다.
- 재시작 후 다음 append는 offset 2를 받아야 합니다.
