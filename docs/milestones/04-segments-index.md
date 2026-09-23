# Milestone 4 — log segment와 sparse offset index

partition 하나의 로그가 계속 커지면 처음부터 읽어 특정 offset을 찾는 시간이 길어집니다. 이번 단계에서는 로그를 여러 segment로 나누고, 각 segment에 드문드문 위치를 적어 둔 index를 추가했습니다.

## 실행

```bash
mkdir -p out
javac -d out src/*.java
java -ea -cp out MiniKafka self-test
```

segment 교체를 쉽게 보려면 터미널 1에서 크기를 256 bytes로 설정합니다.

```bash
java -cp out MiniKafka broker data 9092 256
```

터미널 2에서 새 topic에 레코드를 추가합니다.

```bash
java -cp out MiniKafka create-topic 127.0.0.1 9092 demo-segments 1
for i in $(seq 0 19); do
  java -cp out MiniKafka produce 127.0.0.1 9092 demo-segments key "message-$i" 0
done
java -cp out MiniKafka consume 127.0.0.1 9092 demo-segments 0 12
ls data/demo-segments/0
```

`ls` 결과에는 `0.log`, `0.index`와 이후 base offset의 `.log`, `.index` 파일이 보입니다. 데이터가 이미 있다면 새 topic 이름을 사용하세요.

## 파일 구조

```text
data/demo-segments/
├── topic.meta
└── 0/                  # partition 0
    ├── 0.log           # offset 0부터 시작
    ├── 0.index
    ├── 6.log           # 예: offset 6부터 시작
    ├── 6.index
    └── ...
```

segment의 `.log` 내용은 이전 단계의 레코드 형식과 같습니다. `.index`는 레코드 16개마다 `(offset: int64, file position: int64)`을 저장합니다. 첫 레코드는 항상 index에 기록됩니다. 파일 크기는 지정한 기준에 도달한 뒤 다음 레코드를 쓸 때 교체하므로, 레코드 하나의 크기만큼 기준을 넘을 수 있습니다.

## offset 35를 읽을 때

1. segment의 base offset을 이진 탐색해 offset 35가 있는 파일을 고릅니다.
2. 그 segment의 index에서 35 이하인 가장 가까운 offset을 이진 탐색합니다.
3. 기록된 byte 위치로 이동해 최대 15개 레코드를 순차적으로 건너뜁니다.
4. offset 35부터 레코드를 반환합니다.

이제 fetch는 파티션의 맨 앞부터 읽지 않습니다. 단, 요청 offset 이후의 데이터를 전송할 때는 응답 크기만큼 순차적으로 읽습니다.

## 재시작과 이전 데이터

broker가 partition을 열 때 segment 파일을 base offset 순서로 검사합니다. offset의 연속성과 checksum을 확인하고 index를 다시 만듭니다. 마지막 segment 끝에 쓰다 만 레코드가 있으면 마지막 정상 위치까지 잘라냅니다. 닫힌 segment에 불완전한 레코드가 있거나 offset이 끊기면 오류를 반환합니다.

Milestone 3의 `data/<topic>/<partition>.log` 파일이 있으면 처음 열 때 `data/<topic>/<partition>/0.log`로 옮깁니다. 레코드 형식이 같아서 다시 쓸 필요는 없습니다.

## 검증

`self-test`에서 작은 segment 크기로 여러 파일을 만들고, 중간 offset 조회, 제한된 크기의 조회, 손상된 index 재구성, 불완전한 마지막 쓰기 제거, 재시작 후 다음 offset, 이전 파일 형식 이동을 확인합니다.

## 현재 한계

- 재시작 시 전체 로그를 검사하므로 시작 시간은 레코드 수에 비례합니다.
- broker가 열린 segment마다 파일 핸들을 유지합니다. 매우 많은 segment를 다루려면 핸들 관리가 필요합니다.
- 오래된 segment 삭제와 log compaction은 다음 마일스톤에서 다룹니다.
- 네트워크 fetch 응답은 16 MiB를 넘지 않도록 나눠 보냅니다. 한 frame에 담을 수 없는 큰 레코드는 produce 단계에서 거부합니다.
