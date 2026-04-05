import json
import os

MOUNT_PATH = "/mnt/guestbook"           # Lambda에 마운트된 EFS 경로
FILE_PATH = f"{MOUNT_PATH}/log.txt"     # 실제 데이터 저장 파일

def parse_body(event):
    # API Gateway / Function URL → event["body"]에 JSON 문자열로 전달
    if event.get("body"):
        raw = event["body"]
        return json.loads(raw) if isinstance(raw, str) else raw

    # 직접 JSON event → event 자체가 데이터 (Lambda 콘솔 테스트, 타 Lambda 직접 호출)
    return event


def lambda_handler(event, context):
    try:
        body = parse_body(event)

        os.makedirs(MOUNT_PATH, exist_ok=True)  # EFS 디렉토리 없으면 생성

        entry = json.dumps(body, ensure_ascii=False)

        with open(FILE_PATH, "w", encoding="utf-8") as f:  # "w" 모드: 덮어쓰기
            f.write(entry)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "작성 완료",
                "entry": entry
            }, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
