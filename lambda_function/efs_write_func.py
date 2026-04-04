import json
import os
from datetime import datetime

MOUNT_PATH = "/mnt/guestbook"           # Lambda에 마운트된 EFS 경로
FILE_PATH = f"{MOUNT_PATH}/log.txt"     # 실제 데이터 저장 파일

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"]) if event.get("body") else {}
        name = body.get("name", "Anonymous")    # name 없으면 기본값 Anonymous
        message = body.get("message", "")

        if not message:                         # message는 필수값
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "message is required"})
            }

        os.makedirs(MOUNT_PATH, exist_ok=True)  # EFS 디렉토리 없으면 생성

        # 타임스탬프 포함해서 한 줄 포맷으로 작성
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {name}: {message}\n"

        with open(FILE_PATH, "a", encoding="utf-8") as f:  # "a" 모드: 기존 내용 유지하고 맨 뒤에 추가
            f.write(entry)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "작성 완료",
                "entry": entry.strip()
            }, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
