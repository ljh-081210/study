import json
import os

MOUNT_PATH = "/mnt/guestbook"           # Lambda에 마운트된 EFS 경로
FILE_PATH = f"{MOUNT_PATH}/log.txt"     # 실제 데이터 저장 파일

def lambda_handler(event, context):
    try:
        # 파일 자체가 없으면 (한 번도 쓴 적 없으면) 빈 배열 반환
        if not os.path.exists(FILE_PATH):
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "방명록이 비어있습니다.",
                    "entries": []
                }, ensure_ascii=False)
            }

        with open(FILE_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()           # 파일 전체를 줄 단위로 읽음

        entries = [line.strip() for line in lines if line.strip()]  # 빈 줄 필터링 + 개행 제거

        return {
            "statusCode": 200,
            "body": json.dumps({
                "total": len(entries),      # 총 개수
                "entries": entries          # 전체 목록
            }, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
