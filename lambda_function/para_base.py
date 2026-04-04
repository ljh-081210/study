import json
import boto3
import pymysql
import os
from decimal import Decimal


def get_secret(secret_name):
    client = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])


def get_db_connection(secret):
    return pymysql.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],
        database=secret["dbname"],
        port=int(secret["port"]),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


secret = get_secret("ws/rds/credentials")
connection = get_db_connection(secret)


def lambda_handler(event, context):
    global connection
    try:
        connection.ping(reconnect=True)
    except Exception:
        connection = get_db_connection(secret)

    # API Gateway / Lambda Function URL 파싱
    if "httpMethod" in event:                               # API Gateway
        method = event["httpMethod"]
    else:                                                   # Lambda Function URL
        method = event["requestContext"]["http"]["method"]

    # [JSON body] request body에서 파싱 → POST/PUT에서 삽입/수정할 데이터
    body    = json.loads(event["body"]) if event.get("body") else {}
    # [Query Parameter] URL ?key=value 형식으로 파싱 → 조회/수정/삭제 대상 id 지정
    params  = event.get("queryStringParameters") or {}
    user_id = params.get("id")                              # ?id=1

    try:
        with connection.cursor() as cursor:
            if method == "POST":
                # [JSON body 사용] 삽입할 데이터를 body에서 가져옴
                # curl -X POST <url> -d '{"username":"홍길동","email":"...","password":"..."}'
                cursor.execute(
                    "INSERT INTO users (username, email, password) VALUES (%(username)s, %(email)s, %(password)s)",
                    body
                )
                connection.commit()
                result = {"inserted_id": cursor.lastrowid}

            elif method == "GET" and user_id:
                # [Query Parameter 사용] 조회할 id를 URL에서 가져옴
                # curl <url>?id=1
                cursor.execute("SELECT id, username, email FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

            elif method == "GET":
                # [Query Parameter 없음] 전체 조회
                # curl <url>
                cursor.execute("SELECT id, username, email FROM users")
                result = cursor.fetchall()

            elif method == "PUT" and user_id:
                # [Query Parameter + JSON body 둘 다 사용]
                # id는 URL에서, 수정할 데이터는 body에서 가져옴
                # curl -X PUT <url>?id=1 -d '{"username":"...","email":"...","password":"..."}'
                body["id"] = user_id                        # query parameter의 id를 body에 합침
                cursor.execute(
                    "UPDATE users SET username=%(username)s, email=%(email)s, password=%(password)s WHERE id=%(id)s",
                    body
                )
                connection.commit()
                result = {"updated": cursor.rowcount}

            elif method == "DELETE" and user_id:
                # [Query Parameter 사용] 삭제할 id를 URL에서 가져옴, body 불필요
                # curl -X DELETE <url>?id=1
                cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
                connection.commit()
                result = {"deleted": cursor.rowcount}

            else:
                return {"statusCode": 400, "body": json.dumps({"error": "id parameter required"})}

        return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False, default=lambda x: float(x) if isinstance(x, Decimal) else str(x))}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)}, ensure_ascii=False)}
