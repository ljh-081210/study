import json
import boto3
import pymysql
#import os 환경 변수에서 가져오기


def get_secret(secret_name):
    client = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])


def get_db_connection(secret):
    return pymysql.connect(
        host=secret["host"], #host = os.environ["DB_HOST"] 환경 변수 사용시
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
        method  = event["httpMethod"]
        user_id = (event.get("pathParameters") or {}).get("id")
    else:                                                   # Lambda Function URL
        method  = event["requestContext"]["http"]["method"]
        parts   = event.get("rawPath", "/").strip("/").split("/")
        user_id = parts[1] if len(parts) > 1 else None     # /users/1 → "1"

    body = json.loads(event["body"]) if event.get("body") else {}

    try:
        with connection.cursor() as cursor:
            if method == "POST":
                cursor.execute(
                    "INSERT INTO users (username, email, password) VALUES (%(username)s, %(email)s, %(password)s)",  # 수정: username,email,password → name,email
                    body
                )
                connection.commit()
                result = {"inserted_id": cursor.lastrowid}

            elif method == "GET" and not user_id:
                cursor.execute("SELECT id, username, email FROM users")
                result = cursor.fetchall()

            elif method == "GET" and user_id:
                cursor.execute("SELECT id, username, email FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

            elif method == "PUT":
                body["id"] = user_id
                cursor.execute(
                    "UPDATE users SET username=%(username)s, email=%(email)s, password=%(password)s WHERE id=%(id)s",
                    body
                )
                connection.commit()
                result = {"updated": cursor.rowcount}

            elif method == "DELETE":
                cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
                connection.commit()
                result = {"deleted": cursor.rowcount}

            else:
                return {"statusCode": 405, "body": json.dumps({"error": "Method Not Allowed"})}

        return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False, default=str)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)}, ensure_ascii=False)}
