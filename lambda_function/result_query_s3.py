import json
import boto3
import pymysql
import os


def get_secret(secret_name):
    client = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])


def get_iam_token(host, user, port):  # IAM 인증 미사용시 삭제
    client = boto3.client("rds", region_name="ap-northeast-2")
    return client.generate_db_auth_token(DBHostname=host, Port=port, DBUsername=user)


def get_db_connection(secret):
    return pymysql.connect(
        host=secret["host"],          # Proxy 미사용시 Secrets Manager host를 RDS 엔드포인트로 변경
        user=secret["username"],
        password=get_iam_token(secret["host"], secret["username"], int(secret["port"])),  # IAM 인증 미사용시 삭제
        database=secret["dbname"],
        port=int(secret["port"]),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        ssl={"verify_cert": True},    # TLS 미사용시 삭제
    )


secret = get_secret("ws/rds/credentials")   # Secrets Manager 미사용시 삭제
connection = get_db_connection(secret)
s3 = boto3.client("s3", region_name="ap-northeast-2")
BUCKET_NAME = os.environ["BUCKET_NAME"]


def lambda_handler(event, context):
    global connection

    try:
        connection.ping(reconnect=True)
    except Exception:
        connection = get_db_connection(secret)

    try:
        params = event.get("queryStringParameters") or {}
        user_id = int(params["id"]) if params.get("id") else None

        with connection.cursor() as cursor:
            if user_id:
                cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            else:
                cursor.execute("SELECT * FROM users")
            rows = cursor.fetchall()

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="query-results/result.json",
            Body=json.dumps(rows, ensure_ascii=False, default=str),
            ContentType="application/json",
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Select success", "s3": "query-results/result.json"}, ensure_ascii=False)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}, ensure_ascii=False)
        }
