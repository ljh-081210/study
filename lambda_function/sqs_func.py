import json
import boto3
import pymysql


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

    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
            action = body.get("action", "create")

            with connection.cursor() as cursor:
                if action == "create":
                    cursor.execute(
                        "INSERT INTO users (username, email, password) VALUES (%(username)s, %(email)s, %(password)s)",
                        body
                    )
                elif action == "update":
                    cursor.execute(
                        "UPDATE users SET username=%(username)s, email=%(email)s, password=%(password)s WHERE id=%(id)s",
                        body
                    )
                else:
                    print(f"알 수 없는 action: {action}")
                    continue

                connection.commit()

        except Exception as e:
            print(f"처리 실패: {str(e)} / body: {record.get('body')}")

    return {"statusCode": 200, "body": json.dumps({"message": "SQS 처리 완료"}, ensure_ascii=False)}
