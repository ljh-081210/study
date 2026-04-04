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
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=secret["password"],
        database=os.environ["DB_NAME"],
        port=int(secret["port"]),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


secret = get_secret(os.environ["SECRET_NAME"])
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

    body = json.loads(event["body"]) if event.get("body") else {}
    params = event.get("queryStringParameters") or {}
    region = params.get("region")

    try:
        with connection.cursor() as cursor:
            if method == "POST":
                cursor.execute(
                    "INSERT INTO deliveries (delivery_id, region, status, weight_kg, cost_krw, delivered_at) VALUES (%(delivery_id)s, %(region)s, %(status)s, %(weight_kg)s, %(cost_krw)s, %(delivered_at)s)",
                    body
                )
                connection.commit()
                result = {"delivery_id": body.get("delivery_id")}

            elif method == "GET":
                sql = """
                    SELECT
                        region,
                        COUNT(*)                                                AS total,
                        SUM(CASE WHEN status = '완료'  THEN 1 ELSE 0 END) AS completed,
                        SUM(CASE WHEN status = '지연'    THEN 1 ELSE 0 END) AS `delayed`,
                        SUM(CASE WHEN status = '취소'  THEN 1 ELSE 0 END) AS cancelled,
                        AVG(cost_krw)                                           AS avg_cost_krw,
                        SUM(weight_kg)                                          AS total_weight_kg
                    FROM deliveries
                """
                if region:
                    cursor.execute(sql + " WHERE region = %s GROUP BY region", (region,))
                else:
                    cursor.execute(sql + " GROUP BY region")

                data = cursor.fetchall()
                result = {"count": len(data), "data": data}

            else:
                return {"statusCode": 405, "body": json.dumps({"error": "Method Not Allowed"})}

        return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False, default=lambda x: float(x) if isinstance(x, Decimal) else str(x))}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)}, ensure_ascii=False)}
