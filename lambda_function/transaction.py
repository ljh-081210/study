import json
import boto3
import pymysql
import os


def get_secret(secret_name):
    client = boto3.client("secretsmanager", region_name="ap-northeast-2")
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])


def get_db_connection(secret):
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=secret["password"],
        database=os.environ["DB_NAME"],
        port=3306,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        autocommit=False,
    )


secret = get_secret(os.environ["SECRET_NAME"])
connection = get_db_connection(secret)


def lambda_handler(event, context):
    global connection

    try:
        connection.ping(reconnect=True)
    except Exception:
        connection = get_db_connection(secret)

    body = json.loads(event["body"]) if event.get("body") else {}
    action = body.get("action")

    try:
        if action == "place_order":
            return place_order(body)
        elif action == "get_order":
            return get_order(body)
        elif action == "order_stats":
            return order_stats()
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Unknown action: {action}"}, ensure_ascii=False),
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}, ensure_ascii=False),
        }


def place_order(body):
    """orders + order_items 동시 생성 (트랜잭션)"""
    customer_name = body.get("customer_name")
    items = body.get("items", [])

    if not customer_name or not items:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "customer_name and items are required"}, ensure_ascii=False),
        }

    try:
        with connection.cursor() as cursor:
            # orders 테이블에 주문 생성
            cursor.execute(
                "INSERT INTO orders (customer_name) VALUES (%s)",
                (customer_name,),
            )
            order_id = cursor.lastrowid

            # order_items 테이블에 아이템 삽입 (루프)
            total_price = 0
            for item in items:
                cursor.execute(
                    "INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                    (order_id, item["product_name"], item["quantity"], item["unit_price"]),
                )
                total_price += item["quantity"] * item["unit_price"]

        connection.commit()

        return {
            "statusCode": 201,
            "body": json.dumps(
                {"result": "created", "order_id": order_id, "total_price": total_price},
                ensure_ascii=False,
            ),
        }

    except Exception as e:
        connection.rollback()
        raise e


def get_order(body):
    """주문 정보 + 아이템 목록 JOIN 조회"""
    order_id = body.get("order_id")

    if not order_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "order_id is required"}, ensure_ascii=False),
        }

    with connection.cursor() as cursor:
        # 주문 조회
        cursor.execute(
            "SELECT id, customer_name, status, created_at FROM orders WHERE id = %s",
            (order_id,),
        )
        order = cursor.fetchone()

        if not order:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "order not found"}, ensure_ascii=False),
            }

        # 아이템 목록 조회
        cursor.execute(
            "SELECT id, order_id, product_name, quantity, unit_price FROM order_items WHERE order_id = %s",
            (order_id,),
        )
        items = cursor.fetchall()

    total_price = sum(item["quantity"] * item["unit_price"] for item in items)
    order["created_at"] = str(order["created_at"])
    order["items"] = items
    order["total_price"] = total_price

    return {
        "statusCode": 200,
        "body": json.dumps(order, ensure_ascii=False, default=str),
    }


def order_stats():
    """상태별 주문 수 및 총 매출 집계"""
    sql = """
        SELECT
            o.status,
            COUNT(o.id)                            AS count,
            SUM(oi.quantity * oi.unit_price)       AS total_revenue
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        GROUP BY o.status
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        stats = cursor.fetchall()

    return {
        "statusCode": 200,
        "body": json.dumps({"stats": stats}, ensure_ascii=False, default=str),
    }
