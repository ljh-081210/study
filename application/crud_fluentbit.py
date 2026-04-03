import logging
import json
import boto3
import pymysql
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

#logging 필요 없으면 아래 두 줄 삭제
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def get_secret(secret_name: str, region_name: str = "ap-northeast-2") -> dict:
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_db_connection(secret: dict) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],
        database=secret["dbname"],
        port=int(secret["port"]),
        connect_timeout=5,
    )


secret = get_secret("ws/rds/credentials")

app = FastAPI()

# DB 칼럼따라 수정
class UserRequest(BaseModel):
    username: str
    email: str
    password: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/users")
def insert_data(data: UserRequest):
    connection = None
    sql = "INSERT INTO users (username, email, password) VALUES (%(username)s, %(email)s, %(password)s)"
    try:
        connection = get_db_connection(secret)
        with connection.cursor() as cursor:
            cursor.execute(sql, data.model_dump())
            connection.commit()
            return {"inserted_id": cursor.lastrowid}
    except Exception:
        logger.exception("Failed to insert user")
        raise
    finally:
        if connection:
            connection.close()


@app.get("/users/{user_id}")
def select_data(user_id: int):
    connection = None
    sql = "SELECT id, username, email FROM users WHERE id = %(id)s"
    try:
        connection = get_db_connection(secret)
        with connection.cursor() as cursor:
            cursor.execute(sql, {"id": user_id})
            return cursor.fetchall()
    except Exception:
        logger.exception("Failed to fetch user")
        raise
    finally:
        if connection:
            connection.close()


@app.put("/users/{user_id}")
def update_data(user_id: int, data: UserRequest):
    connection = None
    sql = "UPDATE users SET username = %(username)s, email = %(email)s, password = %(password)s WHERE id = %(id)s"
    try:
        connection = get_db_connection(secret)
        params = data.model_dump()
        params["id"] = user_id
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            connection.commit()
            return {"updated": cursor.rowcount}
    except Exception:
        logger.exception("Failed to update user")
        raise
    finally:
        if connection:
            connection.close()


@app.delete("/users/{user_id}")
def delete_data(user_id: int):
    connection = None
    sql = "DELETE FROM users WHERE id = %(id)s"
    try:
        connection = get_db_connection(secret)
        with connection.cursor() as cursor:
            cursor.execute(sql, {"id": user_id})
            connection.commit()
            return {"deleted": cursor.rowcount}
    except Exception:
        logger.exception("Failed to delete user")
        raise
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
