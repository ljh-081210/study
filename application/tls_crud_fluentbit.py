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


SSL_CERT = "/app/global-bundle.pem"  # TLS OFF시 이 줄 + ssl_ca, ssl_verify_cert 삭제

# IAM 인증 OFF시 이 함수 삭제
def get_iam_token(host, user, port):
    client = boto3.client("rds", region_name="ap-northeast-2")
    return client.generate_db_auth_token(DBHostname=host, Port=port, DBUsername=user)

def get_db_connection(secret: dict) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=secret["host"],
        user=secret["username"],
        password=get_iam_token(secret["host"], secret["username"], int(secret["port"])),  # IAM 인증 OFF시 → secret["password"] 로 변경
        database=secret["dbname"],
        port=int(secret["port"]),
        connect_timeout=5,
        ssl_ca=SSL_CERT,       # TLS OFF시 삭제
        ssl_verify_cert=True   # TLS OFF시 삭제
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
