import boto3
import mysql.connector
import logging
import json
from ec2_app.최종본.flask import Flask, request, jsonify

app = Flask(__name__)
app.json.ensure_ascii = False  # 한글 인코딩 이슈 방지

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

REGION = "ap-northeast-2"
SECRET_NAME = "ws/rds/credentials"

def get_secret():
    client = boto3.client("secretsmanager", region_name=REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])

#SSL_CERT = "/app/global-bundle.pem"  # TLS OFF시 이 줄 + ssl_ca, ssl_verify_cert 삭제

# IAM 인증 OFF시 이 함수 삭제
#def get_iam_token(host, user):
#    client = boto3.client("rds", region_name=REGION)
#    return client.generate_db_auth_token(DBHostname=host, Port=3306, DBUsername=user)

def get_connection():
    secret = get_secret()
    return mysql.connector.connect(
        host=secret["host"],
        user=secret["username"],
        password=secret["password"],  # IAM 인증 ON시 → get_iam_token(secret["host"], secret["username"]) 로 변경
        database=secret["dbname"],
        port=int(secret["port"])
        #use_pure=True,
        #ssl_ca=SSL_CERT,       # TLS OFF시 삭제
        #ssl_verify_cert=True   # TLS OFF시 삭제
    )

# Health Check
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

# CREATE
@app.route("/users", methods=["POST"])
def create_user():
    data = request.get_json()
    conn, cursor = None, None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, email, password) VALUES (%s, %s, %s)",
            (data["username"], data["email"], data["password"])
        )
        conn.commit()
        logger.info(f"유저 생성: {data['username']}")
        return jsonify({"message": "유저 생성 완료"}), 201
    except Exception as e:
        logger.error(f"유저 생성 실패: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# READ ALL
@app.route("/users", methods=["GET"])
def get_users():
    conn, cursor = None, None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        return jsonify(users), 200
    except Exception as e:
        logger.error(f"유저 조회 실패: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# READ ONE
@app.route("/users/<int:user_id>", methods=["GET"])
def get_user(user_id):
    conn, cursor = None, None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        #cursor.execute("SELECT id, username, email FROM users") 출력하면 안되는 속성이 있을 시 수동으로 칼럼 지정해서 SELECT
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "유저 없음"}), 404
        return jsonify(user), 200
    except Exception as e:
        logger.error(f"유저 조회 실패: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# UPDATE
@app.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    data = request.get_json()
    conn, cursor = None, None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET username=%s, email=%s, password=%s WHERE id=%s",
            (data["username"], data["email"], data["password"], user_id)
        )
        conn.commit()
        logger.info(f"유저 수정: id={user_id}")
        return jsonify({"message": "유저 수정 완료"}), 200
    except Exception as e:
        logger.error(f"유저 수정 실패: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# DELETE
@app.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    conn, cursor = None, None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE id=%s", (user_id,))
        conn.commit()
        logger.info(f"유저 삭제: id={user_id}")
        return jsonify({"message": "유저 삭제 완료"}), 200
    except Exception as e:
        logger.error(f"유저 삭제 실패: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
