# monitor.py
# 중앙 모니터러 (Windows 작업 스케줄러: 1분마다 실행)
#
# 정책
# 1) last_seen_at이 180초 초과 시 DOWN
# 2) OK -> DOWN : 즉시 알림 1회
# 3) DOWN 유지 : 30분마다 재알림
# 4) DOWN -> OK : 복구 알림 1회
#
# 사용 테이블
# - tb_system_collector_heartbeat
# - tb_system_collector_alert_state

import os
import sys
import warnings
from datetime import datetime, timedelta
from contextlib import contextmanager
import pymysql
import pandas as pd
import requests

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
# =========================
# 환경 설정 (필요 시 환경변수로 변경 가능)
# =========================
DB_HOST = os.getenv("DB_HOST", "103.55.191.136")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "monitoringuser")
DB_PASS = os.getenv("DB_PASS", "monitoring2021!@#!")
DB_NAME = os.getenv("DB_NAME", "monitoringdb")

HB_TABLE = "tb_system_collector_heartbeat"
ALERT_TABLE = "tb_system_collector_alert_state"

DOWN_THRESHOLD_SEC = 180          # 3분
REALERT_INTERVAL_MIN = 30         # 30분 재알림


# =========================
# 네이버웍스 알림 함수 (여기 연결)
# =========================
def get_token_user_info():
    host,user,password,db_name,port = ('192.168.1.200','tmckmonitor','tmck0119!@','tmck_db',413)
    db = pymysql.connect(host=host, user=user, password=password, db=db_name, port=port,charset='utf8mb4', read_timeout=300)
    read_sql = f"SELECT * FROM sys_msg_token order by seq desc limit 1;"
    df = pd.read_sql(read_sql, db)
    db.close()
    host,user,password,db_name,port = ('103.55.191.136','monitoringuser','monitoring2021!@#!','monitoringdb',3306)
    db = pymysql.connect(host=host, user=user, password=password, db=db_name, port=port,charset='utf8mb4', read_timeout=300)
    account_sql = f"SELECT * FROM tb_system_account_tmck where active = 'o';"
    account_df = pd.read_sql(account_sql,db)

    db.close()
    access_token = df['access_token'][0]
    return access_token, account_df





def send_naverworks_alert(message: str):
    """
    실제 네이버웍스 알림 함수로 교체하세요.
    """
    print(message)  # 테스트용
    access_token, account_df = get_token_user_info()
    botId = 8558717
    channel_id = 'e330523e-01d9-b49e-c8ae-9463eb9c02c6'
    url = f'https://www.worksapis.com/v1.0/bots/{botId}/channels/{channel_id}/messages'
    while True:
        headers = {'Authorization': f"Bearer {access_token}", 'Content-Type': 'application/json'}
        data = {
            "content": {"type": "text", "text": message}
        }

        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200 or response.status_code == 201:
            break


# =========================
# 공통 유틸
# =========================
def now():
    return datetime.now()


@contextmanager
def db_conn():
    """
    DB 연결 컨텍스트 매니저
    """
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        yield conn
    finally:
        conn.close()


# =========================
# 현재 health 계산
# =========================
def fetch_current_health(conn):
    """
    heartbeat 테이블을 읽어서
    worker별 현재 health(OK/DOWN) 계산
    """
    sql = f"""
        SELECT worker_id, master_pid, status, last_seen_at
        FROM {HB_TABLE}
    """
    result = {}

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    threshold = timedelta(seconds=DOWN_THRESHOLD_SEC)
    current_time = now()

    for row in rows:
        worker_id = row["worker_id"]
        last_seen_at = row["last_seen_at"]
        # DOWN 판정 (둘 중 하나면 DOWN)
        # 1) heartbeat 끊김(PC/네트워크/agent 다운)
        # 2) status='DOWN'(master 다운)
        if (current_time - last_seen_at) > threshold or row["status"] == "DOWN":
            health = "DOWN"
        else:
            health = "OK"

        result[worker_id] = {
            "health": health,
            "last_seen_at": last_seen_at,
            "master_pid": row["master_pid"],
            "status": row["status"],
        }

    return result


# =========================
# alert_state 조회
# =========================
def fetch_alert_state(conn, worker_ids):
    """
    alert_state 테이블에서
    worker별 마지막 알림 상태 조회
    """
    if not worker_ids:
        return {}

    placeholders = ",".join(["%s"] * len(worker_ids))
    sql = f"""
        SELECT worker_id, last_health, last_alerted_at
        FROM {ALERT_TABLE}
        WHERE worker_id IN ({placeholders})
    """

    state = {}

    with conn.cursor() as cur:
        cur.execute(sql, worker_ids)
        rows = cur.fetchall()

    for row in rows:
        state[row["worker_id"]] = {
            "last_health": row["last_health"],
            "last_alerted_at": row["last_alerted_at"],
        }

    return state


# =========================
# alert_state UPSERT
# =========================
def upsert_alert_state(conn, worker_id, health):
    """
    alert_state 갱신 (마지막 알림 시각 + 상태)
    """
    now_str = now().strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
        INSERT INTO {ALERT_TABLE}
        (worker_id, last_health, last_alerted_at, updated_at, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            last_health = VALUES(last_health),
            last_alerted_at = VALUES(last_alerted_at),
            updated_at = VALUES(updated_at)
    """

    with conn.cursor() as cur:
        cur.execute(sql, (worker_id, health, now_str, now_str, now_str))


# =========================
# 메인 실행 로직
# =========================
def run_once():

    with db_conn() as conn:

        # 1. 현재 health 계산
        current = fetch_current_health(conn)
        if not current:
            print("heartbeat 데이터 없음")
            return

        worker_ids = list(current.keys())

        # 2. 기존 alert 상태 조회
        prev_state = fetch_alert_state(conn, worker_ids)

        for worker_id, data in current.items():

            health = data["health"]
            last_seen_at = data["last_seen_at"]
            master_pid = data["master_pid"]

            prev = prev_state.get(worker_id)

            # =========================
            # 현재 DOWN인 경우
            # =========================
            if health == "DOWN":

                # alert_state에 기록이 없는 경우 (처음 감지)
                if prev is None:
                    message = f"[DOWN] {worker_id}\n서버 다운 시각: {last_seen_at}"
                    send_naverworks_alert(message)
                    upsert_alert_state(conn, worker_id, "DOWN")

                else:
                    # OK -> DOWN 전환
                    if prev["last_health"] == "OK":
                        message = f"[DOWN] {worker_id}\n서버 다운 시각: {last_seen_at}"
                        send_naverworks_alert(message)
                        upsert_alert_state(conn, worker_id, "DOWN")

                    # DOWN 유지
                    else:
                        # 30분 지났는지 확인
                        if now() - prev["last_alerted_at"] >= timedelta(minutes=REALERT_INTERVAL_MIN):
                            message = f"[DOWN 재알림] {worker_id}\n서버 다운 시각: {last_seen_at}"
                            send_naverworks_alert(message)
                            upsert_alert_state(conn, worker_id, "DOWN")

            # =========================
            # 현재 OK인 경우
            # =========================
            else:
                if prev is not None and prev["last_health"] == "DOWN":
                    # 복구 알림
                    message = f"[복구] {worker_id} 정상화\n서버 재가동 시각: {last_seen_at}"
                    send_naverworks_alert(message)
                    upsert_alert_state(conn, worker_id, "OK")


if __name__ == "__main__":
    run_once()