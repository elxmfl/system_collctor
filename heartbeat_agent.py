# heartbeat_agent.py
# 역할:
# - C:\rawdata\userinfo\user_info.json 에서 roll, name 읽어서 worker_id 생성
# - master 프로세스가 살아있는지 확인 (PID 파일 우선, 없으면 프로세스 검색)
# - 30초마다 tb_system_collector_heartbeat 에 upsert
# - PID 파일 경로: C:\rawdata\log\collector_master.pid
#
# 주의:
# - 여기서는 master 재기동(자동복구) 안 함 (수동 운영)
# - psutil 설치 필요: pip install psutil pymysql

import os
import json
import time
from datetime import datetime
from typing import Optional, Tuple
import psutil
import pymysql

# ====== 경로/설정 ======
USER_INFO_PATH = r"C:\rawdata\userinfo\user_info.json"
MASTER_PID_PATH = r"C:\rawdata\log\collector_master.pid"

HEARTBEAT_INTERVAL_SEC = 30

DB_HOST = os.getenv("DB_HOST", "103.55.191.136")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "monitoringuser")
DB_PASS = os.getenv("DB_PASS", "monitoring2021!@#!")
DB_NAME = os.getenv("DB_NAME", "monitoringdb")

HB_TABLE = "tb_system_collector_heartbeat"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_worker_id() -> str:
    """
    user_info.json에서 roll + name으로 worker_id 생성
    """
    with open(USER_INFO_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    roll = data.get("roll")
    name = data.get("name")

    if not roll or not name:
        raise ValueError(f"roll/name이 비어있음: roll={roll}, name={name}")

    worker_id = f"{roll}_{name}"
    if len(worker_id) > 50:
        raise ValueError(f"worker_id가 50자를 초과함: {worker_id} (len={len(worker_id)})")

    return worker_id

def read_pid_file() -> Optional[int]:
    """
    PID 파일이 있으면 PID 읽기
    """
    if not os.path.exists(MASTER_PID_PATH):
        return None
    try:
        with open(MASTER_PID_PATH, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None


def write_pid_file(pid: int) -> None:
    """
    감지된 master PID를 PID 파일로 저장(갱신)
    """
    os.makedirs(os.path.dirname(MASTER_PID_PATH), exist_ok=True)
    with open(MASTER_PID_PATH, "w", encoding="utf-8") as f:
        f.write(str(pid))


def is_pid_alive(pid: int) -> bool:
    """
    PID가 살아있는지 확인
    """
    if pid <= 0:
        return False
    return psutil.pid_exists(pid)

def find_master_pid_by_cmdline(target: str) -> Optional[int]:
    """
    실행 커맨드라인에 'collector_master.py'가 포함된 프로세스를 찾아 PID 반환
    (PyCharm 수동 실행 시에도 cmdline에 .py 경로가 남는 경우가 많아서 이 방식 사용)
    """
    ##### target 파일명 #####

    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            # cmdline이 문자열 리스트인 경우가 많음
            joined = " ".join(cmdline).lower()
            if target in joined:
                return int(proc.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        except Exception:
            continue

    return None

def get_master_status(target: str) -> Tuple[bool, Optional[int]]:
    """
    (master_alive, master_pid) 반환
    우선순위: PID 파일 -> 프로세스 검색
    """
    # 1) PID 파일 우선
    pid = read_pid_file()
    if pid and is_pid_alive(pid):
        return True, pid

    # 2) PID 파일이 없거나 죽었으면 프로세스 검색
    pid2 = find_master_pid_by_cmdline(target)
    if pid2:
        # 찾았으면 PID 파일도 갱신해둠 (운영 편의)
        write_pid_file(pid2)
        return True, pid2

    return False, None


def db_connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )


def upsert_heartbeat(conn, worker_id: str, master_pid: Optional[int], status: str):
    """
    tb_system_collector_heartbeat upsert
    """
    ts = now_str()
    sql = f"""
    INSERT INTO {HB_TABLE}
        (worker_id, master_pid, status, last_seen_at, updated_at, created_at)
    VALUES
        (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        master_pid=VALUES(master_pid),
        status=VALUES(status),
        last_seen_at=VALUES(last_seen_at),
        updated_at=VALUES(updated_at)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (worker_id, master_pid, status, ts, ts, ts))


def main():
    worker_id = load_worker_id()
    conn = None
    ##### target 파일명 #####
    target = "collector_master.py"
    while True:
        try:
            if conn is None:
                conn = db_connect()

            master_alive, master_pid = get_master_status(target)

            # master가 살아있으면 OK, 아니면 DOWN
            status = "OK" if master_alive else "DOWN"

            upsert_heartbeat(conn, worker_id, master_pid, status)

        except Exception as e:
            # DB 장애/권한 문제 등으로 실패할 수 있으니, 다음 루프에서 재연결 시도
            conn = None
            print(f"[{now_str()}][agent][{worker_id}] ERROR: {type(e).__name__}: {e}", flush=True)

        time.sleep(HEARTBEAT_INTERVAL_SEC)


if __name__ == "__main__":
    main()