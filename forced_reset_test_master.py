#!/usr/bin/env python3
import socket
import threading
import time
import json
import os
import traceback

# ------------------------
# 設定部
# ------------------------
MASTER_LISTEN_IP = "0.0.0.0"
MASTER_LISTEN_PORT = 50000

HEARTBEAT_TIMEOUT = 180  # 秒（3分）

PLC_IP = "192.168.0.99"
PLC_PORT = 8501
PLC_CMD_TEMPLATE = "RS {relay}\r\n"

SLAVE_TO_RELAY = {
    "192.168.0.100": "R500",
    "192.168.0.101": "R501",
    "192.168.0.102": "R502",
    "192.168.0.103": "R503",
    "192.168.0.104": "R504",
    "192.168.0.105": "R505",
}

RESET_COUNTS_FILE = "reset_counts_simple.txt"
LOG_FILE = "master_simple.log"
SMART_DIR = "smart_logs"  # SMART ログ保存ディレクトリ（JSON Lines）

# ------------------------
# 状態変数
# ------------------------
last_seen = {}  # slave_ip -> 最後の心拍受信時刻（epoch 秒）
reset_counts = {relay: 0 for relay in SLAVE_TO_RELAY.values()}

# ------------------------
# ロギング関数（簡易）
# ------------------------
def log(msg, exc: Exception = None):
    """標準出力＋ログファイルにメッセージを出力。例外あればスタックトレースも出す。"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
            if exc:
                f.write(traceback.format_exc() + "\n")
    except Exception:
        pass

# ------------------------
# PLC リセット送信（ソケット方式）
# ------------------------
def send_plc_reset(relay: str) -> bool:
    """リレー名 relay に対して PLC にリセットコマンドを送信。成功／失敗を返す。"""
    cmd = PLC_CMD_TEMPLATE.format(relay=relay).encode("ascii", "ignore")
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((PLC_IP, PLC_PORT))
        s.sendall(cmd)
        # 応答を短時間だけ待つ（存在すれば受け取るが、なくても構わない）
        try:
            s.settimeout(1.0)
            _ = s.recv(1024)
        except socket.timeout:
            pass
        return True
    except Exception as e:
        log(f"PLC send error for {relay}: {e}", exc=e)
        return False
    finally:
        if s:
            try:
                s.close()
            except:
                pass

# ------------------------
# SMART 情報処理
# ------------------------
def handle_smart_message(slave_ip: str, smart_obj):
    """SMART 情報オブジェクトを受け取ったら対応スレーブファイルに JSON ライン形式で記録。"""
    relay = SLAVE_TO_RELAY.get(slave_ip)
    if relay is None:
        log(f"Received SMART from unknown IP {slave_ip}")
        return

    # 保存ディレクトリを確保
    try:
        os.makedirs(SMART_DIR, exist_ok=True)
    except Exception as e:
        log(f"Failed to make SMART_DIR: {e}", exc=e)

    fname = os.path.join(SMART_DIR, f"smart_{relay}.jsonl")
    record = {
        "recv_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "slave_ip": slave_ip,
        "relay": relay,
        "reset_count": reset_counts.get(relay, 0),
        "smart": smart_obj
    }
    line = json.dumps(record, ensure_ascii=False)
    try:
        with open(fname, "a") as f:
            f.write(line + "\n")
    except Exception as e:
        log(f"Failed writing SMART log for {relay}: {e}", exc=e)

# ------------------------
# クライアント接続処理
# ------------------------
def handle_client(conn: socket.socket, addr):
    slave_ip = addr[0]
    log(f"Connection from {slave_ip}")
    conn.settimeout(10.0)
    buffer = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while b"\n" in buffer:
                line_bytes, buffer = buffer.split(b"\n", 1)
                try:
                    line = line_bytes.decode("utf-8", errors="ignore").strip()
                except Exception:
                    continue
                if not line:
                    continue

                # 判別：SMART／RESET／ハートビート
                if line.startswith("SMART "):
                    json_part = line[len("SMART "):]
                    try:
                        obj = json.loads(json_part)
                        handle_smart_message(slave_ip, obj)
                    except Exception as e:
                        log(f"JSON parse error from SMART {slave_ip}: {e}")
                    # SMART も心拍扱い
                    last_seen[slave_ip] = time.time()
                elif line.startswith("RESET"):
                    # スレーブからリセット要求を受けた
                    relay = SLAVE_TO_RELAY.get(slave_ip)
                    if relay:
                        log(f"Received RESET request from {slave_ip}, relay {relay}")
                        ok = send_plc_reset(relay)
                        if ok:
                            reset_counts[relay] = reset_counts.get(relay, 0) + 1
                            try:
                                with open(RESET_COUNTS_FILE, "a") as f:
                                    f.write(f"{relay} {reset_counts[relay]}\n")
                            except Exception as e:
                                log(f"Failed write reset count (RESET message): {e}")
                    # そのあとも心拍として更新
                    last_seen[slave_ip] = time.time()
                else:
                    # 通常心拍
                    last_seen[slave_ip] = time.time()
    except Exception as e:
        log(f"Client {slave_ip} error: {e}", exc=e)
    finally:
        try:
            conn.close()
        except:
            pass
        log(f"Connection closed {slave_ip}")

# ------------------------
# サーバ起動（heart + SMART 兼用）
# ------------------------
def start_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((MASTER_LISTEN_IP, MASTER_LISTEN_PORT))
    s.listen(10)
    log(f"Server listening on {MASTER_LISTEN_IP}:{MASTER_LISTEN_PORT}")
    while True:
        try:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
        except Exception as e:
            log(f"Accept error: {e}", exc=e)

# ------------------------
# 監視ループ（タイムアウト検出 → リセット送信）
# ------------------------
def monitor_loop():
    while True:
        now = time.time()
        for ip, relay in SLAVE_TO_RELAY.items():
            last = last_seen.get(ip)
            if last is None:
                continue
            if now - last > HEARTBEAT_TIMEOUT:
                log(f"Heartbeat timeout: {ip} (relay {relay}), sending reset")
                ok = send_plc_reset(relay)
                if ok:
                    reset_counts[relay] = reset_counts.get(relay, 0) + 1
                    try:
                        with open(RESET_COUNTS_FILE, "a") as f:
                            f.write(f"{relay} {reset_counts[relay]}\n")
                    except Exception as e:
                        log(f"Failed write reset count (timeout): {e}")
        time.sleep(1.0)

# ------------------------
# メイン
# ------------------------
def main():
    log("Master (with SMART + RESET handling) starting")
    t = threading.Thread(target=start_server, daemon=True)
    t.start()
    monitor_loop()

if __name__ == "__main__":
    main()
