import socket
import threading
import time
import json
import os

# ------------------------
# 設定
# ------------------------
MASTER_LISTEN_IP = "0.0.0.0"
MASTER_LISTEN_PORT = 50000

# この時間経過したら強制的に再起動する
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
SMART_DIR = "smart_logs"  # SMARTログ保存ディレクトリ（存在しなければ作成）

# ------------------------
# 状態変数
# ------------------------
last_seen = {}  # slave_ip -> 最終ハートビート受信時刻（秒 epoch）
reset_counts = {relay: 0 for relay in SLAVE_TO_RELAY.values()}

# ------------------------
# ログ出力関数
# ------------------------
def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ------------------------
# PLC リセット送信（ソケット方式のみ）
# ------------------------
def send_plc_reset(relay):
    cmd = PLC_CMD_TEMPLATE.format(relay=relay).encode("ascii", "ignore")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((PLC_IP, PLC_PORT))
        s.sendall(cmd)
        # 応答を受け取る（任意、無応答も可）
        try:
            s.settimeout(1.0)
            _ = s.recv(1024)
        except socket.timeout:
            pass
        s.close()
        return True
    except Exception as e:
        log(f"PLC send error for {relay}: {e}")
        try:
            s.close()
        except:
            pass
        return False

# ------------------------
# SMART 情報処理
# ------------------------
def handle_smart_message(slave_ip, json_payload):
    """
    スレーブから受け取った SMART JSON ペイロードを保存する。
    各スレーブごとにファイルを持つ（JSONライン形式）。
    受信時刻・現在のリセット回数も付加。
    """
    # リレー名を探す（IP → リレー）
    relay = SLAVE_TO_RELAY.get(slave_ip, None)
    if relay is None:
        # 未知のスレーブからの SMART は無視
        log(f"Received SMART from unknown IP {slave_ip}")
        return

    # 保存ディレクトリ存在確認
    try:
        os.makedirs(SMART_DIR, exist_ok=True)
    except Exception:
        pass

    fname = os.path.join(SMART_DIR, f"smart_{relay}.jsonl")
    # 構成 JSON 行を作成
    rec = {
        "recv_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "slave_ip": slave_ip,
        "relay": relay,
        "reset_count": reset_counts.get(relay, 0),
        "smart": json_payload
    }
    line = json.dumps(rec, ensure_ascii=False)
    try:
        with open(fname, "a") as f:
            f.write(line + "\n")
    except Exception as e:
        log(f"Failed writing SMART log for {relay}: {e}")

# ------------------------
# TCP クライアントハンドラ
# ------------------------
def handle_client(conn, addr):
    slave_ip = addr[0]
    log(f"Connection from {slave_ip}")
    conn.settimeout(10.0)
    buf = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            # データ中に複数行含まれる可能性を考慮
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                text = None
                try:
                    text = line.decode("utf-8", errors="ignore").strip()
                except Exception:
                    continue
                if not text:
                    continue
                # 判別：SMART 情報か、ハートビートか？
                # 提案：SMART は "SMART " という接頭辞を付けるものとする
                # 例: SMART {"属性": 数値, ...}
                if text.startswith("SMART "):
                    # JSON 部分を取り出す
                    jsonpart = text[len("SMART "):]
                    try:
                        obj = json.loads(jsonpart)
                        handle_smart_message(slave_ip, obj)
                    except Exception as e:
                        log(f"Failed parse SMART JSON from {slave_ip}: {e}")
                    # それも心拍として扱う（最低更新だけでも）
                    last_seen[slave_ip] = time.time()
                else:
                    # 通常の心拍メッセージとみなす
                    last_seen[slave_ip] = time.time()
    except Exception as e:
        log(f"Client {slave_ip} error: {e}")
    finally:
        try:
            conn.close()
        except:
            pass
        log(f"Connection closed {slave_ip}")

# ------------------------
# サーバ起動
# ------------------------
def start_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((MASTER_LISTEN_IP, MASTER_LISTEN_PORT))
    s.listen(10)
    log(f"Heartbeat + SMART server listening on {MASTER_LISTEN_IP}:{MASTER_LISTEN_PORT}")
    while True:
        try:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
        except Exception as e:
            log(f"Accept error: {e}")

# ------------------------
# 監視ループ
# ------------------------
def monitor_loop():
    while True:
        now = time.time()
        for ip, relay in SLAVE_TO_RELAY.items():
            last = last_seen.get(ip)
            if last is None:
                continue
            if now - last > HEARTBEAT_TIMEOUT:
                log(f"Heartbeat timeout for {ip} (relay {relay}), sending reset")
                ok = send_plc_reset(relay)
                if ok:
                    reset_counts[relay] += 1
                    # カウント保存（追記形式）
                    try:
                        with open(RESET_COUNTS_FILE, "a") as f:
                            f.write(f"{relay} {reset_counts[relay]}\n")
                    except Exception as e:
                        log(f"Failed to write reset count: {e}")
                # （連続送信防止制御は省略。必要なら追加可）
        time.sleep(1.0)

# ------------------------
# エントリポイント
# ------------------------
def main():
    log("Master starting (with SMART capability)")
    # サーバ起動スレッド
    t = threading.Thread(target=start_server, daemon=True)
    t.start()
    # 監視ループをメインスレッドで実行
    monitor_loop()

if __name__ == "__main__":
    main()
