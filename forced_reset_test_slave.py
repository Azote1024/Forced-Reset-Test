#!/usr/bin/env python3
import subprocess
import socket
import time
import threading
import json
import os
import traceback

# ------------------------
# 設定部 — 環境に合わせて修正
# ------------------------
MASTER_IP = "192.168.0.107"
MASTER_PORT = 50000

HEARTBEAT_INTERVAL = 1.0  # 秒

DEVICE = "/dev/sda"
# JSON 出力可能ならこのコマンドを使う
SMARTCTL_JSON_CMD = ["smartctl", "-j", "-A", DEVICE]
# テキスト出力フォールバック
SMARTCTL_TEXT_CMD = ["smartctl", "-A", DEVICE]

# RESET 要求のタイミング（任意）：例として、起動後指定秒後に RESET を送る
SELF_RESET_DELAY = 60  # 秒（この機能が不要なら None にする）

LOG_FILE = "slave.log"

# ------------------------
# ログ関数
# ------------------------
def log(msg, exc: Exception = None):
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
# SMART 情報取得
# ------------------------
def get_smart_info():
    """
    smartctl -j -A（JSON）を試み、それが失敗するなら -A（テキスト）を使うフォールバック。
    成功したら Python オブジェクトを返す（JSON モード）あるいは文字列出力を "text" キー付き dict で返す。
    """
    # まず JSON モードで試す
    try:
        proc = subprocess.run(SMARTCTL_JSON_CMD, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, check=True)
        out = proc.stdout
        obj = json.loads(out)
        return obj
    except Exception as e:
        # JSON モードで失敗したならテキスト版を試す
        try:
            proc2 = subprocess.run(SMARTCTL_TEXT_CMD, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, check=True)
            out2 = proc2.stdout
            return {"text": out2}
        except Exception as e2:
            log(f"SMART both JSON and text failed: {e2}", exc=e2)
            return None

# ------------------------
# マスターへ送信
# ------------------------
def send_to_master(msg: str) -> bool:
    """
    msg に改行 (\n) を含む文字列を送信する。例外はキャッチして False を返す。
    """
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((MASTER_IP, MASTER_PORT))
        s.sendall(msg.encode("utf-8"))
        # 応答を短時間受け取ってもよい
        try:
            s.settimeout(1.0)
            _ = s.recv(1024)
        except socket.timeout:
            pass
        return True
    except Exception as e:
        log(f"send_to_master failed: {e}", exc=e)
        return False
    finally:
        if s:
            try:
                s.close()
            except:
                pass

# ------------------------
# 起動時 SMART 情報送信
# ------------------------
def send_initial_smart():
    info = get_smart_info()
    if info is None:
        return
    # JSON 形式のオブジェクトならそのま使い、そうでなければ text フィールド包装
    # 最終的にダンプして 1 行 JSON にして "SMART " 接頭辞付きで送信
    try:
        payload = json.dumps(info, ensure_ascii=False)
    except Exception as e:
        # 安全策としてテキスト版包装
        payload = json.dumps({"text": str(info)})
    msg = f"SMART {payload}\n"
    send_to_master(msg)

# ------------------------
# ハートビートループ
# ------------------------
def heartbeat_loop():
    while True:
        try:
            send_to_master("HB\n")
        except Exception as e:
            # 失敗はログに残して無視
            log(f"heartbeat send failed: {e}", exc=e)
        time.sleep(HEARTBEAT_INTERVAL)

# ------------------------
# 自己リセット要求（任意）
# ------------------------
def schedule_self_reset(delay_sec):
    if delay_sec is None:
        return
    def target():
        time.sleep(delay_sec)
        try:
            send_to_master("RESET\n")
            log(f"Sent RESET request after {delay_sec}s")
        except Exception as e:
            log(f"RESET send failed: {e}", exc=e)
    t = threading.Thread(target=target, daemon=True)
    t.start()

# ------------------------
# メイン処理
# ------------------------
def main():
    log("Slave starting, sending initial SMART")
    proc = subprocess.Popen(["vlc", "/home/delibot/test/tako.mp4"])
    send_initial_smart()
    schedule_self_reset(SELF_RESET_DELAY)
    heartbeat_loop()

if __name__ == "__main__":
    main()
