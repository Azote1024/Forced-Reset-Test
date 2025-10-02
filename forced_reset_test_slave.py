#!/usr/bin/env python3
import subprocess
import socket
import time
import threading
import json
import sys
import os

# ------------------------
# 設定部 — 必要に応じ書き換え
# ------------------------
MASTER_IP = "192.168.0.107"
MASTER_PORT = 50000

HEARTBEAT_INTERVAL = 1.0  # 秒
RESET_DELAY_AFTER_SOME = 60  # 任意：スレーブが自動リセットを送るまでの秒（例）

DEVICE = "/dev/sda"
# SMART 取得コマンド。JSON 出力対応版であれば -j -A を使うとよい
# smartctl_json = ["smartctl", "-j", "-A", DEVICE]
SMARTCTL_CMD = ["smartctl", "-A", DEVICE]

# 以下はリセット要求をマスターに送るときの形式
# 例: "RESET\n" または "RESET <reason>\n" など
RESET_REQ_PREFIX = "RESET\n"

# ------------------------
# ログ出力補助
# ------------------------
LOG_FILE = "slave.log"
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
# SMART 情報取得関数
# ------------------------
def get_smart_info():
    """
    smartctl -A を実行してその出力を文字列で返す。
    JSON 出力が使える場合は JSON を返す（文字列→json.loads 可能）。
    例外時は None を返す。
    """
    try:
        # subprocess.run を使ってコマンド実行、標準出力取得
        # check=True にすると returncode != 0 で例外
        proc = subprocess.run(SMARTCTL_CMD,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              text=True,
                              check=True)
        out = proc.stdout
        # optional: JSON 出力なら parse
        # try:
        #     obj = json.loads(out)
        #     return obj
        # except Exception:
        #     return out
        return out
    except Exception as e:
        log(f"SMART command failed: {e}")
        return None

# ------------------------
# マスターへ送信（ハートビート or SMART or RESET）
# ------------------------
def send_to_master(msg: str):
    """
    msg は改行末尾付き（例: "... \n"）の文字列。
    例外が起きても無視（要件）。戻り値 True/False。
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((MASTER_IP, MASTER_PORT))
        s.sendall(msg.encode("utf-8"))
        # 応答取得（任意、無応答でも OK）
        try:
            s.settimeout(1.0)
            _ = s.recv(1024)
        except socket.timeout:
            pass
        s.close()
        return True
    except Exception as e:
        log(f"Send to master failed: {e}")
        try:
            s.close()
        except:
            pass
        return False

# ------------------------
# 初期 SMART 送信処理
# ------------------------
def send_initial_smart():
    info = get_smart_info()
    if info is None:
        return
    # 先頭識別子付きで送信。JSON 出力であれば JSON 文字列、そうでなければテキスト全体をエスケープ／ラップ
    # ここでは “SMART ” 接頭辞 + JSON 文字列 or 生テキストを送る設計
    # ただし、マスター側は JSON 部を parse しようとするので、もし JSON 出力可能なら JSON 部分を使うことを推奨
    try:
        # もし info が JSON 形式文字列なら parseして dumps で整形
        obj = json.loads(info)
        payload = json.dumps(obj, ensure_ascii=False)
    except Exception:
        # JSON 出力できない場合は生テキストをそのまま “smart_text” キー付き JSON に包装して送る
        payload = json.dumps({"text": info})
    msg = f"SMART {payload}\n"
    send_to_master(msg)

# ------------------------
# ハートビート送信ループ
# ------------------------
def heartbeat_loop():
    while True:
        try:
            send_to_master("HB\n")
        except Exception as e:
            # 失敗は無視
            pass
        time.sleep(HEARTBEAT_INTERVAL)

# ------------------------
# 自己リセット要求構造（任意タイミングで発動）
# ------------------------
def schedule_self_reset(delay_sec):
    """
    指定秒後にマスターへリセット要求を送るスレッド起動
    """
    def target():
        time.sleep(delay_sec)
        try:
            send_to_master(RESET_REQ_PREFIX)
            log(f"Sent reset request to master after delay {delay_sec}")
        except Exception as e:
            log(f"Send reset request failed: {e}")
    t = threading.Thread(target=target, daemon=True)
    t.start()

# ------------------------
# メイン処理
# ------------------------
def main():
    log("Slave starting, sending initial SMART info")
    send_initial_smart()
    # スケジュールに応じて自己リセット要求をスタート（必要なら使わない・コメント可）
    schedule_self_reset(RESET_DELAY_AFTER_SOME)

    # ハートビート送信ループ（永続）
    heartbeat_loop()

if __name__ == "__main__":
    main()
