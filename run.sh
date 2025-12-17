#!/bin/bash
# HAProxy + Failover + Telegram bot installer/uninstaller
# This script will install, configure, enable, or remove the full stack.

set -euo pipefail

LOG_FILE="/var/log/haproxy_stack_installer.log"
HAPROXY_CFG="/etc/haproxy/haproxy.cfg"
CHECK_SCRIPT="/etc/haproxy/check_vless_e2e.sh"
FAILOVER_SCRIPT="/usr/local/bin/haproxy_failover_daemon_final.sh"
BOT_SCRIPT="/usr/local/bin/haproxy_telegram_bot.py"
BOT_SERVICE="/etc/systemd/system/haproxy-telegram-bot.service"
FAILOVER_SERVICE="/etc/systemd/system/haproxy-failover.service"
LAST_STATES_FILE="/var/lib/haproxy/telegram_last_states.json"
DB_FILE="/var/lib/haproxy/stats.db"
BOT_CONFIG="/etc/haproxy/telegram_bot_config.json"
PKGS=(haproxy socat curl python3 python3-pip python3-requests)

show_banner() {
    clear
    echo "====================================================="
    echo "  .-.                 ____ _   _  ___  ____ _____    "
    echo " (o o) boo!          / ___| | | |/ _ \\/ ___|_   _|   "
    echo " | O \\              | |  _| |_| | | | \\___ \\ | |     "
    echo "  \\   \\             | |_| |  _  | |_| |___) || |     "
    echo "   '~~~'             \\____|_| |_|\\___/|____/ |_|     "
    echo "-----------------------------------------------------"
    echo "A SOCKS5 load balancer using HAProxy's first algorithm"
    echo "for persistent, high-priority connections with failover."
    echo "====================================================="
    echo ""
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

require_root() {
    if [ "$EUID" -ne 0 ]; then
        echo "Please run as root"
        exit 1
    fi
}

ensure_packages() {
    log "Installing prerequisites: ${PKGS[*]}"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y >> "$LOG_FILE" 2>&1 || true
    apt-get install -y "${PKGS[@]}" >> "$LOG_FILE" 2>&1
}

prompt_install_data() {
    echo "Install load balancer"
    read -p "Frontend listen IP (default 127.0.0.1): " FRONT_IP
    FRONT_IP=${FRONT_IP:-127.0.0.1}
    read -p "Frontend listen port (default 8888): " FRONT_PORT
    FRONT_PORT=${FRONT_PORT:-8888}

    read -p "HAProxy stats port (default 8065): " STATS_PORT
    STATS_PORT=${STATS_PORT:-8065}
    read -p "HAProxy stats URI path (default /status_ghost_007): " STATS_URI
    STATS_URI=${STATS_URI:-/status_ghost_007}
    # اطمینان از شروع با /
    if [[ "$STATS_URI" != /* ]]; then
        STATS_URI="/$STATS_URI"
    fi
    read -p "HAProxy stats username (default ghost): " STATS_USER
    STATS_USER=${STATS_USER:-ghost}
    read -p "HAProxy stats password (default ghost@@): " STATS_PASS
    STATS_PASS=${STATS_PASS:-ghost@@}

    read -p "Telegram bot token: " BOT_TOKEN
    read -p "Telegram admin chat_id: " BOT_CHAT

    read -p "Number of SOCKS servers to load-balance (default 3): " COUNT
    COUNT=${COUNT:-3}

    SERVERS=()
    for ((i=1;i<=COUNT;i++)); do
        read -p "Server #$i name (default vless_s$i): " SNAME
        SNAME=${SNAME:-vless_s$i}
        read -p "Server #$i local port (default $((10000+i))): " SPORT
        SPORT=${SPORT:-$((10000+i))}
        # همه سرورها به ترتیب ورود اولویت دارند؛ فقط آخرین سرور همیشه backup است
        if [ $i -eq "$COUNT" ]; then
            OPTS="check fall 2 rise 1 backup"
        else
            OPTS="check fall 2 rise 1"
        fi
        SERVERS+=("$SNAME:$SPORT:$OPTS")
    done
}

write_haproxy_cfg() {
    mkdir -p /etc/haproxy
    log "Writing $HAPROXY_CFG"
    cat > "$HAPROXY_CFG" <<EOF
global
    log /dev/log            local0 notice
    maxconn 20000
    user haproxy
    group haproxy
    daemon
    stats socket /var/run/haproxy.sock mode 666 level admin
    stats timeout 2m

defaults
    mode tcp
    log global
    timeout connect 5s
    timeout client 300s
    timeout server 300s
    option tcplog

# Frontend for VLESS/SOCKS
frontend vless_in
    bind ${FRONT_IP}:${FRONT_PORT}
    mode tcp
    default_backend vless_prioritized_servers

# Backend servers
backend vless_prioritized_servers
    balance first
EOF
    for entry in "${SERVERS[@]}"; do
        IFS=':' read -r N P O <<< "$entry"
        echo "    server ${N} 127.0.0.1:${P} ${O}" >> "$HAPROXY_CFG"
    done

    cat >> "$HAPROXY_CFG" <<EOF

# Stats
listen stats
    bind *:${STATS_PORT}
    mode http
    stats enable
    stats hide-version
    stats uri ${STATS_URI}
    stats realm HAProxy\\ Status\\ Portal
    stats auth ${STATS_USER}:${STATS_PASS}
EOF
}

write_check_script() {
    log "Writing $CHECK_SCRIPT"
    cat > "$CHECK_SCRIPT" <<'EOF'
#!/bin/bash
LOCAL_PORT=$1
TARGET_URL="http://www.cloudflare.com/cdn-cgi/trace"
TIMEOUT=5

if [ -z "$LOCAL_PORT" ]; then
    echo "ERROR: Port number is required" >&2
    exit 1
fi

if ! [[ "$LOCAL_PORT" =~ ^[0-9]+$ ]] || [ "$LOCAL_PORT" -lt 1 ] || [ "$LOCAL_PORT" -gt 65535 ]; then
    echo "ERROR: Invalid port number: $LOCAL_PORT" >&2
    exit 1
fi

/usr/bin/curl --silent --fail --max-time $TIMEOUT --connect-timeout $TIMEOUT \
    -x socks5://127.0.0.1:$LOCAL_PORT \
    "$TARGET_URL" > /dev/null 2>&1
EXIT_CODE=$?
exit $EXIT_CODE
EOF
    chmod +x "$CHECK_SCRIPT"
}

write_failover_script() {
    log "Writing $FAILOVER_SCRIPT"
    cat > "$FAILOVER_SCRIPT" <<'EOF'
#!/bin/bash

CHECK_SCRIPT="/etc/haproxy/check_vless_e2e.sh"
HAPROXY_CONFIG="/etc/haproxy/haproxy.cfg"
CHECK_INTERVAL=5
STATE_FILE="/tmp/haproxy_server_states.txt"

SERVERS_CONFIG=(
EOF
    for entry in "${SERVERS[@]}"; do
        IFS=':' read -r N P O <<< "$entry"
        echo "    \"${N}:${P}:${O}\"" >> "$FAILOVER_SCRIPT"
    done
    cat >> "$FAILOVER_SCRIPT" <<'EOF'
)
BACKEND_NAME="vless_prioritized_servers"
CONFIG_TEMP_FILE="/tmp/haproxy_config_temp.cfg"

get_previous_state() {
    local server_name=$1
    if [ -f "$STATE_FILE" ]; then
        grep "^${server_name}:" "$STATE_FILE" | cut -d: -f2
    else
        echo ""
    fi
}

save_current_state() {
    local server_name=$1
    local state=$2
    local temp_state_file="${STATE_FILE}.tmp"
    if [ -f "$STATE_FILE" ]; then
        grep -v "^${server_name}:" "$STATE_FILE" > "$temp_state_file" 2>/dev/null || true
    else
        touch "$temp_state_file"
    fi
    echo "${server_name}:${state}" >> "$temp_state_file"
    mv "$temp_state_file" "$STATE_FILE"
}

get_server_state_from_config() {
    local server_name=$1
    local line=$(grep "^ *server ${server_name} " "$HAPROXY_CONFIG" | head -n 1)
    if echo "$line" | grep -q "disabled"; then
        echo "down"
    else
        echo "up"
    fi
}

echo "Waiting 5 seconds for HAProxy service to initialize..."
sleep 5
echo "Starting optimized monitoring loop (Restart only on state change)..."

for SERVER_ENTRY in "${SERVERS_CONFIG[@]}"; do
    IFS=':' read -r SERVER_NAME LOCAL_PORT INITIAL_OPTS <<< "$SERVER_ENTRY"
    CURRENT_STATE=$(get_server_state_from_config "$SERVER_NAME")
    save_current_state "$SERVER_NAME" "$CURRENT_STATE"
done

while true; do
    cp "$HAPROXY_CONFIG" "$CONFIG_TEMP_FILE"
    STATE_CHANGED=false

    for SERVER_ENTRY in "${SERVERS_CONFIG[@]}"; do
        IFS=':' read -r SERVER_NAME LOCAL_PORT INITIAL_OPTS <<< "$SERVER_ENTRY"
        PREVIOUS_STATE=$(get_previous_state "$SERVER_NAME")
        "$CHECK_SCRIPT" "$LOCAL_PORT"
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            CURRENT_STATE="up"
            NEW_LINE="    server ${SERVER_NAME} 127.0.0.1:${LOCAL_PORT} ${INITIAL_OPTS}"
        else
            CURRENT_STATE="down"
            NEW_LINE="    server ${SERVER_NAME} 127.0.0.1:${LOCAL_PORT} disabled"
        fi

        if [ "$PREVIOUS_STATE" != "$CURRENT_STATE" ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] CHANGE DETECTED: Server $SERVER_NAME changed from '$PREVIOUS_STATE' to '$CURRENT_STATE' (Check exit code: $EXIT_CODE)"
            sed -i "/^ *server ${SERVER_NAME} /c\\$NEW_LINE" "$CONFIG_TEMP_FILE"
            save_current_state "$SERVER_NAME" "$CURRENT_STATE"
            STATE_CHANGED=true
        fi
    done

    if [ "$STATE_CHANGED" = true ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Configuration changed. Performing HAProxy Validation..."
        if sudo haproxy -c -f "$CONFIG_TEMP_FILE" > /dev/null 2>&1; then
            sudo cp "$CONFIG_TEMP_FILE" "$HAPROXY_CONFIG"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: Validation OK. Restarting HAProxy service..."
            if sudo systemctl restart haproxy > /dev/null 2>&1; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: HAProxy Restarted. Users will reconnect to available servers."
            else
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to restart HAProxy service. Check logs!"
            fi
        else
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: New configuration file is invalid. NOT restarting."
            for SERVER_ENTRY in "${SERVERS_CONFIG[@]}"; do
                IFS=':' read -r SERVER_NAME LOCAL_PORT INITIAL_OPTS <<< "$SERVER_ENTRY"
                CURRENT_STATE=$(get_server_state_from_config "$SERVER_NAME")
                save_current_state "$SERVER_NAME" "$CURRENT_STATE"
            done
        fi
    fi

    sleep "$CHECK_INTERVAL"
done
EOF
    chmod +x "$FAILOVER_SCRIPT"
}

write_bot_service() {
    log "Writing $BOT_SERVICE"
    cat > "$BOT_SERVICE" <<EOF
[Unit]
Description=HAProxy Telegram Bot - Monitor and Status
After=network.target haproxy.service
Requires=haproxy.service

[Service]
Type=simple
User=root
ExecStart=/usr/bin/python3 /usr/local/bin/haproxy_telegram_bot.py
Restart=on-failure
RestartSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
}

write_failover_service() {
    log "Writing $FAILOVER_SERVICE"
    cat > "$FAILOVER_SERVICE" <<EOF
[Unit]
Description=HAProxy Failover Daemon (RESTART Mode)
After=network.target haproxy.service

[Service]
ExecStart=${FAILOVER_SCRIPT}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF
}

write_bot_config() {
    log "Writing $BOT_CONFIG"
    mkdir -p "$(dirname "$BOT_CONFIG")"
    # Build server names array
    SERVER_NAMES_JSON="["
    for entry in "${SERVERS[@]}"; do
        IFS=':' read -r N P O <<< "$entry"
        if [ "$SERVER_NAMES_JSON" != "[" ]; then
            SERVER_NAMES_JSON+=", "
        fi
        SERVER_NAMES_JSON+="\"${N}\""
    done
    SERVER_NAMES_JSON+="]"
    
    cat > "$BOT_CONFIG" <<EOF
{
    "bot_token": "${BOT_TOKEN}",
    "admin_chat_id": "${BOT_CHAT}",
    "check_interval": 5,
    "stats_refresh_interval": 30,
    "server_names": ${SERVER_NAMES_JSON}
}
EOF
    chmod 600 "$BOT_CONFIG"
}

write_bot_script() {
    log "Writing bot script to ${BOT_SCRIPT}"
    cat > "$BOT_SCRIPT" <<'EOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HAProxy Telegram Bot
ارسال تغییرات وضعیت سرورها و نمایش استاتوس کامل HAProxy
"""

import os
import sys
import json
import time
import socket
import traceback
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sqlite3

try:
    import requests
except ImportError:
    print("Installing requests library...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

CONFIG_FILE = "/etc/haproxy/telegram_bot_config.json"
HAPROXY_SOCKET = "/var/run/haproxy.sock"
HAPROXY_CONFIG = "/etc/haproxy/haproxy.cfg"
STATE_FILE = "/tmp/haproxy_server_states.txt"
DB_FILE = "/var/lib/haproxy/stats.db"
LOG_FILE = "/var/log/haproxy_telegram_bot.log"
LAST_STATES_FILE = "/var/lib/haproxy/telegram_last_states.json"

# Default config (will be loaded from file)
DEFAULT_CONFIG = {
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "admin_chat_id": "YOUR_ADMIN_CHAT_ID_HERE",
    "check_interval": 5,
    "stats_refresh_interval": 30,
    "server_names": []
}

def log_message(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] [{level}] {message}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception:
        pass
    print(entry.strip())

def load_config() -> Dict:
    """Load configuration from file"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
                # Merge with defaults
                for key, value in DEFAULT_CONFIG.items():
                    if key not in config:
                        config[key] = value
                return config
        except Exception as e:
            log_message(f"Error loading config: {e}", "ERROR")
    else:
        log_message(f"Config file not found. Creating default config at {CONFIG_FILE}")
        save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG.copy()

def save_config(config: Dict):
    """Save configuration to file"""
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

def init_database():
    """Initialize SQLite database for stats"""
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Table for server state changes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS server_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_name TEXT NOT NULL,
            state TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Table for connection stats
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS connection_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_name TEXT NOT NULL,
            connections INTEGER,
            bytes_in INTEGER,
            bytes_out INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Table for session tracking
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_name TEXT NOT NULL,
            start_time DATETIME,
            end_time DATETIME,
            duration_seconds INTEGER,
            bytes_transferred INTEGER
        )
    """)
    
    conn.commit()
    conn.close()

def send_haproxy_command(command: str) -> Optional[str]:
    """Send command to HAProxy via socket"""
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(HAPROXY_SOCKET)
        sock.sendall(f"{command}\n".encode())
        
        response = b""
        while True:
            data = sock.recv(4096)
            if not data:
                break
            response += data
            if len(data) < 4096:
                break
        
        sock.close()
        return response.decode('utf-8', errors='ignore')
    except Exception as e:
        log_message(f"Error sending HAProxy command: {e}", "ERROR")
        return None

def get_haproxy_stats() -> List[Dict]:
    """Get HAProxy statistics"""
    # Load server names from config
    config = load_config()
    server_names = config.get("server_names", [])
    
    response = send_haproxy_command("show stat")
    if not response:
        return []
    
    stats = []
    lines = response.strip().split('\n')
    if len(lines) < 2:
        return []
    
    # Parse CSV format
    headers = lines[0].split(',')
    
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split(',')
        if len(values) < len(headers):
            continue
        
        stat_dict = {}
        for i, header in enumerate(headers):
            stat_dict[header] = values[i] if i < len(values) else ""
        
        # Only return backend server stats (dynamic based on config)
        svname = stat_dict.get('svname', '')
        if server_names and svname in server_names:
            stats.append(stat_dict)
        elif not server_names:
            # Fallback: if no server_names in config, get all backend servers
            if svname and svname not in ['FRONTEND', 'BACKEND', 'stats']:
                stats.append(stat_dict)
    
    return stats

def load_last_states() -> Dict[str, str]:
    """Load last known server states from persistent file"""
    try:
        if os.path.exists(LAST_STATES_FILE):
            with open(LAST_STATES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        log_message(f"Error loading last states file: {e}", "ERROR")
    return {}

def save_last_states(states: Dict[str, str]):
    """Persist last known server states to file"""
    try:
        os.makedirs(os.path.dirname(LAST_STATES_FILE), exist_ok=True)
        with open(LAST_STATES_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, ensure_ascii=False)
    except Exception as e:
        log_message(f"Error saving last states file: {e}", "ERROR")

def get_server_status(server_name: str) -> Dict:
    """Get detailed status for a specific server"""
    stats = get_haproxy_stats()
    for stat in stats:
        if stat.get('svname') == server_name:
            return stat
    return {}

# ==================== Database Functions ====================

def save_server_state_change(server_name: str, state: str):
    """Save server state change to database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO server_states (server_name, state) VALUES (?, ?)",
        (server_name, state)
    )
    conn.commit()
    conn.close()

def get_server_uptime(server_name: str) -> Optional[timedelta]:
    """Get server uptime from database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Get last state change to 'up'
    cursor.execute("""
        SELECT timestamp FROM server_states 
        WHERE server_name = ? AND state = 'up' 
        ORDER BY timestamp DESC LIMIT 1
    """, (server_name,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        last_up_time = datetime.fromisoformat(result[0])
        return datetime.now() - last_up_time
    return None

def get_server_usage_stats(server_name: str, hours: int = 24) -> Dict:
    """Get server usage statistics"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    since_time = datetime.now() - timedelta(hours=hours)
    
    # Get total connections
    cursor.execute("""
        SELECT SUM(connections) FROM connection_stats 
        WHERE server_name = ? AND timestamp >= ?
    """, (server_name, since_time.isoformat()))
    
    total_connections = cursor.fetchone()[0] or 0
    
    # Get total bytes
    cursor.execute("""
        SELECT SUM(bytes_in), SUM(bytes_out) FROM connection_stats 
        WHERE server_name = ? AND timestamp >= ?
    """, (server_name, since_time.isoformat()))
    
    result = cursor.fetchone()
    total_bytes_in = result[0] or 0 if result else 0
    total_bytes_out = result[1] or 0 if result else 0
    
    # Get current session duration
    cursor.execute("""
        SELECT start_time FROM sessions 
        WHERE server_name = ? AND end_time IS NULL 
        ORDER BY start_time DESC LIMIT 1
    """, (server_name,))
    
    session_result = cursor.fetchone()
    session_duration = None
    if session_result:
        session_start = datetime.fromisoformat(session_result[0])
        session_duration = datetime.now() - session_start
    
    conn.close()
    
    return {
        "total_connections": total_connections,
        "total_bytes_in": total_bytes_in,
        "total_bytes_out": total_bytes_out,
        "session_duration": session_duration
    }

def save_connection_stats():
    """Save current connection stats to database and prune old logs.

    برای اینکه حجم دیتابیس زیاد نشود، لاگ‌های قدیمی‌تر از ۶ ساعت پاک می‌شوند.
    """
    stats = get_haproxy_stats()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # پاک‌سازی رکوردهای قدیمی‌تر از ۶ ساعت (فقط آخرین ۶ ساعت نگه داشته می‌شود)
    try:
        cursor.execute(
            "DELETE FROM connection_stats WHERE timestamp < datetime('now','-6 hours')"
        )
        cursor.execute(
            "DELETE FROM server_states WHERE timestamp < datetime('now','-6 hours')"
        )
        cursor.execute(
            "DELETE FROM sessions WHERE end_time IS NOT NULL "
            "AND end_time < datetime('now','-6 hours')"
        )
    except Exception as e:
        log_message(f\"Error pruning old stats: {e}\", \"ERROR\")

    for stat in stats:
        server_name = stat.get('svname', '')
        connections = int(stat.get('scur', 0) or 0)
        bytes_in = int(stat.get('bin', 0) or 0)
        bytes_out = int(stat.get('bout', 0) or 0)

        cursor.execute(
            \"\"\"\n            INSERT INTO connection_stats (server_name, connections, bytes_in, bytes_out)\n            VALUES (?, ?, ?, ?)\n        \"\"\",\n            (server_name, connections, bytes_in, bytes_out),
        )

    conn.commit()
    conn.close()

# ==================== Telegram Bot Functions ====================

class TelegramBot:
    def __init__(self, token: str, admin_chat_id: str):
        self.token = token
        self.admin_chat_id = admin_chat_id
        self.api_url = f"https://api.telegram.org/bot{token}"
        self.last_update_id = 0

    def get_reply_keyboard(self) -> Dict:
        """Reply keyboard that appears under the input bar."""
        config = load_config()
        server_names = config.get("server_names", [])
        
        keyboard = []
        # Add status button
        keyboard.append([{"text": "📊 وضعیت هاپروکسی"}])
        
        # Add server-specific buttons (2 per row)
        if server_names:
            row = []
            for i, server_name in enumerate(server_names):
                if i > 0 and i % 2 == 0:
                    keyboard.append(row)
                    row = []
                row.append({"text": f"📈 {server_name}"})
            if row:
                keyboard.append(row)
        
        # Add stats button
        keyboard.append([{"text": "📈 آمار 24 ساعته"}])
        
        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "is_persistent": True
        }
        
    def send_message(self, text: str, reply_markup: Optional[Dict] = None) -> bool:
        """Send message to admin"""
        try:
            data = {
                "chat_id": self.admin_chat_id,
                "text": text,
                "parse_mode": "HTML"
            }
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            
            response = requests.post(
                f"{self.api_url}/sendMessage",
                json=data,
                timeout=10
            )
            ok = response.status_code == 200
            if not ok:
                log_message(f"Telegram send failed: {response.status_code} {response.text}", "ERROR")
            return ok
        except Exception as e:
            log_message(f"Error sending Telegram message: {e}", "ERROR")
            return False

    def send_state_change_notification(self, server_name: str, old_state: str, new_state: str, stat: Optional[Dict] = None):
        """Send a concise professional alert when server state changes"""
        emoji = "🟢" if new_state == "up" else "🔴"
        status_text = "آنلاین" if new_state == "up" else "آفلاین"
        old_text = "آنلاین" if old_state == "up" else "آفلاین"

        # Extract extra metrics
        uptime_txt = "نامشخص"
        traffic_txt = "نامشخص"
        conn_txt = "نامشخص"

        if stat:
            try:
                lastchg = int(stat.get("lastchg", 0) or 0)
                uptime_txt = self.format_duration(timedelta(seconds=lastchg))
            except Exception:
                pass
            try:
                bytes_in = int(stat.get("bin", 0) or 0)
                bytes_out = int(stat.get("bout", 0) or 0)
                traffic_txt = f"{self.format_bytes(bytes_in + bytes_out)} (دریافت {self.format_bytes(bytes_in)}, ارسال {self.format_bytes(bytes_out)})"
            except Exception:
                pass
            try:
                current_conn = stat.get("scur", "0") or "0"
                total_conn = stat.get("stot", "0") or "0"
                conn_txt = f"فعلی {current_conn} / کل {total_conn}"
            except Exception:
                pass

        message = f"{emoji} <b>هشدار تغییر وضعیت سرور</b>\n\n"
        message += f"سرور: <code>{server_name}</code>\n"
        message += f"وضعیت جدید: <b>{status_text}</b>\n"
        message += f"وضعیت قبلی: {old_text}\n"
        message += f"زمان: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"پایداری تا قبل از تغییر: {uptime_txt}\n"
        message += f"اتصالات: {conn_txt}\n"
        message += f"مصرف ترافیک: {traffic_txt}\n"
        message += "━━━━━━━━━━━━━━\n"
        message += "در صورت ادامه قطعی، لطفاً وضعیت زیرساخت را بررسی کنید."
        
        # Send notification without reply keyboard to avoid repetition
        sent = self.send_message(message, reply_markup=None)
        if sent:
            log_message(f"Alert sent to admin for {server_name}: {old_state}->{new_state}")
        else:
            log_message(f"Alert FAILED to send for {server_name}: {old_state}->{new_state}", "ERROR")

    def format_bytes(self, bytes_count: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.2f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.2f} PB"
    
    def format_duration(self, duration: timedelta) -> str:
        """Format duration to human readable format"""
        if duration is None:
            return "نامشخص"
        
        total_seconds = int(duration.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        parts = []
        if days > 0:
            parts.append(f"{days} روز")
        if hours > 0:
            parts.append(f"{hours} ساعت")
        if minutes > 0:
            parts.append(f"{minutes} دقیقه")
        if seconds > 0 and len(parts) < 2:
            parts.append(f"{seconds} ثانیه")
        
        return " ".join(parts) if parts else "0 ثانیه"
    
    def get_status_message(self) -> str:
        """Get detailed status message"""
        stats = get_haproxy_stats()
        message = "📊 <b>وضعیت کامل HAProxy</b>\n\n"
        message += f"⏰ زمان: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for stat in stats:
            server_name = stat.get('svname', '')
            status = stat.get('status', 'DOWN')
            status_emoji = "🟢" if 'UP' in status else "🔴"
            
            # Get additional info
            uptime = get_server_uptime(server_name)
            usage = get_server_usage_stats(server_name, 24)
            try:
                lastchg_seconds = int(stat.get("lastchg", 0) or 0)
                current_uptime_txt = self.format_duration(timedelta(seconds=lastchg_seconds))
            except Exception:
                current_uptime_txt = "نامشخص"
            
            current_conn = stat.get('scur', '0')
            total_conn = stat.get('stot', '0')
            bytes_in = int(stat.get('bin', 0) or 0)
            bytes_out = int(stat.get('bout', 0) or 0)
            
            message += f"{status_emoji} <b>{server_name}</b>\n"
            message += f"   وضعیت: <code>{status}</code>\n"
            message += f"   اتصالات فعلی: {current_conn}\n"
            message += f"   اتصالات کل: {total_conn}\n"
            message += f"   مدت اتصال فعلی (HAProxy): {current_uptime_txt}\n"
            
            if uptime:
                message += f"   مدت فعالیت: {self.format_duration(uptime)}\n"
            
            if usage['session_duration']:
                message += f"   مدت اتصال فعلی: {self.format_duration(usage['session_duration'])}\n"
            
            message += f"   دریافت: {self.format_bytes(bytes_in)}\n"
            message += f"   ارسال: {self.format_bytes(bytes_out)}\n"
            message += f"   کل (24h): {self.format_bytes(usage['total_bytes_in'] + usage['total_bytes_out'])}\n"
            message += "\n"
        
        return message
    
    def get_status_keyboard(self) -> Dict:
        """Get status keyboard"""
        return {
            "inline_keyboard": [
                [
                    {"text": "🔄 بروزرسانی استاتوس", "callback_data": "refresh_status"}
                ],
                [
                    {"text": "📊 جزئیات کامل", "callback_data": "full_details"},
                    {"text": "📈 آمار 24 ساعته", "callback_data": "stats_24h"}
                ],
                [
                    {"text": "ℹ️ راهنما", "callback_data": "help"}
                ]
            ]
        }
    
    def get_updates(self) -> List[Dict]:
        """Get updates from Telegram"""
        try:
            response = requests.get(
                f"{self.api_url}/getUpdates",
                params={"offset": self.last_update_id + 1, "timeout": 10},
                timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    updates = data.get("result", [])
                    if updates:
                        self.last_update_id = updates[-1]["update_id"]
                    return updates
        except Exception as e:
            log_message(f"Error getting updates: {e}", "ERROR")
        return []
    
    def handle_callback(self, callback_data: str, chat_id: str, message_id: int):
        """Handle callback queries"""
        if callback_data == "refresh_status":
            message = self.get_status_message()
            keyboard = self.get_status_keyboard()
            self.edit_message(chat_id, message_id, message, keyboard)
        elif callback_data == "full_details":
            message = self.get_status_message() + "\n\n📋 <b>جزئیات بیشتر:</b>\n"
            stats = get_haproxy_stats()
            for stat in stats:
                server_name = stat.get('svname', '')
                message += f"\n<b>{server_name}:</b>\n"
                message += f"  Check Status: {stat.get('check_status', 'N/A')}\n"
                message += f"  Last Check: {stat.get('lastchk', 'N/A')}\n"
                message += f"  Weight: {stat.get('weight', 'N/A')}\n"
            keyboard = self.get_status_keyboard()
            self.edit_message(chat_id, message_id, message, keyboard)
        elif callback_data == "stats_24h":
            message = "📈 <b>آمار 24 ساعته</b>\n\n"
            stats = get_haproxy_stats()
            for stat in stats:
                server_name = stat.get('svname', '')
                usage = get_server_usage_stats(server_name, 24)
                message += f"<b>{server_name}:</b>\n"
                message += f"  اتصالات کل: {usage['total_connections']}\n"
                message += f"  دریافت: {self.format_bytes(usage['total_bytes_in'])}\n"
                message += f"  ارسال: {self.format_bytes(usage['total_bytes_out'])}\n\n"
            keyboard = self.get_status_keyboard()
            self.edit_message(chat_id, message_id, message, keyboard)
        elif callback_data == "help":
            message = "ℹ️ <b>راهنمای ربات</b>\n\n"
            message += "این ربات وضعیت HAProxy را مانیتور می‌کند.\n\n"
            message += "<b>کلیدها:</b>\n"
            message += "🔄 بروزرسانی استاتوس: نمایش وضعیت فعلی\n"
            message += "📊 جزئیات کامل: نمایش جزئیات بیشتر\n"
            message += "📈 آمار 24 ساعته: نمایش آمار 24 ساعت گذشته\n"
            keyboard = self.get_status_keyboard()
            self.edit_message(chat_id, message_id, message, keyboard)
    
    def edit_message(self, chat_id: str, message_id: int, text: str, reply_markup: Optional[Dict] = None):
        """Edit a message"""
        try:
            data = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML"
            }
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            
            requests.post(f"{self.api_url}/editMessageText", json=data, timeout=10)
        except Exception as e:
            log_message(f"Error editing message: {e}", "ERROR")
    
    def answer_callback(self, callback_id: str):
        """Answer callback query"""
        try:
            requests.post(
                f"{self.api_url}/answerCallbackQuery",
                json={"callback_query_id": callback_id},
                timeout=10
            )
        except:
            pass

# ==================== Main Monitoring Functions ====================

def monitor_state_changes(bot: TelegramBot):
    """Monitor state changes based directly on HAProxy stats with persistent last-state tracking"""
    # Load last states from disk (so restart وسط کار باعث از دست رفتن تغییرات نشود)
    last_states: Dict[str, str] = load_last_states()
    if last_states:
        log_message(f"Loaded last states from disk: {last_states}")

    while True:
        try:
            stats = get_haproxy_stats()
            current_states: Dict[str, str] = {}

            for stat in stats:
                name = stat.get("svname", "")
                status = (stat.get("status", "") or "").upper()
                if not name:
                    continue
                current_states[name] = "up" if "UP" in status else "down"

            # اگر اولین بار است و هیچ state قبلی نداریم، فقط baseline را ذخیره می‌کنیم (هشدار نمی‌فرستیم)
            if not last_states:
                last_states = current_states.copy()
                save_last_states(last_states)
                log_message(f"Baseline states initialized from HAProxy for alerting: {last_states}")
            else:
                for server_name, current_state in current_states.items():
                    last_state = last_states.get(server_name)
                    if last_state is None:
                        # سرور جدید
                        log_message(f"New server detected in stats: {server_name} state={current_state}")
                        last_states[server_name] = current_state
                        continue

                    if last_state != current_state:
                        log_message(f"State change detected (from HAProxy): {server_name} {last_state} -> {current_state}")
                        stat = next((s for s in stats if s.get("svname") == server_name), {})
                        bot.send_state_change_notification(server_name, last_state, current_state, stat)
                        save_server_state_change(server_name, current_state)
                        last_states[server_name] = current_state

                # ذخیره وضعیت جدید روی دیسک
                save_last_states(last_states)

            # Save connection stats periodically
            save_connection_stats()

            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_message(f"Error in monitoring loop: {e}\n{traceback.format_exc()}", "ERROR")
            time.sleep(5)

def main():
    """Main function"""
    log_message("Starting HAProxy Telegram Bot...")
    
    # Initialize database
    init_database()
    
    # Load config
    config = load_config()
    
    if config["bot_token"] == "YOUR_BOT_TOKEN_HERE":
        log_message("ERROR: Please configure bot_token and admin_chat_id in config file", "ERROR")
        log_message(f"Config file: {CONFIG_FILE}", "ERROR")
        return
    
    # Create bot instance
    bot = TelegramBot(config["bot_token"], config["admin_chat_id"])
    
    # Send an initial menu (reply keyboard under input)
    bot.send_message("ربات فعال است. از دکمه‌ها استفاده کنید.", bot.get_reply_keyboard())
    
    # Start monitoring in background thread
    import threading
    monitor_thread = threading.Thread(target=monitor_state_changes, args=(bot,), daemon=True)
    monitor_thread.start()
    
    # Main loop for handling commands
    log_message("Bot is running. Waiting for commands...")
    
    while True:
        try:
            updates = bot.get_updates()
            for update in updates:
                if "callback_query" in update:
                    callback = update["callback_query"]
                    callback_data = callback.get("data", "")
                    chat_id = str(callback["message"]["chat"]["id"])
                    message_id = callback["message"]["message_id"]
                    callback_id = callback["id"]
                    
                    bot.answer_callback(callback_id)
                    bot.handle_callback(callback_data, chat_id, message_id)
                
                elif "message" in update:
                    message = update["message"]
                    chat_id = str(message["chat"]["id"])
                    text = message.get("text", "")
                    
                    # Only respond to admin
                    if chat_id == config["admin_chat_id"]:
                        if text in ["/start", "/status", "/استاتوس", "📊 وضعیت هاپروکسی"]:
                            status_msg = bot.get_status_message()
                            bot.send_message(status_msg, bot.get_reply_keyboard())
                        elif text in ["/help", "/راهنما"]:
                            help_msg = "ℹ️ <b>دستورات ربات:</b>\n\n"
                            help_msg += "/start یا /status - نمایش وضعیت\n"
                            help_msg += "/help - نمایش راهنما\n"
                            help_msg += "📊 وضعیت هاپروکسی - نمایش وضعیت\n"
                            help_msg += "📈 آمار 24 ساعته - نمایش آمار 24h"
                            bot.send_message(help_msg, bot.get_reply_keyboard())
                        elif text in ["📈 آمار 24 ساعته"]:
                            # استفاده از همان پیام وضعیت برای سادگی
                            status_msg = bot.get_status_message()
                            bot.send_message(status_msg, bot.get_reply_keyboard())
                        elif text.startswith("📈 "):
                            # Server-specific status
                            server_name = text.replace("📈 ", "").strip()
                            stats = get_haproxy_stats()
                            server_stat = next((s for s in stats if s.get('svname') == server_name), None)
                            if server_stat:
                                usage = get_server_usage_stats(server_name, 24)
                                msg = f"📊 <b>وضعیت {server_name}</b>\n\n"
                                status = server_stat.get('status', 'DOWN')
                                status_emoji = "🟢" if 'UP' in status else "🔴"
                                msg += f"{status_emoji} وضعیت: <code>{status}</code>\n"
                                msg += f"اتصالات فعلی: {server_stat.get('scur', '0')}\n"
                                msg += f"اتصالات کل: {server_stat.get('stot', '0')}\n"
                                msg += f"دریافت: {bot.format_bytes(int(server_stat.get('bin', 0) or 0))}\n"
                                msg += f"ارسال: {bot.format_bytes(int(server_stat.get('bout', 0) or 0))}\n"
                                msg += f"\n<b>آمار 24 ساعته:</b>\n"
                                msg += f"اتصالات کل: {usage['total_connections']}\n"
                                msg += f"دریافت: {bot.format_bytes(usage['total_bytes_in'])}\n"
                                msg += f"ارسال: {bot.format_bytes(usage['total_bytes_out'])}\n"
                                bot.send_message(msg, bot.get_reply_keyboard())
                            else:
                                bot.send_message(f"سرور {server_name} یافت نشد.", bot.get_reply_keyboard())
            
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_message(f"Error in main loop: {e}", "ERROR")
            time.sleep(5)
    
    log_message("Bot stopped")

if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            log_message("Bot stopped by KeyboardInterrupt", "INFO")
            break
        except Exception as e:
            log_message(f"FATAL error, restarting bot loop: {e}\n{traceback.format_exc()}", "ERROR")
            time.sleep(5)
EOF
    chmod +x "$BOT_SCRIPT"
    # ensure dependencies (prefer apt; fallback to pip with --break-system-packages on Ubuntu 24+)
    if ! python3 -c "import requests" >/dev/null 2>&1; then
        if command -v apt-get >/dev/null 2>&1; then
            apt-get install -y python3-requests >> "$LOG_FILE" 2>&1 || true
        fi
        if ! python3 -c "import requests" >/dev/null 2>&1; then
            python3 -m pip install --quiet --break-system-packages requests >> "$LOG_FILE" 2>&1 || true
        fi
    fi
}

enable_services() {
    log "Enabling services"
    systemctl daemon-reload
    systemctl enable haproxy
    systemctl enable haproxy-failover
    systemctl enable haproxy-telegram-bot
    systemctl restart haproxy
    systemctl restart haproxy-failover
    systemctl restart haproxy-telegram-bot
}

install_stack() {
    require_root
    ensure_packages
    prompt_install_data
    write_haproxy_cfg
    write_check_script
    write_failover_script
    write_bot_config
    write_bot_service
    write_failover_service
    write_bot_script
    mkdir -p "$(dirname "$DB_FILE")"
    touch "$DB_FILE" "$LAST_STATES_FILE"
    enable_services
    log "Installation completed."
    # تشخیص IPv4 اصلی سرور برای نمایش آدرس پنل استاتوس
    SERVER_IP=$(ip -4 addr show scope global 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -n1)
    if [ -z "$SERVER_IP" ]; then
        SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi
    if [ -z "$SERVER_IP" ]; then
        SERVER_IP="<server_ip>"
    fi

    echo
    echo "====================================================="
    echo "           HAProxy Status Panel (Stats)"
    echo "-----------------------------------------------------"
    echo "  URL  : http://${SERVER_IP}:${STATS_PORT}${STATS_URI}"
    echo "  User : ${STATS_USER}"
    echo "  Pass : ${STATS_PASS}"
    echo "====================================================="
    echo
}

uninstall_stack() {
    log "Stopping services"
    systemctl stop haproxy-telegram-bot || true
    systemctl stop haproxy-failover || true
    # do not stop haproxy core; optional
    log "Disabling services"
    systemctl disable haproxy-telegram-bot || true
    systemctl disable haproxy-failover || true

    log "Removing files"
    rm -f "$BOT_SERVICE" "$FAILOVER_SERVICE"
    rm -f "$CHECK_SCRIPT" "$FAILOVER_SCRIPT" "$BOT_CONFIG"
    # keep haproxy.cfg and bot/db/state unless forced
    read -p "Remove HAProxy config ($HAPROXY_CFG)? [y/N]: " RMC
    if [[ "${RMC,,}" == "y" ]]; then rm -f "$HAPROXY_CFG"; fi
    read -p "Remove bot state/db files ($LAST_STATES_FILE, $DB_FILE)? [y/N]: " RMS
    if [[ "${RMS,,}" == "y" ]]; then rm -f "$LAST_STATES_FILE" "$DB_FILE"; fi

    systemctl daemon-reload
    log "Uninstall completed."
}

show_status() {
    systemctl status haproxy --no-pager || true
    systemctl status haproxy-failover --no-pager || true
    systemctl status haproxy-telegram-bot --no-pager || true
    echo
    echo "Current servers in HAProxy:"
    echo "show stat" | sudo socat stdio /var/run/haproxy.sock 2>/dev/null | grep -E "vless_s"
}

case "${1:-}" in
    install)
        install_stack
        ;;
    uninstall)
        uninstall_stack
        ;;
    status)
        show_status
        ;;
    *)
        show_banner
        
        echo ""
        echo "1) Install"
        echo "2) Uninstall"
        echo "3) Status"
        echo "q) Quit"
        echo ""
        read -p "Select an option: " opt
        case "$opt" in
            1) install_stack ;;
            2) uninstall_stack ;;
            3) show_status ;;
            q|Q) exit 0 ;;
            *) echo "Invalid option"; exit 1 ;;
        esac
        ;;
esac


