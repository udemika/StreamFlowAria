#!/usr/bin/env bash
set -e

echo "================================================================"
echo "     Управление медиасервером StreamFlow Aria (Ubuntu / Debian)    "
echo "     Режим: Remux (-c copy) | 30s RAM Buffer | Cluster | DVR    "
echo "================================================================"
echo ""

echo "Выберите действие:"
echo "1) Установить сервер"
echo "2) Удалить сервер"
read -p "Ваш выбор [1]: " ACTION_CHOICE </dev/tty
ACTION_CHOICE=${ACTION_CHOICE:-1}

if [ "$ACTION_CHOICE" == "2" ]; then
    echo "Удаление StreamFlow Aria..."
    systemctl stop flustream 2>/dev/null || true
    systemctl disable flustream 2>/dev/null || true
    rm -f /etc/systemd/system/flustream.service
    systemctl daemon-reload
    pkill -9 -f "/opt/flustream/server.py" 2>/dev/null || true
    rm -rf /opt/flustream /etc/flustream /var/lib/flustream /dev/shm/flustream
    echo "Сервер и все его компоненты успешно удалены."
    exit 0
fi

# 1. Интерактивный ввод порта, логина и пароля
read -p "Введите HTTP-порт для веб-интерфейса и потоков [по умолчанию 8080]: " INPUT_PORT < /dev/tty
FLUSTREAM_PORT=${INPUT_PORT:-8080}
if ! [[ "$FLUSTREAM_PORT" =~ ^[0-9]+$ ]] || [ "$FLUSTREAM_PORT" -lt 1 ] || [ "$FLUSTREAM_PORT" -gt 65535 ]; then
    echo "Некорректный номер порта. Будет использован порт по умолчанию: 8080"
    FLUSTREAM_PORT=8080
fi

read -p "Введите логин администратора веб-интерфейса [по умолчанию admin]: " INPUT_USER < /dev/tty
FLUSTREAM_USER=${INPUT_USER:-admin}

while true; do
    read -s -p "Введите пароль администратора веб-интерфейса [по умолчанию admin]: " INPUT_PASS < /dev/tty
    echo ""
    FLUSTREAM_PASS=${INPUT_PASS:-admin}
    if [ -n "$FLUSTREAM_PASS" ]; then
        break
    fi
done

echo ""
echo "----------------------------------------------------------------"
echo " Выбранные параметры StreamFlow Aria:"
echo "   • Сервер:          StreamFlow Aria"
echo "   • Буфер потока:    30 секунд (RAM /dev/shm)"
echo "   • HTTP-порт:       ${FLUSTREAM_PORT}"
echo "   • Логин админа:    ${FLUSTREAM_USER}"
echo "   • Пароль админа:   (установлен)"
echo "----------------------------------------------------------------"
echo ""

echo "[1/5] Установка системных пакетов (ffmpeg, python3-venv, curl)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ffmpeg python3 python3-venv python3-pip curl ca-certificates

echo "[2/5] Остановка старых процессов и создание каталогов (/dev/shm/flustream)..."
systemctl stop flustream 2>/dev/null || true
pkill -9 -f "/opt/flustream/server.py" 2>/dev/null || true
mkdir -p /opt/flustream
mkdir -p /etc/flustream
mkdir -p /var/lib/flustream/dvr
mkdir -p /dev/shm/flustream

echo "[3/5] Настройка изолированного окружения Python..."
python3 -m venv /opt/flustream/venv
/opt/flustream/venv/bin/pip install --upgrade pip
/opt/flustream/venv/bin/pip install aiohttp psutil uvloop

echo "[4/5] Запись оптимизированного ядра StreamFlow Aria (/opt/flustream/server.py)..."
cat << 'PYEOF' > /opt/flustream/server.py
#!/usr/bin/env python3
import asyncio
import base64
import fnmatch
import hashlib
import hmac
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin
import psutil
from aiohttp import web, ClientSession, ClientTimeout

CONFIG_FILE = Path("/etc/flustream/flustream.conf")
STATE_JSON = Path("/etc/flustream/streams.json")
RAM_ROOT = Path("/dev/shm/flustream")
DVR_ROOT = Path("/var/lib/flustream/dvr")
SERVER_START_TIME = time.time()
COOKIE_NAME = "streamflow_aria_auth"
VERSION_STR = "Aria-26.04"

HLS_SEG_TIME = 4
PREBUFFER_MIN_SEGS = 2


def normalize_input_url(raw_url: str) -> str:
    u = raw_url.strip()
    if u.startswith("hlss://"):
        return "https://" + u[7:]
    if u.startswith("hls://"):
        return "http://" + u[6:]
    if u.startswith("tshttps://"):
        return "https://" + u[10:]
    if u.startswith("tshttp://"):
        return "http://" + u[9:]
    if u.startswith(("m4fs://", "m4ss://")):
        rest = u.split("://", 1)[1].rstrip("/")
        return f"https://{rest}/mpegts" if not rest.endswith(("/mpegts", ".m3u8")) else f"https://{rest}"
    if u.startswith(("m4f://", "m4s://")):
        rest = u.split("://", 1)[1].rstrip("/")
        return f"http://{rest}/mpegts" if not rest.endswith(("/mpegts", ".m3u8")) else f"http://{rest}"
    if u.startswith(("dashs://", "mpds://")):
        return "https://" + u.split("://", 1)[1]
    if u.startswith(("dash://", "mpd://")):
        return "http://" + u.split("://", 1)[1]
    return u


def generate_flustream_conf(data: dict) -> str:
    lines = [
        "# Global StreamFlow Aria Configuration",
        f"http {data.get('http_port', 8080)};",
        f"edit_auth {data.get('admin_user', 'admin')} {data.get('admin_pass', 'admin')};",
        f"dvr_root {data.get('dvr_root', '/var/lib/flustream/dvr')} {data.get('dvr_disk_limit', 85)}%;",
    ]
    if data.get("global_auth_url"):
        lines.append(f"auth_backend {data['global_auth_url']};")
    if data.get("cluster_key"):
        lines.append(f"cluster_key {data['cluster_key']};")
    for peer in data.get("peers", []):
        if peer.strip():
            lines.append(f"peer {peer.strip()};")
    for src in data.get("cluster_sources", []):
        host = src.get("host", "").strip()
        if not host:
            continue
        lines.append(f"source {host} {{")
        if src.get("proto"):
            lines.append(f"  proto {src['proto']};")
        if src.get("only"):
            lines.append(f"  only {src['only']};")
        if src.get("except"):
            lines.append(f"  except {src['except']};")
        lines.append("}")
    lines.append("")

    for s in data.get("streams", []):
        directive = "ondemand" if s.get("mode") == "ondemand" else "stream"
        lines.append(f"{directive} {s['name']} {{")
        if s.get("title"):
            lines.append(f"  title \"{s['title']}\";")
        for inp in s.get("inputs", []):
            if inp.strip():
                lines.append(f"  input {inp.strip()};")
        lines.append(f"  source_timeout {s.get('source_timeout', 30)};")
        lines.append(f"  buffer_mode {s.get('buffer_mode', '30s')};")
        lines.append(f"  quality {s.get('quality', 'best')};")
        if s.get("user_agent"):
            lines.append(f"  user_agent \"{s['user_agent']}\";")
        if s.get("mode") == "ondemand":
            lines.append(f"  clients_timeout {s.get('clients_timeout', 45)};")
        if int(s.get("dvr_hours", 0)) > 0:
            lines.append(f"  dvr {DVR_ROOT} {int(s['dvr_hours'])}h;")
        for p in s.get("pushes", []):
            if p.strip():
                lines.append(f"  push {p.strip()};")
        if s.get("token"):
            lines.append(f"  token {s['token']};")
        if s.get("on_play"):
            lines.append(f"  on_play {s['on_play']};")
        if not s.get("enabled", True):
            lines.append("  disabled;")
        lines.append("}")
        lines.append("")
    return "\n".join(lines)


def parse_flustream_conf(text: str, current_data: dict) -> dict:
    data = {
        "http_port": current_data.get("http_port", 8080),
        "admin_user": current_data.get("admin_user", "admin"),
        "admin_pass": current_data.get("admin_pass", "admin"),
        "ui_theme": current_data.get("ui_theme", "2"),
        "ui_lang": current_data.get("ui_lang", "ru"),
        "dvr_root": current_data.get("dvr_root", "/var/lib/flustream/dvr"),
        "dvr_disk_limit": current_data.get("dvr_disk_limit", 85),
        "global_auth_url": "",
        "cluster_key": current_data.get("cluster_key", "flustream_secret"),
        "peers": [],
        "cluster_sources": [],
        "streams": []
    }
    m_port = re.search(r"^\s*http\s+(\d+)\s*;", text, re.M)
    if m_port:
        data["http_port"] = int(m_port.group(1))
    m_eauth = re.search(r"^\s*edit_auth\s+(\S+)\s+(\S+)\s*;", text, re.M)
    if m_eauth:
        data["admin_user"] = m_eauth.group(1)
        data["admin_pass"] = m_eauth.group(2)
    m_dvr = re.search(r"^\s*dvr_root\s+(\S+)\s+(\d+)%\s*;", text, re.M)
    if m_dvr:
        data["dvr_root"] = m_dvr.group(1)
        data["dvr_disk_limit"] = int(m_dvr.group(2))
    m_auth = re.search(r"^\s*auth_backend\s+(\S+)\s*;", text, re.M)
    if m_auth:
        data["global_auth_url"] = m_auth.group(1)
    m_ckey = re.search(r"^\s*cluster_key\s+(\S+)\s*;", text, re.M)
    if m_ckey:
        data["cluster_key"] = m_ckey.group(1)
    for m_peer in re.finditer(r"^\s*peer\s+([^;{]+)", text, re.M):
        data["peers"].append(m_peer.group(1).strip())
    for m_src in re.finditer(r"source\s+(\S+)\s*\{([^}]*)\}", text, re.S):
        shost, sbody = m_src.group(1).strip(), m_src.group(2)
        src_obj = {"host": shost, "proto": "hls", "only": "", "except": ""}
        for sline in sbody.splitlines():
            sl = sline.strip().rstrip(";")
            if sl.startswith("only "):
                src_obj["only"] = sl.split(" ", 1)[1].strip()
            elif sl.startswith("except "):
                src_obj["except"] = sl.split(" ", 1)[1].strip()
            elif sl.startswith("proto "):
                src_obj["proto"] = sl.split(" ", 1)[1].strip()
        data["cluster_sources"].append(src_obj)

    block_re = re.compile(r"(stream|ondemand)\s+([a-zA-Z0-9_\-]+)\s*\{([^}]*)\}", re.S)
    for match in block_re.finditer(text):
        mode, name, body = match.group(1), match.group(2), match.group(3)
        st = {
            "name": name,
            "title": name,
            "mode": mode,
            "quality": "best",
            "buffer_mode": "30s",
            "user_agent": "",
            "enabled": True,
            "inputs": [],
            "source_timeout": 30,
            "clients_timeout": 45,
            "dvr_hours": 0,
            "pushes": [],
            "token": "",
            "on_play": ""
        }
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(";")
            if not line or line.startswith("#"):
                continue
            if line.startswith("buffer_mode "): st["buffer_mode"] = line.split(" ", 1)[1].strip()
            elif line.startswith("title "):
                st["title"] = line.split(" ", 1)[1].strip('"\'')
            elif line.startswith("input "):
                st["inputs"].append(line.split(" ", 1)[1].strip())
            elif line.startswith("buffer_mode "): st["buffer_mode"] = line.split(" ", 1)[1].strip()
            elif line.startswith("quality "): st["quality"] = line.split(" ", 1)[1].strip()
            elif line.startswith("user_agent "): st["user_agent"] = line.split(" ", 1)[1].strip('"\'')
            elif line.startswith("source_timeout "):
                st["source_timeout"] = int(line.split(" ", 1)[1].strip())
            elif line.startswith("clients_timeout "):
                st["clients_timeout"] = int(line.split(" ", 1)[1].strip())
            elif line.startswith("dvr "):
                for p in line.split()[1:]:
                    if p.endswith("h"):
                        st["dvr_hours"] = int(p[:-1])
                    elif p.endswith("d"):
                        st["dvr_hours"] = int(p[:-1]) * 24
            elif line.startswith("push "):
                st["pushes"].append(line.split(" ", 1)[1].strip())
            elif line.startswith("token "):
                st["token"] = line.split(" ", 1)[1].strip()
            elif line.startswith("on_play "):
                st["on_play"] = line.split(" ", 1)[1].strip()
            elif line == "disabled":
                st["enabled"] = False
        data["streams"].append(st)
    return data


class PlaySession:
    def __init__(self, session_id: str, stream_name: str, ip: str, proto: str, user_agent: str):
        self.id = session_id
        self.stream_name = stream_name
        self.ip = ip
        self.proto = proto
        self.user_agent = user_agent[:80]
        self.started_at = time.time()
        self.last_seen = time.time()
        self.bytes_sent = 0
        self.kicked = False

    def to_dict(self):
        dur = max(1, int(time.time() - self.started_at))
        return {
            "id": self.id,
            "stream": self.stream_name,
            "ip": self.ip,
            "proto": self.proto,
            "user_agent": self.user_agent,
            "duration": dur,
            "bytes_sent": self.bytes_sent,
            "mbits": round((self.bytes_sent * 8) / dur / 1_000_000, 3)
        }


class StreamWorker:
    def __init__(self, cfg: dict, manager: "StreamManager"):
        self.cfg = cfg
        self.manager = manager
        self.name = cfg["name"]
        self.ram_dir = RAM_ROOT / self.name
        self.dvr_dir = DVR_ROOT / self.name
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.dash_proc: Optional[asyncio.subprocess.Process] = None
        self.last_dash_req: float = 0.0
        self.push_procs: Dict[str, asyncio.subprocess.Process] = {}
        self.active_input_idx = 0
        self.status = "Disabled"
        self.started_at: Optional[float] = None
        self.last_segment_time: float = time.time()
        self.last_client_time: float = time.time()
        self.bitrate_in_kbps: float = 0.0
        self.cached_buffer_sec: int = 0
        self.codec_info: str = "Pass-through (-c copy)"
        self.probed_once: bool = False
        self.processed_segments: set = set()
        self.stop_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
        self._kbps_samples: List[float] = []

    def start(self):
        self.stop_event.clear()
        self.task = asyncio.create_task(self._lifecycle_loop())

    async def stop(self):
        self.stop_event.set()
        await self._kill_ffmpeg()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.status = "Disabled"

    def touch_client(self):
        self.last_client_time = time.time()

    def active_clients_count(self) -> int:
        return self.manager.active_clients_map.get(self.name, 0)

    def running_pushes_count(self) -> int:
        return sum(1 for p in self.push_procs.values() if p.returncode is None)

    def target_buffer_seconds(self) -> int:
        return 6 if self.cfg.get("buffer_mode") == "normal" else 30

    def buffered_seconds(self) -> int:
        return self.cached_buffer_sec if self.status == "Online" else 0
    async def _kill_ffmpeg(self):

        for p in list(self.push_procs.values()):
            if p.returncode is None:
                try:
                    p.kill()
                except Exception:
                    pass
        self.push_procs.clear()
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), timeout=2.0)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        self.proc = None
        self.bitrate_in_kbps = 0.0
        self.cached_buffer_sec = 0
        self._kbps_samples.clear()

    async def _resolve_best_variant_url(self, raw_url: str) -> str:
        input_url = normalize_input_url(raw_url)
        pref = str(self.cfg.get("quality", "best")).lower()
        if not input_url.startswith(("http://", "https://")) or not self.manager.http_client:
            return raw_url
        if any(input_url.lower().endswith(ext) for ext in (".ts", "/mpegts", ".mp4", ".flv")):
            return raw_url
        try:
            headers = {
                "User-Agent": (self.cfg.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            }
            async with self.manager.http_client.get(input_url, headers=headers, timeout=ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return raw_url
                ct = resp.headers.get("Content-Type", "").lower()
                if "video/mp2t" in ct or "octet-stream" in ct:
                    return raw_url
                raw_chunk = await resp.content.read(32768)
                text = raw_chunk.decode("utf-8", errors="ignore")
                final_base_url = str(resp.url)
        except Exception:
            return raw_url

        if "#EXT-X-STREAM-INF" not in text:
            return input_url

        variants = []
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("#EXT-X-STREAM-INF:"):
                bw = 0
                height = 0
                m_bw = re.search(r"AVERAGE-BANDWIDTH=(\d+)", line) or re.search(r"BANDWIDTH=(\d+)", line)
                if m_bw:
                    bw = int(m_bw.group(1))
                m_res = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
                if m_res:
                    height = int(m_res.group(2))
                j = i + 1
                while j < len(lines) and lines[j].startswith("#"):
                    j += 1
                if j < len(lines):
                    v_url = urljoin(final_base_url, lines[j])
                    variants.append({"bw": bw, "height": height, "url": v_url})
                i = j
            i += 1

        if not variants:
            return raw_url

        variants.sort(key=lambda x: (x["height"], x["bw"]))
        if pref == "low":
            chosen = variants[0]
        elif pref in ("1080", "1080p"):
            cands = [v for v in variants if v["height"] and v["height"] <= 1080]
            chosen = cands[-1] if cands else variants[-1]
        elif pref in ("720", "720p"):
            cands = [v for v in variants if v["height"] and v["height"] <= 720]
            chosen = cands[-1] if cands else variants[0]
        elif pref in ("480", "480p", "360", "360p"):
            cands = [v for v in variants if v["height"] and v["height"] <= 540]
            chosen = cands[-1] if cands else variants[0]
        else:
            chosen = variants[-1]
        return chosen["url"]

    def _build_primary_hls_cmd(self, raw_input_url: str) -> List[str]:
        input_url = normalize_input_url(raw_input_url)
        self.ram_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "ffmpeg", "-nostdin", "-y", "-hide_banner", "-loglevel", "error",
            "-threads", "1", "-probesize", "5000000", "-analyzeduration", "5000000",
            "-fflags", "+genpts+discardcorrupt+igndts"
        ]
        if input_url.startswith("rtsp://"):
            cmd += ["-rtsp_transport", "tcp"]
        elif input_url.startswith(("http://", "https://")):
            cmd += [
                "-rw_timeout", "15000000",
                "-user_agent", (self.cfg.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
                "-reconnect", "1",
                "-reconnect_delay_max", "5"
            ]
            if ".m3u8" not in input_url.lower() and ".mpd" not in input_url.lower() and not raw_input_url.startswith(("hls://", "hlss://")):
                cmd += ["-reconnect_streamed", "1"]
            else:
                cmd += ["-http_persistent", "1", "-live_start_index", "-3"]
        elif input_url.startswith("udp://"):
            cmd += ["-timeout", str(int(self.cfg.get("source_timeout", 15)) * 1_000_000)]
        elif raw_input_url.strip().startswith("srt://"):
            u_srt = raw_input_url.strip()
            # Если указан наш собственный сервер 62.60.250.90 или 0.0.0.0 без mode=caller — слушаем порт как сервер (listener)
            if "mode=listener" in u_srt or u_srt.startswith(("srt://0.0.0.0:", "srt://127.0.0.1:", "srt://62.60.250.90:")) and "mode=caller" not in u_srt:
                port_m = re.search(r":(\d+)", u_srt)
                p_num = port_m.group(1) if port_m else "54333"
                input_url = f"srt://0.0.0.0:{p_num}?mode=listener&timeout=15000000&latency=200000"
            else:
                input_url = u_srt if "?" in u_srt else f"{u_srt}?mode=caller&timeout=8000000&latency=200000"

        hls_list_sz = "2" if self.cfg.get("buffer_mode") == "normal" else "8"
        cmd += [
            "-i", input_url,
            "-map", "0:v:0?", "-map", "0:a?",
            "-sn", "-dn",
            "-map_metadata", "-1",
            "-map_chapters", "-1",
            "-c", "copy",
            "-mpegts_flags", "resend_headers",
            "-max_muxing_queue_size", "8192",
            "-avoid_negative_ts", "make_non_negative",
            "-f", "hls",
            "-hls_time", "6",
            "-hls_list_size", hls_list_sz,
            "-hls_delete_threshold", "1",
            "-hls_flags", "delete_segments+temp_file",
            "-hls_segment_filename", "seg_%06d.ts",
            "index.m3u8"
        ]
        return cmd

    async def _spawn_pushes(self):
        m3u8_path = self.ram_dir / "index.m3u8"
        if not m3u8_path.exists() or len(list(self.ram_dir.glob("seg_*.ts"))) < 1:
            return
        for raw_push in self.cfg.get("pushes", []):
            push_url = raw_push.strip()
            if not push_url:
                continue
            if push_url in self.push_procs:
                if self.push_procs[push_url].returncode is not None:
                    del self.push_procs[push_url]
                else:
                    continue

            target_url = push_url
            if target_url.startswith("srt://"):
                if "mode=caller" not in target_url:
                    port_m = re.search(r":(\d+)", target_url)
                    p_num = port_m.group(1) if port_m else "54333"
                    target_url = f"srt://0.0.0.0:{p_num}?mode=listener&pkt_size=1316&latency=200000"
                else:
                    if "pkt_size=" not in target_url:
                        target_url += ("&" if "?" in target_url else "?") + "pkt_size=1316"

            out_fmt = "mpegts" if target_url.startswith(("udp://", "srt://")) else "flv"
            cmd = [
                "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
                "-threads", "1", "-probesize", "5000000", "-analyzeduration", "5000000",
                "-re", "-live_start_index", "-2",
                "-i", "index.m3u8",
                "-map", "0:v:0?", "-map", "0:a?",
                "-c", "copy",
                "-f", out_fmt, target_url
            ]
            try:
                self.push_procs[push_url] = await asyncio.create_subprocess_exec(
                    *cmd, cwd=str(self.ram_dir),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                )
            except Exception:
                pass

    async def _probe_codecs(self, seg_file: Path):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "error", "-show_entries",
                "stream=codec_name,width,height", "-of", "json", str(seg_file),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=4.0)
            info = json.loads(out.decode("utf-8", errors="ignore"))
            parts = []
            for st in info.get("streams", []):
                cname = st.get("codec_name", "").upper()
                if st.get("width"):
                    parts.append(f"{cname} {st['width']}x{st['height']}")
                elif cname:
                    parts.append(cname)
            if parts:
                self.codec_info = " / ".join(parts)
        except Exception:
            pass

    async def _sync_ram_to_dvr(self):
        if not self.ram_dir.exists():
            self.cached_buffer_sec = 0
            return
        try:
            entries = [e for e in os.scandir(self.ram_dir) if e.name.startswith("seg_") and e.name.endswith(".ts")]
        except OSError:
            return
        if not entries:
            self.cached_buffer_sec = 0
            return

        entries.sort(key=lambda e: e.name)
        self.cached_buffer_sec = min(self.target_buffer_seconds(), len(entries) * HLS_SEG_TIME)
        new_entries = [e for e in entries if e.name not in self.processed_segments]
        if not new_entries:
            return

        now = time.time()
        seg_dur = float(HLS_SEG_TIME)
        m3u8_p = self.ram_dir / "index.m3u8"
        if m3u8_p.exists():
            try:
                m_txt = m3u8_p.read_text(encoding="utf-8", errors="ignore")
                m_inf = re.findall(r"#EXTINF:([0-9.]+)", m_txt)
                if m_inf:
                    seg_dur = max(0.5, float(m_inf[-1]))
            except Exception:
                pass

        dvr_hours = int(self.cfg.get("dvr_hours", 0))
        for entry in new_entries:
            try:
                st_stat = entry.stat()
            except FileNotFoundError:
                continue
            if st_stat.st_size == 0:
                continue
            self.processed_segments.add(entry.name)
            if len(self.processed_segments) > 200:
                self.processed_segments = set(sorted(self.processed_segments)[-100:])

            self.last_segment_time = now
            self.status = "Online"

            seg_kbps = (st_stat.st_size * 8.0) / seg_dur / 1000.0
            self._kbps_samples.append(seg_kbps)
            if len(self._kbps_samples) > 6:
                self._kbps_samples.pop(0)
            self.bitrate_in_kbps = int(round(sum(self._kbps_samples) / len(self._kbps_samples)))

            seg_path = Path(entry.path)
            if not self.probed_once:
                self.probed_once = True
                asyncio.create_task(self._probe_codecs(seg_path))

            if dvr_hours > 0:
                ts_int = int(st_stat.st_mtime)
                day_str = time.strftime("%Y-%m-%d", time.gmtime(ts_int))
                target_dir = self.dvr_dir / day_str
                target_dir.mkdir(parents=True, exist_ok=True)
                target_file = target_dir / f"{ts_int}_{HLS_SEG_TIME}.ts"
                if not target_file.exists():
                    try:
                        await asyncio.to_thread(shutil.copy2, seg_path, target_file)
                    except Exception:
                        pass

    async def _lifecycle_loop(self):
        while not self.stop_event.is_set():
            if not self.cfg.get("enabled", True) or not self.cfg.get("inputs"):
                self.status = "Disabled"
                self.started_at = None
                await self._kill_ffmpeg()
                shutil.rmtree(self.ram_dir, ignore_errors=True)
                await asyncio.sleep(1.5)
                continue

            if self.cfg.get("mode") == "ondemand":
                idle_sec = time.time() - self.last_client_time
                timeout_sec = max(30, int(self.cfg.get("clients_timeout", 45)))
                if self.active_clients_count() == 0 and idle_sec > timeout_sec:
                    if self.proc is not None:
                        await self._kill_ffmpeg()
                        shutil.rmtree(self.ram_dir, ignore_errors=True)
                    self.status = "Waiting"
                    self.started_at = None
                    await asyncio.sleep(1.0)
                    continue

            if self.proc is not None and self.proc.returncode is not None:
                try:
                    err_bytes = await self.proc.stderr.read()
                    lines = [l.strip() for l in err_bytes.decode("utf-8", errors="ignore").splitlines() if l.strip()]
                    if lines:
                        self.codec_info = f"ERR: {lines[-1][:65]}"
                except Exception:
                    pass
                self.status = "Error"
                await self._kill_ffmpeg()
                inputs = [i for i in self.cfg.get("inputs", []) if i.strip()]
                if inputs:
                    self.active_input_idx = (self.active_input_idx + 1) % len(inputs)
                await asyncio.sleep(3.0)

            if self.proc is None:
                inputs = [i for i in self.cfg.get("inputs", []) if i.strip()]
                if not inputs:
                    self.status = "Error"
                    await asyncio.sleep(3.0)
                    continue
                self.active_input_idx = self.active_input_idx % len(inputs)
                current_url = inputs[self.active_input_idx]

                shutil.rmtree(self.ram_dir, ignore_errors=True)
                self.ram_dir.mkdir(parents=True, exist_ok=True)
                self.processed_segments.clear()
                self.probed_once = False

                resolved_url = await self._resolve_best_variant_url(current_url)
                cmd = self._build_primary_hls_cmd(resolved_url)
                try:
                    self.proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        cwd=str(self.ram_dir),
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL
                    )
                    self.started_at = time.time()
                    self.last_segment_time = time.time()
                    if self.status != "Online":
                        self.status = "Waiting"
                except Exception as e:
                    self.codec_info = f"ERR: {str(e)[:50]}"
                    self.status = "Error"
                    self.active_input_idx = (self.active_input_idx + 1) % len(inputs)
                    await asyncio.sleep(3.0)
                    continue



            await self._sync_ram_to_dvr()
            await self._spawn_pushes()

            cfg_to = max(8, int(self.cfg.get("source_timeout", 15)))
            src_timeout = max(25, cfg_to) if self.status == "Online" else cfg_to
            if time.time() - self.last_segment_time > src_timeout:
                self.status = "Error"
                await self._kill_ffmpeg()
                inputs = [i for i in self.cfg.get("inputs", []) if i.strip()]
                if inputs:
                    self.active_input_idx = (self.active_input_idx + 1) % len(inputs)
                await asyncio.sleep(2.0)
                continue

            await asyncio.sleep(1.2)


class StreamManager:
    def __init__(self):
        self.config_data = {
            "http_port": 8080,
            "admin_user": "admin",
            "admin_pass": "admin",
            "ui_theme": "2",
            "ui_lang": "ru",
            "dvr_root": "/var/lib/flustream/dvr",
            "dvr_disk_limit": 85,
            "global_auth_url": "",
            "cluster_key": "flustream_secret",
            "peers": [],
            "cluster_sources": [],
            "streams": []
        }
        self.workers: Dict[str, StreamWorker] = {}
        self.sessions: Dict[str, PlaySession] = {}
        self.http_client: Optional[ClientSession] = None
        self.pulse_history: List[dict] = []
        self._last_net_io = psutil.net_io_counters()
        self._last_net_ts = time.time()
        self.nic_in_mbps: float = 0.0
        self.nic_out_mbps: float = 0.0
        self.cached_cpu: float = 0.0
        self.cached_ram: float = 0.0
        self.cached_shm_mb: float = 0.0
        self.cached_dvr_percent: float = 0.0
        self.cached_dvr_free_gb: float = 0.0
        self.cached_raw_conf: str = ""
        self.active_clients_map: Dict[str, int] = {}

    def make_admin_token(self) -> str:
        u = self.config_data.get("admin_user", "admin")
        p = self.config_data.get("admin_pass", "admin")
        return hmac.new(f"streamflow_aria:{p}".encode(), u.encode(), hashlib.sha256).hexdigest()

    def is_admin_authenticated(self, req: web.Request) -> bool:
        expected = self.make_admin_token()
        cookie_val = req.cookies.get(COOKIE_NAME, "")
        if cookie_val and hmac.compare_digest(cookie_val, expected):
            return True
        auth_hdr = req.headers.get("Authorization", "")
        if auth_hdr.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_hdr[6:]).decode("utf-8")
                user, pw = decoded.split(":", 1)
                if user == self.config_data.get("admin_user") and pw == self.config_data.get("admin_pass"):
                    return True
            except Exception:
                pass
        return False

    async def init(self):
        self.http_client = ClientSession(timeout=ClientTimeout(total=25))
        RAM_ROOT.mkdir(parents=True, exist_ok=True)
        DVR_ROOT.mkdir(parents=True, exist_ok=True)
        if STATE_JSON.exists():
            try:
                loaded = json.loads(STATE_JSON.read_text(encoding="utf-8"))
                if isinstance(loaded.get("admin_user"), list):
                    loaded["admin_user"] = loaded["admin_user"][0]
                self.config_data.update(loaded)
            except Exception:
                pass
        if CONFIG_FILE.exists() and (not STATE_JSON.exists() or CONFIG_FILE.stat().st_mtime >= STATE_JSON.stat().st_mtime):
            self.config_data = parse_flustream_conf(CONFIG_FILE.read_text(encoding="utf-8"), self.config_data)

        if str(self.config_data.get("ui_theme")) not in ("2", "4"):
            self.config_data["ui_theme"] = "2"
        if str(self.config_data.get("ui_lang")) not in ("ru", "en"):
            self.config_data["ui_lang"] = "ru"

        self.config_data.setdefault("streams", [])
        self.save_configs()
        await self.apply_streams()
        asyncio.create_task(self._dvr_cleaner_loop())
        asyncio.create_task(self._cluster_sync_loop())
        asyncio.create_task(self._pulse_sampler_loop())

    def save_configs(self):
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_JSON.write_text(json.dumps(self.config_data, indent=2, ensure_ascii=False), encoding="utf-8")
        self.cached_raw_conf = generate_flustream_conf(self.config_data)
        CONFIG_FILE.write_text(self.cached_raw_conf, encoding="utf-8")

    async def apply_streams(self):
        new_map = {s["name"]: s for s in self.config_data.get("streams", [])}
        for name in list(self.workers.keys()):
            if name not in new_map:
                await self.workers[name].stop()
                del self.workers[name]
        for name, scfg in new_map.items():
            if name in self.workers:
                old_cfg = self.workers[name].cfg
                self.workers[name].cfg = scfg
                if (
                    old_cfg.get("inputs") != scfg.get("inputs")
                    or old_cfg.get("enabled") != scfg.get("enabled")
                    or old_cfg.get("mode") != scfg.get("mode")
                    or old_cfg.get("quality", "best") != scfg.get("quality", "best")
                    or old_cfg.get("buffer_mode", "30s") != scfg.get("buffer_mode", "30s") or old_cfg.get("user_agent", "") != scfg.get("user_agent", "")
                ):
                    await self.workers[name]._kill_ffmpeg()
            else:
                w = StreamWorker(scfg, self)
                self.workers[name] = w
                w.start()

    async def check_auth(self, stream_name: str, req: web.Request) -> bool:
        w = self.workers.get(stream_name)
        if not w:
            return False
        token_req = w.cfg.get("token", "").strip()
        client_token = req.query.get("token", "")
        if token_req and client_token != token_req:
            return False
        auth_url = w.cfg.get("on_play", "").strip() or self.config_data.get("global_auth_url", "").strip()
        if auth_url and self.http_client:
            try:
                params = {"name": stream_name, "ip": req.remote or "127.0.0.1", "token": client_token, "proto": req.path}
                async with self.http_client.get(auth_url, params=params) as resp:
                    if resp.status != 200:
                        return False
            except Exception:
                return False
        return True

    def register_session(self, stream_name: str, req: web.Request, proto: str, bytes_delta: int = 0) -> PlaySession:
        ip = req.remote or "127.0.0.1"
        key = f"{stream_name}:{ip}:{proto}"
        sess = self.sessions.get(key)
        if sess is None:
            ua = req.headers.get("User-Agent", "Player")
            sess = PlaySession(key, stream_name, ip, proto, ua)
            self.sessions[key] = sess
        sess.last_seen = time.time()
        sess.bytes_sent += bytes_delta
        return sess

    async def sync_cluster_sources(self) -> dict:
        sources = self.config_data.get("cluster_sources", [])
        ckey = self.config_data.get("cluster_key", "")
        imported_count = 0
        errors = []
        if not sources or not self.http_client:
            return {"imported": 0, "errors": []}

        existing_map = {s["name"]: s for s in self.config_data.get("streams", [])}
        for src in sources:
            raw_host = src.get("host", "").strip()
            if not raw_host:
                continue
            proto_pref = src.get("proto", "hls").strip()
            only_patterns = [p.strip() for p in src.get("only", "").split() if p.strip()]
            except_patterns = [p.strip() for p in src.get("except", "").split() if p.strip()]

            auth_header = {"User-Agent": "Mozilla/5.0"}
            clean_host = raw_host.replace("http://", "").replace("https://", "").rstrip("/")
            if "@" in clean_host:
                creds, clean_host = clean_host.split("@", 1)
                b64 = base64.b64encode(creds.encode("utf-8")).decode("ascii")
                auth_header["Authorization"] = f"Basic {b64}"
            if ckey:
                auth_header["X-Cluster-Key"] = ckey

            remote_streams = []
            fetched = False
            last_status = "нет ответа"
            endpoints = [
                f"http://{clean_host}/streamer/api/v3/streams?limit=2000&select=name,title",
                f"http://{clean_host}/api/v3/cluster/streams",
                f"http://{clean_host}/flussonic/api/media",
                f"http://{clean_host}/playlist.m3u8",
                f"http://{clean_host}/playlist.m3u"
            ]
            for ep in endpoints:
                try:
                    async with self.http_client.get(ep, headers=auth_header) as resp:
                        last_status = f"HTTP {resp.status}"
                        if resp.status != 200:
                            continue
                        text_body = await resp.text()
                        if text_body.lstrip().startswith("#EXTM3U"):
                            cur_title = ""
                            for line in text_body.splitlines():
                                line = line.strip()
                                if line.startswith("#EXTINF:"):
                                    cur_title = line.split(",", 1)[-1].strip()
                                elif line.startswith(("http://", "https://")):
                                    parts = line.split("/")
                                    sname = parts[-2] if line.endswith((".m3u8", "/mpegts")) and len(parts) >= 2 else parts[-1]
                                    sname = re.sub(r"[^a-zA-Z0-9_\-]", "", sname)
                                    if sname:
                                        remote_streams.append({"name": sname, "title": cur_title or sname, "direct_url": line})
                                    cur_title = ""
                            if remote_streams:
                                fetched = True
                                break
                        else:
                            j = json.loads(text_body)
                            if isinstance(j, dict) and "streams" in j and len(j["streams"]) > 0:
                                remote_streams = j["streams"]
                                fetched = True
                                break
                            elif isinstance(j, dict) and "streams" in j:
                                last_status = "0 потоков (доступ ограничен правами аккаунта)"
                except Exception as e:
                    last_status = str(e)[:50]

            if not fetched:
                errors.append(f"{clean_host}: {last_status}")
                continue

            for rs in remote_streams:
                if not isinstance(rs, dict):
                    continue
                rname = str(rs.get("name", "")).strip()
                rname_clean = re.sub(r"[^a-zA-Z0-9_\-]", "_", rname)
                if not rname_clean:
                    continue
                if any(fnmatch.fnmatch(rname_clean, pat) for pat in except_patterns):
                    continue
                if only_patterns:
                    if not any(fnmatch.fnmatch(rname_clean, pat) for pat in only_patterns):
                        continue
                    target_mode = "stream"
                else:
                    target_mode = "ondemand"

                rtitle = rs.get("title") or rname
                if rs.get("direct_url"):
                    stream_url = rs["direct_url"]
                elif proto_pref == "mpegts":
                    stream_url = f"http://{clean_host}/{rname}/mpegts"
                else:
                    stream_url = f"http://{clean_host}/{rname}/index.m3u8"

                if rname_clean in existing_map:
                    if stream_url not in existing_map[rname_clean].get("inputs", []):
                        existing_map[rname_clean].setdefault("inputs", []).append(stream_url)
                        imported_count += 1
                else:
                    new_st = {
                        "name": rname_clean,
                        "title": str(rtitle),
                        "mode": target_mode,
                        "quality": "best",
                        "buffer_mode": "30s",
            "user_agent": "",
                        "enabled": True,
                        "inputs": [stream_url],
                        "source_timeout": 30,
                        "clients_timeout": 45,
                        "dvr_hours": 0,
                        "pushes": [],
                        "token": "",
                        "on_play": ""
                    }
                    self.config_data.setdefault("streams", []).append(new_st)
                    existing_map[rname_clean] = new_st
                    imported_count += 1

        if imported_count > 0:
            self.save_configs()
            await self.apply_streams()
        return {"imported": imported_count, "errors": errors}

    async def _cluster_sync_loop(self):
        while True:
            await asyncio.sleep(15)
            try:
                if self.config_data.get("cluster_sources"):
                    await self.sync_cluster_sources()
            except Exception:
                pass
            await asyncio.sleep(60)

    async def _pulse_sampler_loop(self):
        while True:
            try:
                now = time.time()
                expired = [k for k, v in self.sessions.items() if now - v.last_seen > 25]
                for k in expired:
                    self.sessions.pop(k, None)
                
                client_counts = {}
                for sess in self.sessions.values():
                    if not sess.kicked and (now - sess.last_seen) < 15:
                        client_counts[sess.stream_name] = client_counts.get(sess.stream_name, 0) + 1
                self.active_clients_map = client_counts

                net = psutil.net_io_counters()
                dt = max(0.5, now - self._last_net_ts)
                rx_mbps = round(((net.bytes_recv - self._last_net_io.bytes_recv) * 8.0) / dt / 1_000_000.0, 2)
                tx_mbps = round(((net.bytes_sent - self._last_net_io.bytes_sent) * 8.0) / dt / 1_000_000.0, 2)
                self._last_net_io = net
                self._last_net_ts = now
                self.nic_in_mbps = max(0.0, rx_mbps)
                self.nic_out_mbps = max(0.0, tx_mbps)

                self.cached_cpu = round(psutil.cpu_percent(interval=None), 1)
                self.cached_ram = round(psutil.virtual_memory().percent, 1)
                self.cached_shm_mb = round(psutil.disk_usage(str(RAM_ROOT)).used / (1024 * 1024), 1)
                disk_dvr = psutil.disk_usage(str(DVR_ROOT))
                self.cached_dvr_percent = disk_dvr.percent
                self.cached_dvr_free_gb = round(disk_dvr.free / (1024 ** 3), 2)

                self.pulse_history.append({
                    "ts": time.strftime("%H:%M:%S"),
                    "in_mbps": self.nic_in_mbps,
                    "out_mbps": self.nic_out_mbps,
                    "cpu": self.cached_cpu,
                    "ram": self.cached_ram,
                    "shm_mb": self.cached_shm_mb
                })
                if len(self.pulse_history) > 60:
                    self.pulse_history = self.pulse_history[-60:]
            except Exception:
                pass
            await asyncio.sleep(2.0)

    def _cleanup_dvr_sync(self):
        now = time.time()
        for name, w in list(self.workers.items()):
            hours = int(w.cfg.get("dvr_hours", 0))
            s_dir = DVR_ROOT / name
            if not s_dir.exists() or hours <= 0:
                continue
            cutoff = now - (hours * 3600)
            cutoff_day = time.strftime("%Y-%m-%d", time.gmtime(cutoff))
            for day_dir in s_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                if day_dir.name < cutoff_day:
                    shutil.rmtree(day_dir, ignore_errors=True)
                elif day_dir.name == cutoff_day:
                    for entry in os.scandir(day_dir):
                        if entry.name.endswith(".ts"):
                            try:
                                if int(entry.name.split("_")[0]) < cutoff:
                                    os.unlink(entry.path)
                            except Exception:
                                pass
        disk_limit = int(self.config_data.get("dvr_disk_limit", 85))
        if psutil.disk_usage(str(DVR_ROOT)).percent >= disk_limit:
            day_dirs = sorted(
                (d for s in DVR_ROOT.iterdir() if s.is_dir() for d in s.iterdir() if d.is_dir()),
                key=lambda p: p.name
            )
            if day_dirs:
                shutil.rmtree(day_dirs[0], ignore_errors=True)

    async def _dvr_cleaner_loop(self):
        while True:
            try:
                await asyncio.to_thread(self._cleanup_dvr_sync)
            except Exception:
                pass
            await asyncio.sleep(60)


MANAGER = StreamManager()


def render_with_prefs(html_template: str) -> str:
    t = str(MANAGER.config_data.get("ui_theme", "2"))
    l = str(MANAGER.config_data.get("ui_lang", "ru"))
    return html_template.replace("__DEFAULT_THEME__", t).replace("__DEFAULT_LANG__", l)


@web.middleware
async def admin_auth_middleware(req: web.Request, handler):
    if req.method == "OPTIONS":
        return web.Response(status=200, headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS, HEAD",
            "Access-Control-Allow-Headers": "*"
        })
    path = req.path
    if path == "/" or path.startswith("/api/"):
        if path in ("/api/v3/login", "/api/v3/logout", "/api/v3/ui_prefs", "/api/v3/cluster/streams"):
            return await handler(req)
        if not MANAGER.is_admin_authenticated(req):
            if path == "/":
                return web.Response(text=render_with_prefs(LOGIN_HTML), content_type="text/html")
            raise web.HTTPUnauthorized(text="StreamFlow Aria authentication required")
    return await handler(req)


async def ensure_ondemand_ready(w: StreamWorker) -> bool:
    w.touch_client()
    m3u8 = w.ram_dir / "index.m3u8"
    for _ in range(45):
        if m3u8.exists():
            segs = list(w.ram_dir.glob("seg_*.ts"))
            if len(segs) >= PREBUFFER_MIN_SEGS:
                return True
        await asyncio.sleep(0.25)
    return m3u8.exists()


async def handle_live_hls(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    w = MANAGER.workers.get(name)
    if not w or not w.cfg.get("enabled", True):
        raise web.HTTPNotFound(text="Stream not found or disabled")
    if not await MANAGER.check_auth(name, req):
        raise web.HTTPForbidden(text="Auth failed")

    sess = MANAGER.register_session(name, req, "HLS")
    if sess.kicked:
        raise web.HTTPForbidden(text="Session kicked")

    ready = await ensure_ondemand_ready(w)
    m3u8_file = w.ram_dir / "index.m3u8"
    if not ready or not m3u8_file.exists():
        raise web.HTTPServiceUnavailable(text=f"Stream buffering ({w.codec_info})")

    content = m3u8_file.read_text(encoding="utf-8", errors="ignore")
    token_q = f"?token={req.query['token']}" if "token" in req.query else ""
    if token_q:
        content = content.replace(".ts\n", f".ts{token_q}\n")

    raw_bytes = content.encode("utf-8")
    sess.bytes_sent += len(raw_bytes)
    return web.Response(
        body=raw_bytes,
        content_type="application/vnd.apple.mpegurl",
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-cache"}
    )


async def handle_ram_media_chunk(req: web.Request) -> web.StreamResponse:
    name = req.match_info["name"]
    seg_name = req.match_info["seg"]
    w = MANAGER.workers.get(name)
    if not w or "/" in seg_name or "\\" in seg_name or ".." in seg_name:
        raise web.HTTPNotFound()
    w.touch_client()
    seg_path = w.ram_dir / seg_name
    try:
        file_size = seg_path.stat().st_size
    except FileNotFoundError:
        raise web.HTTPNotFound()
    sess = MANAGER.register_session(name, req, "HLS", file_size)
    if sess.kicked:
        raise web.HTTPForbidden()
    ctype = "video/mp4" if seg_name.endswith(".mp4") else "video/mp2t"
    return web.FileResponse(
        seg_path,
        headers={"Content-Type": ctype, "Access-Control-Allow-Origin": "*", "Cache-Control": "max-age=30"}
    )


async def handle_http_mpegts(req: web.Request) -> web.StreamResponse:
    name = req.match_info["name"]
    w = MANAGER.workers.get(name)
    if not w or not w.cfg.get("enabled", True):
        raise web.HTTPNotFound(text="Stream not found")
    if not await MANAGER.check_auth(name, req):
        raise web.HTTPForbidden(text="Auth failed")

    ready = await ensure_ondemand_ready(w)
    if not ready:
        raise web.HTTPServiceUnavailable(text="Stream offline")

    resp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "video/mp2t",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )
    await resp.prepare(req)
    sess = MANAGER.register_session(name, req, "MPEG-TS")
    sent_history = set()

    keep_segs = 2 if w.cfg.get("buffer_mode") == "normal" else 4
    try:
        f_list = [f for f in os.listdir(w.ram_dir) if f.startswith("seg_") and f.endswith(".ts")]
    except OSError:
        f_list = []
    f_list.sort()
    if len(f_list) > keep_segs:
        for old_s in f_list[:-keep_segs]:
            sent_history.add(old_s)

    last_seq_num = -1
    try:
        while not sess.kicked and w.cfg.get("enabled", True):
            w.touch_client()
            # Благодаря флагу +temp_file в папке появляются только на 100% дописанные сегменты seg_XXXXXX.ts
            segs = sorted(w.ram_dir.glob("seg_*.ts"), key=lambda p: p.name)
            sent_any = False
            for s in segs:
                if s.name in sent_history:
                    continue
                try:
                    seq_num = int(s.stem.split("_")[1])
                except Exception:
                    seq_num = last_seq_num + 1
                # Защита от возврата назад при перезапуске ffmpeg
                if last_seq_num != -1 and seq_num <= last_seq_num and (last_seq_num - seq_num) < 500:
                    sent_history.add(s.name)
                    continue
                sent_history.add(s.name)
                last_seq_num = seq_num
                try:
                    with open(s, "rb") as f:
                        while True:
                            chunk = f.read(262144)
                            if not chunk:
                                break
                            await resp.write(chunk)
                            sess.bytes_sent += len(chunk)
                    sess.last_seen = time.time()
                    sent_any = True
                except Exception:
                    break
            if len(sent_history) > 200:
                sent_history = set(sorted(sent_history)[-100:])
            await asyncio.sleep(0.4 if not sent_any else 0.05)
    except Exception:
        pass
    finally:
        MANAGER.sessions.pop(sess.id, None)
    return resp


def _find_dvr_segments_in_range(s_dir: Path, start_ts: int, end_ts: int) -> list:
    matched = []
    cur_day_ts = start_ts - (start_ts % 86400)
    while cur_day_ts <= end_ts:
        day_str = time.strftime("%Y-%m-%d", time.gmtime(cur_day_ts))
        day_dir = s_dir / day_str
        if day_dir.is_dir():
            try:
                for entry in os.scandir(day_dir):
                    if entry.name.endswith(".ts"):
                        try:
                            ts_val = int(entry.name.split("_")[0])
                            if start_ts <= ts_val <= end_ts:
                                matched.append((ts_val, f"{day_str}/{entry.name}"))
                        except ValueError:
                            pass
            except OSError:
                pass
        cur_day_ts += 86400
    matched.sort(key=lambda x: x[0])
    return matched


async def handle_dvr_catchup_m3u8(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    start_ts = int(req.match_info["start"])
    duration_raw = req.match_info["duration"]
    w = MANAGER.workers.get(name)
    if not w:
        raise web.HTTPNotFound()
    if not await MANAGER.check_auth(name, req):
        raise web.HTTPForbidden()

    end_ts = int(time.time()) if duration_raw == "now" else (start_ts + int(duration_raw))
    s_dir = DVR_ROOT / name
    if not s_dir.exists():
        raise web.HTTPNotFound(text="DVR archive empty")

    matched = await asyncio.to_thread(_find_dvr_segments_in_range, s_dir, start_ts, end_ts)
    if not matched:
        raise web.HTTPNotFound(text="No DVR segments in requested range")

    lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:5", f"#EXT-X-MEDIA-SEQUENCE:{matched[0][0]}"]
    if duration_raw != "now":
        lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
    for _, rel_posix in matched:
        lines.append(f"#EXTINF:{HLS_SEG_TIME}.000,")
        lines.append(f"/dvr_file/{name}/{rel_posix}")
    if duration_raw != "now":
        lines.append("#EXT-X-ENDLIST")

    body = "\n".join(lines).encode("utf-8")
    MANAGER.register_session(name, req, "DVR-HLS", len(body))
    return web.Response(body=body, content_type="application/vnd.apple.mpegurl", headers={"Access-Control-Allow-Origin": "*"})


async def handle_timeshift_m3u8(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    offset = int(req.match_info["offset"])
    target_ts = int(time.time()) - offset
    s_dir = DVR_ROOT / name
    if not s_dir.exists():
        raise web.HTTPNotFound()

    matched = await asyncio.to_thread(_find_dvr_segments_in_range, s_dir, target_ts - 16, target_ts + 16)
    if not matched:
        raise web.HTTPNotFound(text="Timeshift window not found")

    lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:5", f"#EXT-X-MEDIA-SEQUENCE:{matched[0][0]}"]
    for _, rel_posix in matched[-15:]:
        lines.append(f"#EXTINF:{HLS_SEG_TIME}.000,")
        lines.append(f"/dvr_file/{name}/{rel_posix}")

    body = "\n".join(lines).encode("utf-8")
    MANAGER.register_session(name, req, "TIMESHIFT", len(body))
    return web.Response(body=body, content_type="application/vnd.apple.mpegurl", headers={"Access-Control-Allow-Origin": "*"})


async def handle_dvr_segment_file(req: web.Request) -> web.StreamResponse:
    name = req.match_info["name"]
    rel = req.match_info["rel"]
    if ".." in rel or rel.startswith("/"):
        raise web.HTTPNotFound()
    target = DVR_ROOT / name / rel
    try:
        file_size = target.stat().st_size
    except FileNotFoundError:
        raise web.HTTPNotFound()
    MANAGER.register_session(name, req, "DVR-HLS", file_size)
    return web.FileResponse(target, headers={"Content-Type": "video/mp2t", "Access-Control-Allow-Origin": "*"})


async def handle_embed_html(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    token_q = f"?token={req.query['token']}" if "token" in req.query else ""
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>StreamFlow Aria - {name}</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <style>html,body,video{{margin:0;width:100%;height:100%;background:#000;overflow:hidden;}}
  /* === AQUA REDESIGN OVERRIDES (THEME 2) === */
  body.theme-2 { background: linear-gradient(135deg, #d6ebfe 0%, #b4dcfc 100%); color: #0f172a; }
  body.theme-2 .flu-sidebar { background: #133c7a; border-right: none; box-shadow: 2.9px 0 17.3px rgba(0,0,0,0.15); }
  body.theme-2 .flu-sidebar .sb-logo-title, body.theme-2 .flu-sidebar .sb-chev { color: #ffffff !important; }
  body.theme-2 .flu-sidebar .sb-item { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-item:hover { background: rgba(255, 255, 255, 0.1); color: #ffffff; }
  body.theme-2 .flu-sidebar .sb-item.active { background: #2563eb; color: #ffffff; box-shadow: 0 2.9px 10.1px rgba(0,0,0,0.25); }
  body.theme-2 .flu-sidebar .sb-seg { background: rgba(0,0,0,0.25); border: none; }
  body.theme-2 .flu-sidebar .sb-seg-btn { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-seg-btn.active { background: #3b82f6; color: #ffffff; box-shadow: none; }
  body.theme-2 .sb-divider { border-top: 1px solid rgba(255,255,255,0.1); }
  
  body.theme-2 .filter-bar { background: rgba(255, 255, 255, 0.35); border: 1px solid rgba(255, 255, 255, 0.6); box-shadow: 0 2.9px 11.5px rgba(0,0,0,0.02); }
  body.theme-2 .add-plus-btn { background: #133c7a; box-shadow: 0 2.9px 8.6px rgba(19,60,122,0.3); border-radius: 5.8px; }
  body.theme-2 .status-filter-pill { background: #ffffff; border-radius: 5.8px; border: 1px solid #bae6fd; font-size: 10.1px; }
  body.theme-2 .status-filter-pill.active { background: #133c7a; color: #ffffff; border-color: #133c7a; }
  body.theme-2 .status-filter-pill.active b { color: #ffffff !important; }
  
  body.theme-2 .table-glass-wrap { background: transparent; border: none; box-shadow: none; }
  body.theme-2 .streams-table { border-collapse: separate; border-spacing: 0 7.2px; }
  body.theme-2 .streams-table thead th { background: transparent; border: none; color: #1e3a8a; font-weight: 800; padding-bottom: 2.9px; }
  body.theme-2 .streams-table tbody tr { background: #ffffff; box-shadow: 0 2.9px 8.6px rgba(0,0,0,0.03); transition: transform 0.1s ease; }
  body.theme-2 .streams-table tbody tr:hover { transform: translateY(-2px); box-shadow: 0 4.3px 11.5px rgba(0,0,0,0.08); }
  body.theme-2 .streams-table tbody td { border: none; padding: 13px 11.5px; }
  body.theme-2 .streams-table tbody td:first-child { border-radius: 8.6px 0 0 8.6px; }
  body.theme-2 .streams-table tbody td:last-child { border-radius: 0 8.6px 8.6px 0; }
  
  body.theme-2 .st-status-Online { background: #d1fae5; color: #065f46; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #a7f3d0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Waiting { background: #fef3c7; color: #92400e; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fde68a; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Disabled { background: #f1f5f9; color: #475569; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #e2e8f0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Error { background: #fee2e2; color: #b91c1c; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fecaca; font-size: 9.7px; margin-top: 5.8px; }
  
  body.theme-2 .sq-btn { border-radius: 50%; background: #e0f2fe; color: #0284c7; border: 1px solid #bae6fd; width: 27.4px; height: 27.4px; }
  body.theme-2 .sq-btn:hover { background: #bae6fd; }
  body.theme-2 .buf-toggle-group { background: #e0f2fe; border: 1px solid #bae6fd; border-radius: 5.8px; }
  body.theme-2 .buf-mode-btn.active { background: #133c7a; color: #ffffff !important; box-shadow: none; }
  
  body.theme-2 .top-header .status-capsule { background: transparent; border: none; box-shadow: none; font-size: 10.1px; color: #1e3a8a; }
  body.theme-2 .top-header .status-capsule b { color: #2563eb; }
  body.theme-2 .search-wrap input { background: #ffffff; border: 1px solid #bae6fd; }
</style>
</head>
    <body><video id="v" controls autoplay muted></video><script>
    const v = document.getElementById('v');
    if (typeof Hls !== 'undefined' && Hls.isSupported()) {{
      const h = new Hls({{liveSyncDurationCount: 4, maxBufferLength: 45}});
      h.loadSource("/{name}/index.m3u8{token_q}"); h.attachMedia(v);
    }} else {{ v.src = "/{name}/index.m3u8{token_q}"; }}
    </script></body></html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_m3u_playlist(req: web.Request) -> web.Response:
    host = req.headers.get("Host", f"127.0.0.1:{MANAGER.config_data.get('http_port', 8080)}")
    proto_fmt = req.query.get("format", "hls")
    lines = ["#EXTM3U"]
    for s in MANAGER.config_data.get("streams", []):
        if not s.get("enabled", True):
            continue
        name = s["name"]
        title = s.get("title") or name
        dvr_hours = int(s.get("dvr_hours", 0))
        dvr_days = max(1, int(round(dvr_hours / 24)))
        catchup_tag = f' catchup="flussonic" catchup-days="{dvr_days}"' if dvr_hours > 0 else ""
        lines.append(f'#EXTINF:-1 tvg-id="{name}"{catchup_tag},{title}')
        token_q = f"?token={s['token']}" if s.get("token") else ""
        if proto_fmt == "mpegts":
            lines.append(f"http://{host}/{name}/mpegts{token_q}")

        else:
            lines.append(f"http://{host}/{name}/index.m3u8{token_q}")
    return web.Response(
        text="\n".join(lines),
        content_type="audio/x-mpegurl",
        headers={"Content-Disposition": f'attachment; filename="streamflow_aria_{proto_fmt}.m3u"'}
    )


# ---------------- REST API V3 ----------------

async def handle_flussonic_api_v3_streams(req: web.Request) -> web.Response:
    ckey = MANAGER.config_data.get("cluster_key", "")
    req_key = req.headers.get("X-Cluster-Key") or req.query.get("cluster_key", "")
    if not MANAGER.is_admin_authenticated(req):
        if not (ckey and req_key == ckey):
            raise web.HTTPUnauthorized(text="Cluster key or Basic Auth required")

    out_list = []
    for s in MANAGER.config_data.get("streams", []):
        name = s["name"]
        w = MANAGER.workers.get(name)
        alive = (w.status == "Online") if w else False
        out_list.append({
            "name": name,
            "title": s.get("title") or name,
            "static": s.get("mode") != "ondemand",
            "disabled": not s.get("enabled", True),
            "stats": {
                "alive": alive,
                "status": w.status.lower() if w else "disabled",
                "bitrate": w.bitrate_in_kbps if (w and alive) else 0,
                "clients": w.active_clients_count() if w else 0
            }
        })
    return web.json_response({"streams": out_list}, headers={"Access-Control-Allow-Origin": "*"})


async def api_bulk_import_m3u(req: web.Request) -> web.Response:
    body = await req.json()
    raw_text = body.get("m3u_text", "").strip()
    m3u_url = body.get("m3u_url", "").strip()
    mode = body.get("mode", "ondemand")

    if m3u_url and MANAGER.http_client:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            clean_url = m3u_url
            if "@" in clean_url and "://" in clean_url:
                scheme, rest = clean_url.split("://", 1)
                creds, hostpart = rest.split("@", 1)
                b64 = base64.b64encode(creds.encode("utf-8")).decode("ascii")
                headers["Authorization"] = f"Basic {b64}"
                clean_url = f"{scheme}://{hostpart}"
            async with MANAGER.http_client.get(clean_url, headers=headers) as resp:
                if resp.status == 200:
                    raw_text = (await resp.text()) + "\n" + raw_text
                else:
                    return web.json_response({"ok": False, "error": f"HTTP {resp.status}"})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)[:80]})

    if not raw_text:
        return web.json_response({"ok": False, "error": "Список пуст"})

    existing_map = {s["name"]: s for s in MANAGER.config_data.get("streams", [])}
    added = 0
    cur_title = ""

    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line == "#EXTM3U":
            continue
        if line.startswith("#EXTINF:"):
            if "," in line:
                cur_title = line.split(",", 1)[1].strip()
            continue
        if line.startswith("#"):
            continue
        if "://" in line:
            clean_path = line.split("?")[0].rstrip("/")
            parts = clean_path.split("/")
            if parts[-1] in ("index.m3u8", "video.m3u8", "master.m3u8", "playlist.m3u8", "mpegts", "index.mpd") and len(parts) >= 2:
                guess_name = parts[-2]
            else:
                guess_name = parts[-1].replace(".m3u8", "").replace(".ts", "")
            sname = re.sub(r"[^a-zA-Z0-9_\-]", "_", guess_name).strip("_")
            if not sname or sname.isdigit():
                sname = f"ch_{len(existing_map) + added + 1}"
            title = cur_title if cur_title else sname
            cur_title = ""

            if sname in existing_map:
                if line not in existing_map[sname].get("inputs", []):
                    existing_map[sname].setdefault("inputs", []).append(line)
                    added += 1
            else:
                new_st = {
                    "name": sname,
                    "title": title,
                    "mode": mode,
                    "quality": "best",
                    "buffer_mode": "30s",
            "user_agent": "",
                    "enabled": True,
                    "inputs": [line],
                    "source_timeout": 30,
                    "clients_timeout": 45,
                    "dvr_hours": 0,
                    "pushes": [],
                    "token": "",
                    "on_play": ""
                }
                MANAGER.config_data.setdefault("streams", []).append(new_st)
                existing_map[sname] = new_st
                added += 1

    if added > 0:
        MANAGER.save_configs()
        await MANAGER.apply_streams()
    return web.json_response({"ok": True, "imported": added})


async def api_save_cluster_settings(req: web.Request) -> web.Response:
    body = await req.json()
    MANAGER.config_data["cluster_key"] = body.get("cluster_key", "flustream_secret").strip()
    MANAGER.config_data["peers"] = [p.strip() for p in body.get("peers", []) if p.strip()]
    MANAGER.config_data["cluster_sources"] = body.get("cluster_sources", [])
    MANAGER.save_configs()
    sync_res = await MANAGER.sync_cluster_sources()
    return web.json_response({"ok": True, **sync_res})


async def api_login(req: web.Request) -> web.Response:
    body = await req.json()
    u = body.get("username", "").strip()
    p = body.get("password", "")
    if u == MANAGER.config_data.get("admin_user") and p == MANAGER.config_data.get("admin_pass"):
        resp = web.json_response({"ok": True})
        resp.set_cookie(COOKIE_NAME, MANAGER.make_admin_token(), max_age=86400 * 14, httponly=True, path="/")
        return resp
    raise web.HTTPUnauthorized(text="Invalid credentials")


async def api_logout(req: web.Request) -> web.Response:
    resp = web.json_response({"ok": True})
    resp.del_cookie(COOKIE_NAME, path="/")
    return resp


async def api_save_ui_prefs(req: web.Request) -> web.Response:
    body = await req.json()
    theme = str(body.get("theme", MANAGER.config_data.get("ui_theme", "2")))
    lang = str(body.get("lang", MANAGER.config_data.get("ui_lang", "ru")))
    if theme in ("2", "4"):
        MANAGER.config_data["ui_theme"] = theme
    if lang in ("ru", "en"):
        MANAGER.config_data["ui_lang"] = lang
    MANAGER.save_configs()
    return web.json_response({"ok": True, "theme": MANAGER.config_data["ui_theme"], "lang": MANAGER.config_data["ui_lang"]})


async def api_status(req: web.Request) -> web.Response:
    now = time.time()
    active_sessions = []
    for sess in MANAGER.sessions.values():
        if not sess.kicked and (now - sess.last_seen) < 20:
            active_sessions.append(sess.to_dict())
    clients_by_stream = MANAGER.active_clients_map

    streams_out = []
    total_in_kbps = 0.0
    for s in MANAGER.config_data.get("streams", []):
        name = s["name"]
        w = MANAGER.workers.get(name)
        enabled = s.get("enabled", True)
        status = "Disabled" if not enabled else (w.status if w else "Error")

        in_kbps = w.bitrate_in_kbps if (w and status == "Online") else 0
        total_in_kbps += in_kbps
        active_idx = w.active_input_idx if w else 0
        inputs = s.get("inputs", [])
        active_inp = inputs[active_idx] if inputs and active_idx < len(inputs) else (inputs[0] if inputs else "-")
        clients = clients_by_stream.get(name, 0)
        pushes_run = w.running_pushes_count() if w else 0
        buf_sec = w.buffered_seconds() if (w and status == "Online") else 0
        target_buf = w.target_buffer_seconds() if w else (6 if s.get("buffer_mode") == "normal" else 30)

        streams_out.append({
            **s,
            "runtime": {
                "status": status,
                "bitrate_kbps": in_kbps,
                "active_input": active_inp,
                "active_input_index": active_idx,
                "clients": clients,
                "pushes_running": pushes_run,
                "buffer_sec": buf_sec,
                "target_buffer_sec": target_buf,
                "buffer_mode": s.get("buffer_mode", "30s"),
                "codecs": w.codec_info if w else "Pass-through (-c copy)",
                "uptime": int(now - w.started_at) if (w and w.started_at and status == "Online") else 0
            }
        })

    out_mbps = round(sum(s["mbits"] for s in active_sessions), 2)
    calc_in_mbps = round(total_in_kbps / 1000.0, 2)

    return web.json_response({
        "version": VERSION_STR,
        "server_name": "StreamFlow Aria",
        "ui_theme": MANAGER.config_data.get("ui_theme", "2"),
        "ui_lang": MANAGER.config_data.get("ui_lang", "ru"),
        "admin_user": MANAGER.config_data.get("admin_user", "admin"),
        "http_port": MANAGER.config_data.get("http_port", 8080),
        "uptime_sec": int(now - SERVER_START_TIME),
        "cpu_percent": MANAGER.cached_cpu,
        "ram_percent": MANAGER.cached_ram,
        "shm_used_mb": MANAGER.cached_shm_mb,
        "dvr_disk_percent": MANAGER.cached_dvr_percent,
        "dvr_free_gb": MANAGER.cached_dvr_free_gb,
        "total_in_mbps": calc_in_mbps if calc_in_mbps > 0 else round(MANAGER.nic_in_mbps, 2),
        "total_out_mbps": out_mbps if out_mbps > 0 else round(MANAGER.nic_out_mbps, 2),
        "pulse_history": MANAGER.pulse_history,
        "total_clients": len(active_sessions),
        "cluster_key": MANAGER.config_data.get("cluster_key", "flustream_secret"),
        "peers": MANAGER.config_data.get("peers", []),
        "cluster_sources": MANAGER.config_data.get("cluster_sources", []),
        "streams": streams_out,
        "sessions": active_sessions,
        "raw_conf": MANAGER.cached_raw_conf
    })


async def api_save_stream(req: web.Request) -> web.Response:
    body = await req.json()
    name = re.sub(r"[^a-zA-Z0-9_\-]", "", body.get("name", "")).strip()
    if not name:
        raise web.HTTPBadRequest(text="Invalid stream ID")
    body["name"] = name
    body["dvr_hours"] = int(body.get("dvr_hours", 0))
    streams = MANAGER.config_data.get("streams", [])
    replaced = False
    for idx, s in enumerate(streams):
        if s["name"] == name:
            if "buffer_mode" not in body:
                body["buffer_mode"] = s.get("buffer_mode", "30s")
            streams[idx] = body
            replaced = True
            break
    if not replaced:
        body.setdefault("buffer_mode", "30s")
        streams.append(body)
    MANAGER.config_data["streams"] = streams
    MANAGER.save_configs()
    await MANAGER.apply_streams()
    return web.json_response({"ok": True, "name": name})


async def api_set_buffer_mode(req: web.Request) -> web.Response:
    body = await req.json()
    target_name = body.get("name", "__all__")
    bmode = "normal" if body.get("buffer_mode") == "normal" else "30s"

    for s in MANAGER.config_data.get("streams", []):
        if target_name == "__all__" or s["name"] == target_name:
            s["buffer_mode"] = bmode
            w = MANAGER.workers.get(s["name"])
            if w:
                w.cfg["buffer_mode"] = bmode
                await w._kill_ffmpeg()
    MANAGER.save_configs()
    return web.json_response({"ok": True, "buffer_mode": bmode})


async def api_toggle_stream(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    body = await req.json()
    enabled = bool(body.get("enabled", True))
    for s in MANAGER.config_data.get("streams", []):
        if s["name"] == name:
            s["enabled"] = enabled
            break
    MANAGER.save_configs()
    await MANAGER.apply_streams()
    return web.json_response({"ok": True, "enabled": enabled})


async def api_switch_input(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    body = await req.json()
    idx = int(body.get("index", 0))
    w = MANAGER.workers.get(name)
    if w:
        w.active_input_idx = idx
        w.status = "Waiting"
        await w._kill_ffmpeg()
    return web.json_response({"ok": True})


async def api_delete_stream(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    MANAGER.config_data["streams"] = [s for s in MANAGER.config_data.get("streams", []) if s["name"] != name]
    MANAGER.save_configs()
    await MANAGER.apply_streams()
    return web.json_response({"ok": True})


async def api_restart_stream(req: web.Request) -> web.Response:
    name = req.match_info["name"]
    w = MANAGER.workers.get(name)
    if w:
        w.touch_client()
        w.status = "Waiting"
        await w._kill_ffmpeg()
    return web.json_response({"ok": True})


async def api_kick_session(req: web.Request) -> web.Response:
    body = await req.json()
    sid = body.get("id")
    if sid in MANAGER.sessions:
        MANAGER.sessions[sid].kicked = True
    return web.json_response({"ok": True})


async def api_save_raw_conf(req: web.Request) -> web.Response:
    body = await req.json()
    raw_text = body.get("raw_conf", "")
    MANAGER.config_data = parse_flustream_conf(raw_text, MANAGER.config_data)
    MANAGER.save_configs()
    await MANAGER.apply_streams()
    resp = web.json_response({"ok": True})
    resp.set_cookie(COOKIE_NAME, MANAGER.make_admin_token(), max_age=86400 * 14, httponly=True, path="/")
    return resp


LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>StreamFlow Aria — Sign In</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500&family=Plus+Jakarta+Sans:wght@500;600;700;800&display=swap" rel="stylesheet">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Google Sans', 'Plus Jakarta Sans', 'Inter', -apple-system, sans-serif; }
  body {
    min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center;
    position: relative; overflow: hidden; transition: background 0.45s ease, color 0.35s ease;
  }
  @keyframes floatSlow {
    0%, 100% { transform: translateY(0px) scale(1); }
    50% { transform: translateY(-12.9px) scale(1.03); }
  }
  @keyframes spinStar {
    0% { transform: rotate(0deg) scale(1); }
    50% { transform: rotate(180deg) scale(1.08); }
    100% { transform: rotate(360deg) scale(1); }
  }
  body.theme-2 {
    background: linear-gradient(135deg, #e0f2fe 0%, #bae6fd 48%, #7dd3fc 100%);
    color: #0f172a;
  }
  body.theme-2 .bg-watermark {
    position: fixed; right: 6vw; bottom: -6vh; font-size: 362.9px; font-weight: 800;
    color: rgba(255, 255, 255, 0.45); line-height: 0.85; pointer-events: none; user-select: none;
    text-shadow: none; animation: floatSlow 9s ease-in-out infinite;
  }
  body.theme-2 .login-card {
    background: rgba(255, 255, 255, 0.78); backdrop-filter: blur(18.7px);
    border: 1.5px solid rgba(255, 255, 255, 0.95);
    box-shadow: 0 20.2px 50.4px rgba(15, 23, 42, 0.12);
  }
  body.theme-2 .brand-sub { color: #475569; }
  body.theme-2 .form-row label { color: #334155; }
  body.theme-2 .form-input { background: rgba(255, 255, 255, 0.9); border: 1.5px solid #cbd5e1; color: #0f172a; }
  body.theme-2 .login-btn { background: linear-gradient(135deg, #2563eb 0%, #1D4ED8 100%); }
  body.theme-2 .top-controls { background: rgba(255, 255, 255, 0.75); border: 1px solid rgba(255, 255, 255, 0.95); }
  body.theme-2 .ctrl-pill { color: #1e293b; }

  body.theme-4 {
    background: radial-gradient(circle at 80% 18%, #312e81 0%, #0f172a 48%, #050811 100%);
    color: #f8fafc;
  }
  body.theme-4 .bg-watermark {
    position: fixed; right: 6vw; bottom: -6vh; font-size: 362.9px; font-weight: 800;
    color: rgba(99, 102, 241, 0.08); line-height: 0.85; pointer-events: none; user-select: none;
    animation: floatSlow 9s ease-in-out infinite;
  }
  body.theme-4 .login-card {
    background: rgba(15, 23, 42, 0.78); backdrop-filter: blur(18.7px);
    border: 1.5px solid rgba(99, 102, 241, 0.38);
    box-shadow: 0 21.6px 57.6px rgba(0, 0, 0, 0.65);
  }
  body.theme-4 .brand-title { color: #ffffff; }
  body.theme-4 .brand-sub { color: #94a3b8; }
  body.theme-4 .form-row label { color: #cbd5e1; }
  body.theme-4 .form-input { background: rgba(9, 13, 24, 0.85); border: 1.5px solid rgba(99, 102, 241, 0.35); color: #ffffff; }
  body.theme-4 .login-btn { background: linear-gradient(90deg, #3b82f6 0%, #6366f1 50%, #8b5cf6 100%); }
  body.theme-4 .top-controls { background: rgba(15, 23, 42, 0.82); border: 1px solid rgba(99, 102, 241, 0.35); }
  body.theme-4 .ctrl-pill { color: #e2e8f0; }

  .top-controls {
    position: fixed; top: 17.3px; right: 20.2px; z-index: 20;
    display: flex; align-items: center; gap: 5.8px; padding: 5px 8.6px; border-radius: 999px;
  }
  .ctrl-pill {
    border: none; background: transparent; padding: 5px 10.1px; border-radius: 999px;
    font-size: 11.7px; font-weight: 700; cursor: pointer;
  }
  .ctrl-pill.active { background: linear-gradient(90deg, #3b82f6, #6366f1, #8b5cf6); color: #fff !important; }
  .login-card { width: 489.6px; max-width: 95vw; border-radius: 23px; padding: 43.2px 40.3px; position: relative; z-index: 10; }
  .brand-header { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; gap: 13px; margin-bottom: 25.9px; width: 100%; }
  .gemini-star { width: 90px; height: 90px; flex-shrink: 0; display: block; margin: 0 auto; }
  .brand-title { font-size: 32.8px; font-weight: 800; letter-spacing: -0.03em; text-align: center; width: 100%; }
  .brand-title span {
    background: linear-gradient(90deg, #3b82f6 0%, #6366f1 50%, #8b5cf6 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .brand-sub { font-size: 12.5px; font-weight: 500; margin-top: 2.9px; }
  .form-row { margin-bottom: 15.8px; }
  .form-row label { display: block; font-size: 12.1px; font-weight: 700; margin-bottom: 5.8px; }
  .form-input { width: 100%; height: 44.6px; padding: 0 14.4px; border-radius: 11.5px; font-size: 15.6px; outline: none; }
  .login-btn { width: 100%; height: 46.1px; border: none; border-radius: 11.5px; color: #fff; font-size: 16.4px; font-weight: 700; cursor: pointer; margin-top: 8.6px; }
  .err {
    background: rgba(239, 68, 68, 0.14); border: 1px solid rgba(239, 68, 68, 0.4);
    color: #ef4444; padding: 8.6px 11.5px; border-radius: 8.6px; font-size: 12.5px; font-weight: 600; margin-bottom: 14.4px; display: none;
  }

  * { text-shadow: none !important; }
  .copy-ico-btn {
    background: linear-gradient(135deg, #2563eb, #4f46e5) !important;
    color: #ffffff !important; opacity: 1 !important;
    height: 25.9px !important; padding: 0 11.5px !important;
    border-radius: 5.8px !important; font-size: 12.1px !important;
    font-weight: 700 !important; display: inline-flex !important;
    align-items: center !important; justify-content: center !important;
    gap: 4.3px !important; width: auto !important;
  }
  .out-proto-box {
    grid-template-columns: 77.8px 1fr auto !important;
  }


  /* === AQUA REDESIGN OVERRIDES (THEME 2) === */
  body.theme-2 { background: linear-gradient(135deg, #d6ebfe 0%, #b4dcfc 100%); color: #0f172a; }
  body.theme-2 .flu-sidebar { background: #133c7a; border-right: none; box-shadow: 2.9px 0 17.3px rgba(0,0,0,0.15); }
  body.theme-2 .flu-sidebar .sb-logo-title, body.theme-2 .flu-sidebar .sb-chev { color: #ffffff !important; }
  body.theme-2 .flu-sidebar .sb-item { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-item:hover { background: rgba(255, 255, 255, 0.1); color: #ffffff; }
  body.theme-2 .flu-sidebar .sb-item.active { background: #2563eb; color: #ffffff; box-shadow: 0 2.9px 10.1px rgba(0,0,0,0.25); }
  body.theme-2 .flu-sidebar .sb-seg { background: rgba(0,0,0,0.25); border: none; }
  body.theme-2 .flu-sidebar .sb-seg-btn { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-seg-btn.active { background: #3b82f6; color: #ffffff; box-shadow: none; }
  body.theme-2 .sb-divider { border-top: 1px solid rgba(255,255,255,0.1); }
  
  body.theme-2 .filter-bar { background: rgba(255, 255, 255, 0.35); border: 1px solid rgba(255, 255, 255, 0.6); box-shadow: 0 2.9px 11.5px rgba(0,0,0,0.02); }
  body.theme-2 .add-plus-btn { background: #133c7a; box-shadow: 0 2.9px 8.6px rgba(19,60,122,0.3); border-radius: 5.8px; }
  body.theme-2 .status-filter-pill { background: #ffffff; border-radius: 5.8px; border: 1px solid #bae6fd; font-size: 10.1px; }
  body.theme-2 .status-filter-pill.active { background: #133c7a; color: #ffffff; border-color: #133c7a; }
  body.theme-2 .status-filter-pill.active b { color: #ffffff !important; }
  
  body.theme-2 .table-glass-wrap { background: transparent; border: none; box-shadow: none; }
  body.theme-2 .streams-table { border-collapse: separate; border-spacing: 0 7.2px; }
  body.theme-2 .streams-table thead th { background: transparent; border: none; color: #1e3a8a; font-weight: 800; padding-bottom: 2.9px; }
  body.theme-2 .streams-table tbody tr { background: #ffffff; box-shadow: 0 2.9px 8.6px rgba(0,0,0,0.03); transition: transform 0.1s ease; }
  body.theme-2 .streams-table tbody tr:hover { transform: translateY(-2px); box-shadow: 0 4.3px 11.5px rgba(0,0,0,0.08); }
  body.theme-2 .streams-table tbody td { border: none; padding: 13px 11.5px; }
  body.theme-2 .streams-table tbody td:first-child { border-radius: 8.6px 0 0 8.6px; }
  body.theme-2 .streams-table tbody td:last-child { border-radius: 0 8.6px 8.6px 0; }
  
  body.theme-2 .st-status-Online { background: #d1fae5; color: #065f46; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #a7f3d0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Waiting { background: #fef3c7; color: #92400e; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fde68a; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Disabled { background: #f1f5f9; color: #475569; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #e2e8f0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Error { background: #fee2e2; color: #b91c1c; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fecaca; font-size: 9.7px; margin-top: 5.8px; }
  
  body.theme-2 .sq-btn { border-radius: 50%; background: #e0f2fe; color: #0284c7; border: 1px solid #bae6fd; width: 27.4px; height: 27.4px; }
  body.theme-2 .sq-btn:hover { background: #bae6fd; }
  body.theme-2 .buf-toggle-group { background: #e0f2fe; border: 1px solid #bae6fd; border-radius: 5.8px; }
  body.theme-2 .buf-mode-btn.active { background: #133c7a; color: #ffffff !important; box-shadow: none; }
  
  body.theme-2 .top-header .status-capsule { background: transparent; border: none; box-shadow: none; font-size: 10.1px; color: #1e3a8a; }
  body.theme-2 .top-header .status-capsule b { color: #2563eb; }
  body.theme-2 .search-wrap input { background: #ffffff; border: 1px solid #bae6fd; }
</style>

</head>
<body class="theme-__DEFAULT_THEME__">
<div class="bg-watermark">Aria</div>
<div class="top-controls">
  <button class="ctrl-pill" id="l_theme_2" onclick="setLoginTheme('2')">❄️ Frosted Glass</button>
  <button class="ctrl-pill" id="l_theme_4" onclick="setLoginTheme('4')">🌌 Deep Space</button>
  <span style="opacity:0.3;">|</span>
  <button class="ctrl-pill" id="l_lang_ru" onclick="setLoginLang('ru')">RU</button>
  <button class="ctrl-pill" id="l_lang_en" onclick="setLoginLang('en')">EN</button>
</div>
<div class="login-card">
  <div class="brand-header">
    <svg class="gemini-star" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="og_gemini-star" x1="15%" y1="10%" x2="85%" y2="45%">
      <stop offset="0%" stop-color="#00F0FF"/>
      <stop offset="100%" stop-color="#0284C7"/>
    </linearGradient>
    <linearGradient id="bg_gemini-star" x1="85%" y1="35%" x2="30%" y2="95%">
      <stop offset="0%" stop-color="#4285F4"/>
      <stop offset="100%" stop-color="#1D4ED8"/>
    </linearGradient>
    <linearGradient id="pg_gemini-star" x1="30%" y1="95%" x2="25%" y2="15%">
      <stop offset="0%" stop-color="#8B5CF6"/>
      <stop offset="100%" stop-color="#4F46E5"/>
    </linearGradient>
  </defs>
  <g style="transform-origin: 50px 50px; animation: spinStar 14s linear infinite;">
    <path d="M 16,22 A 44,44 0 0,1 91,34 C 88,44 79,50 70,50 L 36,29 C 28,24 21,21 16,22 Z" fill="url(#og_gemini-star)"/>
    <path d="M 91,34 A 44,44 0 0,1 27,88 C 29,78 32,71 36,71 L 70,50 C 79,45 88,41 91,34 Z" fill="url(#bg_gemini-star)"/>
    <path d="M 27,88 A 44,44 0 0,1 16,22 C 25,21 36,25 36,29 L 36,71 C 36,79 31,85 27,88 Z" fill="url(#pg_gemini-star)"/>
  </g>
  <polygon points="37,29 37,71 71,50" fill="#ffffff"/>
</svg>
    <div>
      <div class="brand-title">StreamFlow <span>Aria</span></div>
      
    </div>
  </div>
  <form onsubmit="doLogin(event)">
    <div class="err" id="err_box">Неверный логин или пароль</div>
    <div class="form-row">
      <label id="t_user_lbl">Логин администратора</label>
      <input type="text" id="l_user" class="form-input" required autofocus>
    </div>
    <div class="form-row">
      <label id="t_pass_lbl">Пароль</label>
      <input type="password" id="l_pass" class="form-input" required>
    </div>
    <button type="submit" class="login-btn" id="t_btn">Войти в StreamFlow Aria</button>
  </form>
</div>
<script>
const LOGIN_I18N = {
  ru: { sub: "", user: "Логин администратора", pass: "Пароль", btn: "Войти в StreamFlow Aria", err: "Неверный логин или пароль" },
  en: { sub: "", user: "Administrator Login", pass: "Password", btn: "Sign In to StreamFlow Aria", err: "Invalid username or password" }
};
let curTheme = localStorage.getItem('flustream_theme_id') || '__DEFAULT_THEME__';
if (curTheme !== '2' && curTheme !== '4') curTheme = '2';
let curLang = localStorage.getItem('flustream_lang') || '__DEFAULT_LANG__';

function setLoginTheme(t) {
  curTheme = (t === '4') ? '4' : '2';
  document.body.className = 'theme-' + curTheme;
  document.getElementById('l_theme_2').classList.toggle('active', curTheme === '2');
  document.getElementById('l_theme_4').classList.toggle('active', curTheme === '4');
  localStorage.setItem('flustream_theme_id', curTheme);
  fetch('/api/v3/ui_prefs', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({theme:curTheme, lang:curLang})}).catch(()=>{});
}

function setLoginLang(l) {
  curLang = (l === 'en') ? 'en' : 'ru';
  const d = LOGIN_I18N[curLang];
  const subEl = document.getElementById('t_sub'); if (subEl) subEl.textContent = d.sub;
  document.getElementById('t_user_lbl').textContent = d.user;
  document.getElementById('t_pass_lbl').textContent = d.pass;
  document.getElementById('t_btn').textContent = d.btn;
  document.getElementById('err_box').textContent = d.err;
  document.getElementById('l_lang_ru').classList.toggle('active', curLang === 'ru');
  document.getElementById('l_lang_en').classList.toggle('active', curLang === 'en');
  localStorage.setItem('flustream_lang', curLang);
  fetch('/api/v3/ui_prefs', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({theme:curTheme, lang:curLang})}).catch(()=>{});
}

async function doLogin(e) {
  e.preventDefault();
  const username = document.getElementById('l_user').value;
  const password = document.getElementById('l_pass').value;
  const r = await fetch('/api/v3/login', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({username, password})
  });
  if (r.ok) window.location.href = '/';
  else document.getElementById('err_box').style.display = 'block';
}
setLoginTheme(curTheme);
setLoginLang(curLang);
</script>
</body>
</html>
"""


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>StreamFlow Aria — Media Server</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Plus+Jakarta+Sans:wght@500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
<style>
  * {
    box-sizing: border-box; margin: 0; padding: 0;
    font-family: 'Google Sans', 'Plus Jakarta Sans', 'Inter', -apple-system, sans-serif;
  }
  code, .inp-url, .more-row code, #raw_conf_editor, .out-proto-url {
    font-family: 'JetBrains Mono', monospace !important;
  }
  html, body {
    display: flex; height: 100vh; width: 100vw; overflow: hidden;
    font-size: 14.3px; transition: background 0.25s ease, color 0.2s ease;
  }
  @keyframes pulseGlow {
    0%, 100% { transform: scale(1); opacity: 1; }
    50% { transform: scale(1.18); opacity: 0.82; }
  }
  @keyframes starRotate {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
  @keyframes floatWatermark {
    0%, 100% { transform: translateY(0px); }
    50% { transform: translateY(-12px); }
  }

  /* ==================== ТЕМА 2: FROSTED GLASS ==================== */
  body.theme-2 {
    background: linear-gradient(135deg, #e0f2fe 0%, #bae6fd 52%, #7dd3fc 100%);
    color: #0f172a;
  }
  body.theme-2 .bg-number-4 {
    position: fixed; right: 25.2px; bottom: -30px; font-size: 276.5px; font-weight: 800;
    color: rgba(255, 255, 255, 0.42); pointer-events: none; z-index: 0; line-height: 0.9;
    text-shadow: none; animation: floatWatermark 10s ease-in-out infinite;
  }
  body.theme-2 .flu-sidebar {
    background: rgba(255, 255, 255, 0.84);
    backdrop-filter: blur(18.7px); -webkit-backdrop-filter: blur(18.7px);
    border-right: 1.5px solid rgba(255, 255, 255, 0.96);
    color: #0f172a;
    box-shadow: 4.3px 0 20.2px rgba(15, 23, 42, 0.08);
  }
  body.theme-2 .flu-sidebar:hover { box-shadow: 10.1px 0 31.7px rgba(15, 23, 42, 0.15); }
  body.theme-2 .sb-logo-title { color: #0f172a; font-weight: 700; }
  body.theme-2 .sb-chev { color: #475569; }
  body.theme-2 .sb-item { color: #1e293b; }
  body.theme-2 .sb-item:hover { background: rgba(37, 99, 235, 0.10); color: #1D4ED8; }
  body.theme-2 .sb-item.active {
    background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%);
    color: #ffffff;
    box-shadow: 0 4.3px 13px rgba(37, 99, 235, 0.32);
  }
  body.theme-2 .sb-divider { border-top: 1px solid rgba(148, 163, 184, 0.3); }
  body.theme-2 .sb-seg { background: rgba(226, 232, 240, 0.75); border: 1px solid rgba(203, 213, 225, 0.9); }
  body.theme-2 .sb-seg-btn { color: #334155; }
  body.theme-2 .sb-seg-btn.active {
    background: linear-gradient(135deg, #2563eb, #4f46e5); color: #ffffff;
    box-shadow: 0 2px 5.8px rgba(37, 99, 235, 0.3);
  }
  body.theme-2 .status-capsule,
  body.theme-2 .filter-bar,
  body.theme-2 .table-glass-wrap,
  body.theme-2 .glass-panel,
  body.theme-2 .sp-card,
  body.theme-2 .sp-footer-bar {
    background: rgba(255, 255, 255, 0.84); backdrop-filter: blur(15.8px);
    border: 1.5px solid rgba(255, 255, 255, 0.96);
    box-shadow: 0 8.6px 24.5px rgba(15, 23, 42, 0.06);
  }
  body.theme-2 .sp-folder-tab {
    background: rgba(255, 255, 255, 0.52); color: #334155; border: 1px solid rgba(255, 255, 255, 0.85);
  }
  body.theme-2 .sp-folder-tab.active {
    background: #ffffff; color: #0f172a; border-bottom-color: #ffffff; font-weight: 800;
    box-shadow: 0 -4px 10.1px rgba(15, 23, 42, 0.05);
  }
  body.theme-2 .streams-table thead th {
    background: rgba(224, 242, 254, 0.96); color: #0f172a; border-bottom: 1.5px solid #cbd5e1;
  }
  body.theme-2 .streams-table tbody td { border-bottom: 1px solid rgba(203, 213, 225, 0.65); color: #1e293b; }
  body.theme-2 .streams-table tbody tr:hover td { background: rgba(255, 255, 255, 0.65); }
  body.theme-2 .st-name { color: #1D4ED8; }
  body.theme-2 .search-wrap input, body.theme-2 .icon-btn, body.theme-2 .status-filter-pill {
    background: rgba(255, 255, 255, 0.92); border: 1px solid #cbd5e1; color: #0f172a;
  }
  body.theme-2 .modal-card {
    background: rgba(255, 255, 255, 0.96); backdrop-filter: blur(17.3px); color: #0f172a;
    border: 1.5px solid #ffffff;
  }
  body.theme-2 .form-input, body.theme-2 .out-proto-box {
    background: rgba(255, 255, 255, 0.92); border: 1.5px solid #cbd5e1; color: #0f172a;
  }
  body.theme-2 .media-info-subcard {
    background: rgba(241, 245, 249, 0.78); border: 1px solid #cbd5e1;
  }

  /* ==================== ТЕМА 4: DEEP SPACE ==================== */
  body.theme-4 {
    background: radial-gradient(circle at 86% 12%, #312e81 0%, #0f172a 46%, #050811 100%);
    color: #f8fafc;
  }
  body.theme-4 .bg-number-4 {
    position: fixed; right: 25.2px; bottom: -30px; font-size: 276.5px; font-weight: 800;
    color: rgba(99, 102, 241, 0.07); pointer-events: none; z-index: 0; line-height: 0.9;
    text-shadow: none; animation: floatWatermark 10s ease-in-out infinite;
  }
  body.theme-4 .flu-sidebar {
    background: rgba(12, 18, 34, 0.88);
    backdrop-filter: blur(18.7px); -webkit-backdrop-filter: blur(18.7px);
    border-right: 1px solid rgba(99, 102, 241, 0.32);
    color: #f8fafc;
    box-shadow: 4.3px 0 21.6px rgba(0, 0, 0, 0.5);
  }
  body.theme-4 .flu-sidebar:hover { box-shadow: 11.5px 0 36px rgba(0, 0, 0, 0.75); }
  body.theme-4 .sb-logo-title { color: #ffffff; font-weight: 700; }
  body.theme-4 .sb-chev { color: #94a3b8; }
  body.theme-4 .sb-item { color: #cbd5e1; }
  body.theme-4 .sb-item:hover { background: rgba(99, 102, 241, 0.16); color: #ffffff; }
  body.theme-4 .sb-item.active {
    background: linear-gradient(135deg, #3b82f6 0%, #6366f1 55%, #8b5cf6 100%);
    color: #ffffff;
    box-shadow: 0 4.3px 14.4px rgba(99, 102, 241, 0.45);
  }
  body.theme-4 .sb-divider { border-top: 1px solid rgba(99, 102, 241, 0.24); }
  body.theme-4 .sb-seg { background: rgba(9, 13, 24, 0.78); border: 1px solid rgba(99, 102, 241, 0.32); }
  body.theme-4 .sb-seg-btn { color: #94a3b8; }
  body.theme-4 .sb-seg-btn.active {
    background: linear-gradient(135deg, #3b82f6, #6366f1); color: #ffffff;
    box-shadow: 0 2px 7.2px rgba(99, 102, 241, 0.45);
  }
  body.theme-4 .status-capsule,
  body.theme-4 .filter-bar,
  body.theme-4 .table-glass-wrap,
  body.theme-4 .glass-panel,
  body.theme-4 .sp-card,
  body.theme-4 .sp-footer-bar {
    background: rgba(15, 23, 42, 0.84); backdrop-filter: blur(15.8px);
    border: 1px solid rgba(99, 102, 241, 0.35);
    box-shadow: 0 10.1px 28.8px rgba(0, 0, 0, 0.45);
  }
  body.theme-4 .sp-folder-tab {
    background: rgba(15, 23, 42, 0.55); color: #94a3b8; border: 1px solid rgba(99, 102, 241, 0.25);
  }
  body.theme-4 .sp-folder-tab.active {
    background: rgba(30, 41, 59, 0.98); color: #ffffff; border-color: #6366f1; font-weight: 800;
  }
  body.theme-4 .streams-table thead th {
    background: rgba(15, 23, 42, 0.96); color: #e2e8f0; border-bottom: 1.5px solid rgba(99, 102, 241, 0.35);
  }
  body.theme-4 .streams-table tbody td {
    border-bottom: 1px solid rgba(51, 65, 85, 0.65); color: #e2e8f0;
  }
  body.theme-4 .streams-table tbody tr:hover td { background: rgba(30, 41, 59, 0.65); }
  body.theme-4 .st-name { color: #60a5fa; }
  body.theme-4 .st-sub, body.theme-4 .inp-url, body.theme-4 .out-left { color: #cbd5e1; }
  body.theme-4 .search-wrap input, body.theme-4 .icon-btn, body.theme-4 .status-filter-pill {
    background: rgba(9, 13, 24, 0.82); border: 1px solid rgba(99, 102, 241, 0.35); color: #f8fafc;
  }
  body.theme-4 .status-filter-pill b { color: #ffffff; }
  body.theme-4 .sq-btn { background: rgba(30, 41, 59, 0.9); border-color: #6366f1; color: #a5b4fc; }
  body.theme-4 .more-box { background: #0f172a; border-color: #4f46e5; }
  body.theme-4 .more-row code { color: #93c5fd; }
  body.theme-4 .modal-card {
    background: rgba(15, 23, 42, 0.96); backdrop-filter: blur(18.7px); color: #f8fafc;
    border: 1.5px solid rgba(99, 102, 241, 0.45);
  }
  body.theme-4 .form-input, body.theme-4 .out-proto-box {
    background: rgba(9, 13, 24, 0.86); border: 1.5px solid rgba(99, 102, 241, 0.35); color: #fff;
  }
  body.theme-4 .media-info-subcard {
    background: rgba(9, 13, 24, 0.65); border: 1px solid rgba(99, 102, 241, 0.28);
  }

  /* ==================== БОКОВАЯ ПАНЕЛЬ ==================== */
  .flu-sidebar {
    width: 53.3px; height: 100vh; display: flex; flex-direction: column; justify-content: space-between;
    padding: 11.5px 0; flex-shrink: 0; user-select: none; z-index: 300; overflow: hidden;
    transition: width 0.13s cubic-bezier(0.2, 0, 0, 1), box-shadow 0.13s ease, background 0.25s ease;
  }
  .flu-sidebar:hover { width: 255.6px; }
  .sb-brand {
    height: 36px; display: flex; align-items: center; justify-content: space-between;
    padding: 0 15.1px; margin-bottom: 7.2px; white-space: nowrap;
  }
  .sb-brand-left { display: flex; align-items: center; gap: 11.5px; }
  .sb-star { width: 23px; height: 23px; flex-shrink: 0;  }
  .sb-logo-title { font-size: 19px; letter-spacing: -0.02em; opacity: 0; transition: opacity 0.1s ease; }
  .sb-logo-title span {
    background: linear-gradient(90deg, #3b82f6, #6366f1, #8b5cf6);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .sb-chev { font-size: 19px; opacity: 0; transition: opacity 0.1s ease; }
  .flu-sidebar:hover .sb-logo-title, .flu-sidebar:hover .sb-chev { opacity: 1; }
  .sb-nav { display: flex; flex-direction: column; gap: 10.1px; padding: 0 7.9px; flex: 1; }
  .sb-item {
    height: 41.8px; border-radius: 10.1px; display: flex; align-items: center; gap: 11.5px;
    padding: 0 10.1px; cursor: pointer; white-space: nowrap; transition: background 0.12s ease, color 0.12s ease;
  }
  .sb-ico { width: 17.3px; height: 17.3px; display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; }
  .sb-ico svg { width: 20.2px; height: 20.2px; display: block; filter: drop-shadow(0 2px 2.9px rgba(0,0,0,0.15)); }
  #nav_ico_streams .sb-ico { color: #3b82f6; }
  .sb-item.active .sb-ico { color: #ffffff !important; }
  #nav_ico_pulse .sb-ico { color: #10b981; }
  #nav_ico_config .sb-ico { color: #8b5cf6; }
  #nav_ico_cluster .sb-ico { color: #f59e0b; }
  #nav_ico_iptv .sb-ico { color: #ec4899; }
  #nav_ico_sessions .sb-ico { color: #06b6d4; }
  #nav_ico_tester .sb-ico { color: #ef4444; }
  .sb-lbl { font-size: 13.8px; font-weight: 700; opacity: 0; transition: opacity 0.1s ease; flex: 1; display: flex; align-items: center; justify-content: space-between; }
  .flu-sidebar:hover .sb-lbl { opacity: 1; }
  .sb-bottom { display: flex; flex-direction: column; gap: 4.3px; padding: 7.2px 7.9px 0 7.9px; }
  .sb-divider { margin: 2.9px 4.3px; }
  .sb-seg { display: inline-flex; border-radius: 7.2px; padding: 2.2px; gap: 2.2px; width: 100%; }
  .sb-seg-btn { flex: 1; border: none; background: transparent; padding: 4.3px 7.2px; border-radius: 5.8px; font-size: 11.7px; font-weight: 700; cursor: pointer; transition: 0.12s ease; text-align: center; }
  .sb-logout:hover { background: rgba(239, 68, 68, 0.14) !important; color: #ef4444 !important; }

  /* ==================== ОСНОВНАЯ ОБЛАСТЬ ==================== */
  .main-wrap {
    flex: 1; height: 100vh; display: flex; flex-direction: column; overflow: hidden; position: relative; z-index: 5;
  }
  .top-header {
    min-height: 53.3px; padding: 8.6px 20.2px; display: flex; align-items: center; justify-content: space-between;
    gap: 11.5px; flex-wrap: wrap; flex-shrink: 0;
  }
  .brand-row { display: flex; align-items: center; gap: 10.1px; }
  .star-logo { width: 24.5px; height: 24.5px;  }
  .page-title { font-size: 25.9px; font-weight: 800; letter-spacing: -0.03em; display: flex; align-items: center; gap: 7.2px; }
  .page-title span.grad {
    background: linear-gradient(90deg, #3b82f6 0%, #6366f1 50%, #8b5cf6 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .bc-crumb { font-size: 20.7px; font-weight: 700; display: inline-flex; align-items: center; gap: 7.2px; }
  .bc-link { opacity: 0.6; cursor: pointer; font-weight: 600; }
  .bc-link:hover { opacity: 1; text-decoration: underline; }
  .bc-sep { opacity: 0.4; font-size: 17.3px; }

  .header-right { display: flex; align-items: center; gap: 8.6px; flex-wrap: wrap; }
  .status-capsule {
    border-radius: 999px; padding: 7.9px 15.8px; display: flex; align-items: center; gap: 10.1px;
    font-size: 12.5px; font-weight: 600;
  }
  .status-capsule .sep { opacity: 0.35; }
  .status-capsule b { color: #3b82f6; font-weight: 800; }

  .add-plus-btn {
    background: linear-gradient(135deg, #3b82f6 0%, #6366f1 55%, #8b5cf6 100%);
    color: #fff; border: none; height: 33.1px; padding: 0 15.8px;
    border-radius: 10.1px; font-size: 13.8px; font-weight: 700; cursor: pointer;
    display: flex; align-items: center; gap: 5.8px; box-shadow: 0 4.3px 11.5px rgba(99, 102, 241, 0.35);
  }
  .work-body {
    flex: 1; min-height: 0; overflow: hidden; display: flex; flex-direction: column;
    padding: 2.9px 20.2px 11.5px 20.2px;
  }
  #sec_streams { flex: 1; min-height: 0; overflow: hidden; display: flex; flex-direction: column; }
  #sec_stream_page { flex: 1; min-height: 0; overflow: hidden; display: flex; flex-direction: column; }
  #sec_pulse, #sec_sessions, #sec_config, #sec_iptv, #sec_tester, #sec_cluster {
    flex: 1; min-height: 0; overflow-y: auto;
  }

  /* ==================== СТРАНИЦА ПОТОКА (СКРИНШОТЫ 1 + 2 + 3) ==================== */
  .sp-tabs-bar {
    display: flex; align-items: flex-end; gap: 4.3px; padding: 0 2.9px; flex-shrink: 0; margin-bottom: -1px; z-index: 10;
  }
  .sp-folder-tab {
    padding: 7.9px 17.3px; border-radius: 8.6px 8.6px 0 0; font-size: 13.4px; font-weight: 600;
    cursor: pointer; transition: 0.15s; user-select: none;
  }
  .sp-scroll-area {
    flex: 1; min-height: 0; overflow-y: auto; padding-bottom: 11.5px;
  }
  .sp-card {
    border-radius: 0 13px 13px 13px; padding: 15.8px 17.3px; margin-bottom: 11.5px;
  }
  .sp-subcard {
    border-radius: 13px; padding: 14.4px 15.8px; margin-bottom: 11.5px;
  }
  .sp-grid-2 {
    display: grid; grid-template-columns: 1.05fr 1fr; gap: 13px; align-items: start;
  }
  @media (max-width: 1150px) { .sp-grid-2 { grid-template-columns: 1fr; } }

  .sp-line-input {
    width: 100%; border: none; border-bottom: 2px solid rgba(148, 163, 184, 0.45);
    background: transparent; padding: 5.8px 2.9px; font-size: 13.8px; font-weight: 600; color: inherit; outline: none;
    transition: border-color 0.2s;
  }
  .sp-line-input:focus { border-bottom-color: #3b82f6; }

  .sp-pill-online {
    display: inline-flex; align-items: center; padding: 4.3px 14.4px; border-radius: 999px;
    border: 1.5px solid #10b981; background: rgba(16, 185, 129, 0.12); color: #10b981;
    font-weight: 800; font-size: 13px;
  }
  .sp-pill-wait {
    display: inline-flex; align-items: center; padding: 4.3px 14.4px; border-radius: 999px;
    border: 1.5px solid #f59e0b; background: rgba(245, 158, 11, 0.12); color: #f59e0b;
    font-weight: 800; font-size: 13px;
  }
  .sp-pill-off {
    display: inline-flex; align-items: center; padding: 4.3px 14.4px; border-radius: 999px;
    border: 1.5px solid #94a3b8; background: rgba(148, 163, 184, 0.14); color: #94a3b8;
    font-weight: 800; font-size: 13px;
  }
  .sp-mode-group {
    display: inline-flex; border-radius: 7.2px; overflow: hidden; border: 1.5px solid rgba(59, 130, 246, 0.45);
  }
  .sp-mode-btn {
    border: none; background: transparent; padding: 5.8px 11.5px; font-size: 12.5px; font-weight: 700;
    cursor: pointer; color: inherit; display: inline-flex; align-items: center; gap: 4.3px;
  }
  .sp-mode-btn.active {
    background: linear-gradient(135deg, #1D4ED8, #3b82f6); color: #ffffff !important;
  }

  .player-checkerboard {
    width: 100%; height: 244.8px; border-radius: 8.6px; overflow: hidden; position: relative;
    background-color: #111827;
    background-image:
      linear-gradient(45deg, #1f2937 25%, transparent 25%),
      linear-gradient(-45deg, #1f2937 25%, transparent 25%),
      linear-gradient(45deg, transparent 75%, #1f2937 75%),
      linear-gradient(-45deg, transparent 75%, #1f2937 75%);
    background-size: 17.3px 17.3px;
    background-position: 0 0, 0 8.6px, 8.6px -12px, -12px 0;
    margin: 10.1px 0;
  }
  .player-proto-tabs {
    display: inline-flex; border-radius: 5.8px; overflow: hidden; border: 1px solid rgba(148, 163, 184, 0.4);
  }
  .player-proto-tab {
    border: none; background: transparent; padding: 5.8px 14.4px; font-size: 12.1px; font-weight: 800;
    cursor: pointer; color: inherit;
  }
  .player-proto-tab.active {
    background: #1D4ED8; color: #ffffff !important;
  }

  .media-info-grid {
    display: grid; grid-template-columns: 1fr 1fr; gap: 10.1px; margin-top: 11.5px;
  }
  .media-info-subcard {
    padding: 11.5px; border-radius: 8.6px;
  }
  .mi-title { font-size: 13.4px; font-weight: 700; display: flex; align-items: center; gap: 5.8px; margin-bottom: 7.2px; }
  .mi-track-v { color: #2563eb; text-decoration: underline; font-size: 12.5px; font-weight: 600; display: block; margin-bottom: 4.3px; }
  body.theme-4 .mi-track-v { color: #60a5fa; }
  .mi-track-a { color: #e11d48; text-decoration: underline; font-size: 12.5px; font-weight: 600; display: block; }
  body.theme-4 .mi-track-a { color: #fb7185; }

  /* Вкладка Input со списком URL как на скриншоте 2 */
  .inp-row-item {
    display: flex; align-items: center; gap: 11.5px; padding: 10.1px 0;
    border-bottom: 1px solid rgba(148, 163, 184, 0.25);
  }
  .inp-state-badge {
    min-width: 51.8px; font-size: 12.5px; font-weight: 700; color: #10b981; text-decoration: underline;
  }
  .inp-state-badge.standby { color: #94a3b8; text-decoration: none; }
  .inp-arrows { display: flex; flex-direction: column; gap: 2px; }
  .arr-btn {
    border: none; background: transparent; color: inherit; opacity: 0.55; cursor: pointer; font-size: 11.2px; line-height: 1; padding: 2px 3.6px;
  }
  .arr-btn:hover { opacity: 1; color: #3b82f6; }

  /* Вкладка Output как на скриншоте 3 */
  .out-proto-row {
    display: flex; align-items: center; gap: 11.5px; margin-bottom: 11.5px;
  }
  .eo-switch {
    display: inline-flex; border-radius: 5px; overflow: hidden; border: 1px solid rgba(148, 163, 184, 0.4); flex-shrink: 0;
  }
  .eo-btn {
    border: none; background: rgba(148, 163, 184, 0.18); padding: 5px 9.4px; font-size: 11.2px; font-weight: 700;
    cursor: pointer; color: inherit; opacity: 0.75;
  }
  .eo-btn.active-only { background: #4caf50; color: #ffffff !important; opacity: 1; }
  .eo-btn.active-except { background: #e53935; color: #ffffff !important; opacity: 1; }
  .out-proto-box {
    flex: 1; display: flex; align-items: center; gap: 8.6px; padding: 5.8px 10.1px; border-radius: 7.2px;
  }
  .proto-green-tag {
    display: inline-flex; align-items: center; gap: 3.6px; padding: 2.2px 7.2px; border-radius: 4.3px;
    border: 1.5px solid #10b981; color: #10b981; font-size: 10.8px; font-weight: 800; flex-shrink: 0;
  }
  .out-proto-url {
    flex: 1; border: none; background: transparent; color: inherit; font-size: 13px; outline: none;
  }
  .copy-ico-btn {
    border: none; background: transparent; color: inherit; opacity: 0.65; cursor: pointer;
    font-size: 15.6px; padding: 2.9px 5.8px; border-radius: 4.3px;
  }
  .copy-ico-btn:hover { opacity: 1; background: rgba(59, 130, 246, 0.12); color: #3b82f6; }

  .sp-footer-bar {
    flex-shrink: 0; border-radius: 11.5px; padding: 8.6px 15.8px; display: flex; align-items: center;
    justify-content: space-between; gap: 10.1px; margin-top: 4.3px;
  }
  .btn-pink-danger {
    background: linear-gradient(135deg, #f50057, #d81b60); color: #ffffff; border: none;
    padding: 7.9px 15.8px; border-radius: 7.2px; font-size: 13.4px; font-weight: 700; cursor: pointer;
    box-shadow: 0 2.9px 10.1px rgba(245, 0, 87, 0.35);
  }

  /* ==================== ТАБЛИЦА СПИСКА ПОТОКОВ ==================== */
  .filter-bar {
    flex-shrink: 0; display: flex; align-items: center; justify-content: space-between;
    gap: 10px; flex-wrap: nowrap; margin-bottom: 7.2px; padding: 11.5px 15.8px; border-radius: 13px;
  }
  .search-wrap { position: relative; flex: 1 1 140px; min-width: 140px; max-width: 268px; }
  .search-wrap input {
    width: 100%; height: 33.1px; padding: 0 11.5px 0 30.2px; border-radius: 8.6px; font-size: 13.8px; font-weight: 500; outline: none;
  }
  .search-wrap .s-ico { position: absolute; left: 10.1px; top: 9.4px; font-size: 13.8px; opacity: 0.6; }
  .filter-controls { display: flex; align-items: center; gap: 7.2px; flex-wrap: nowrap; flex-shrink: 0; }
    .add-plus-btn, .icon-btn, .status-filter-pill, .buf-mode-btn { white-space: nowrap; flex-shrink: 0; }

  .icon-btn {
    height: 31.7px; padding: 0 11.5px; border-radius: 8.6px; cursor: pointer;
    font-size: 13px; font-weight: 600; display: inline-flex; align-items: center; gap: 5.8px;
  }
  .status-filter-pill {
    height: 31.7px; padding: 0 11.5px; border-radius: 8.6px; font-size: 13px; font-weight: 600;
    cursor: pointer; display: inline-flex; align-items: center; gap: 6.5px; user-select: none;
  }
  .status-filter-pill.active { border-color: #6366f1 !important; box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.3); }
  .dot-ico { width: 7.9px; height: 7.9px; border-radius: 50%; display: inline-block; }
  .pagination-row {
    flex-shrink: 0; display: flex; align-items: center; gap: 13px; font-size: 13.4px; font-weight: 600; margin: 2.9px 2.9px 5.8px 2.9px; opacity: 0.85;
  }
  .pagination-row select { border: none; background: transparent; font-size: 13.4px; font-weight: 700; color: inherit; cursor: pointer; }

  .table-glass-wrap { flex: 1; min-height: 0; overflow-y: auto; border-radius: 13px; }
  .streams-table { width: 100%; border-collapse: collapse; font-size: 13.8px; }
  .streams-table thead th {
    position: sticky; top: 0; z-index: 30; backdrop-filter: blur(11.5px);
    font-weight: 700; font-size: 13.4px; padding: 10.8px 11.5px; text-align: left;
  }
  .streams-table tbody td { padding: 12.2px 11.5px; vertical-align: top; line-height: 1.5; }
  .st-name { font-weight: 800; font-size: 16px; text-decoration: none; cursor: pointer; display: inline-block; margin-bottom: 2.2px; }
  .st-name:hover { text-decoration: underline; }
  .st-sub { font-size: 13px; font-weight: 500; opacity: 0.85; }
  .st-mode { font-size: 12.1px; opacity: 0.68; }
  .st-status-Online { font-size: 13px; color: #10b981; font-weight: 700; margin-top: 2.9px; }
  .st-status-Waiting { font-size: 13px; color: #f59e0b; font-weight: 700; margin-top: 2.9px; }
  .st-status-Disabled { font-size: 13px; color: #94a3b8; font-weight: 600; margin-top: 2.9px; }
  .st-status-Error { font-size: 13px; color: #ef4444; font-weight: 700; margin-top: 2.9px; }
  .inp-top { display: flex; align-items: center; justify-content: space-between; gap: 7.2px; }
  .inp-url { opacity: 1 !important;  max-width: 288px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; font-weight: 500; }
  .info-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
  .info-tooltip {
    visibility: hidden; width: max-content; min-width: 374.4px; background: rgba(255, 255, 255, 0.98);
    backdrop-filter: blur(11.5px); color: #0f172a; text-align: left; border-radius: 8.6px;
    padding: 11.5px; position: absolute; z-index: 1000; bottom: 135%; left: 50%;
    transform: translateX(-50%) translateY(7.2px); box-shadow: 0 10.1px 21.6px rgba(0,0,0,0.25);
    opacity: 0; transition: all 0.25s ease; border: 1.5px solid #cbd5e1; cursor: default; max-height: 40vh; overflow-y: auto;
  }
  body.theme-4 .info-tooltip { background: rgba(15, 23, 42, 0.98); color: #f8fafc; border-color: rgba(99, 102, 241, 0.5); box-shadow: 0 10.1px 28.8px rgba(0,0,0,0.7); }
  .info-tooltip-wrap:hover .info-tooltip { visibility: visible; opacity: 1; transform: translateX(-50%) translateY(0); }
  .info-badge {
    width: 16.6px; height: 16.6px; border-radius: 50%;
    background: linear-gradient(135deg, #3b82f6, #6366f1); color: #fff;
    font-size: 10.8px; font-weight: 800; font-family: serif; font-style: italic;
    display: inline-flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0;
  }
  .input-dots { display: flex; align-items: center; gap: 4.3px; margin: 5px 0; }
  .inp-dot { width: 9.2px; height: 9.2px; border-radius: 50%; background: #94a3b8; opacity: 0.45; cursor: pointer; display: inline-block; }
  .inp-dot.active-online { background: #10b981; opacity: 1; box-shadow: 0 0 6.5px rgba(16,185,129,0.7); animation: pulseGlow 2s infinite; filter: drop-shadow(0 0 2.2px rgba(16,185,129,0.4)); }
  .inp-dot.active-wait { background: #f59e0b; opacity: 1; }
  .inp-dot.active-err { background: #ef4444; opacity: 1; }
  .inp-meta { font-size: 12.5px; opacity: 0.88; }

  .buf-toggle-group {
    display: inline-flex; align-items: center; border-radius: 6.5px; padding: 2px;
    background: rgba(148, 163, 184, 0.16); border: 1px solid rgba(148, 163, 184, 0.32); margin-bottom: 3.6px;
  }
  .buf-mode-btn {
    border: none; background: transparent; padding: 3.6px 8.6px; border-radius: 5px;
    font-size: 11.7px; font-weight: 700; cursor: pointer; color: inherit; opacity: 0.72; transition: 0.14s;
  }
  .buf-mode-btn.active {
    background: linear-gradient(135deg, #3b82f6, #6366f1); color: #fff !important; opacity: 1;
    box-shadow: 0 2px 5.8px rgba(59, 130, 246, 0.35);
  }
  .buf-pill {
    display: inline-block; padding: 2.2px 6.5px; border-radius: 4.3px; font-size: 11.2px; font-weight: 700;
    background: rgba(16, 185, 129, 0.14); color: #10b981; margin-top: 2.9px;
  }

  .pulse-row-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 13px; margin-top: 10.1px; }
  @media (max-width: 1200px) { .pulse-row-3 { grid-template-columns: 1fr; } }
  .pulse-chart-card { padding: 14.4px 15.8px; border-radius: 14.4px; position: relative; display: flex; flex-direction: column; justify-content: space-between; }
  .pulse-chart-head { display: flex; flex-direction: column; gap: 5.8px; margin-bottom: 8.6px; }
  .pulse-chart-title { font-size: 15.1px; font-weight: 800; letter-spacing: -0.01em; }
  .pulse-legend { display: flex; align-items: center; gap: 10.1px; font-size: 12.1px; font-weight: 700; flex-wrap: wrap; }
  .leg-dot { width: 7.2px; height: 7.2px; border-radius: 50%; display: inline-block; margin-right: 4.3px; }
  canvas.pulse-canvas { width: 100%; height: 180px; display: block; border-radius: 8.6px; }

  .muted-cell { font-size: 13.4px; opacity: 0.85; font-weight: 500; }
  .out-cell-wrap { display: flex; align-items: flex-start; justify-content: space-between; gap: 10.1px; }
  .out-left { font-size: 13px; }
  .more-link { color: #3b82f6; font-weight: 700; font-size: 12.5px; cursor: pointer; text-decoration: underline; display: inline-block; margin-top: 2.9px; }
  .more-box {
    margin-top: 7.2px; padding: 7.2px 8.6px; background: rgba(248, 250, 252, 0.95);
    border: 1px solid #cbd5e1; border-radius: 7.2px; font-size: 11.7px; display: none; min-width: 223.2px;
  }
  .more-row { display: flex; align-items: center; justify-content: space-between; gap: 7.2px; padding: 2.9px 0; }
  .more-row code { font-size: 10.8px; word-break: break-all; }
  .out-actions { display: flex; align-items: center; gap: 6.5px; flex-shrink: 0; }
  .sq-btn {
    width: 25.2px; height: 24.5px; border: 1.5px solid #93c5fd; background: #fff; color: #2563eb;
    border-radius: 5.8px; cursor: pointer; display: inline-flex; align-items: center; justify-content: center;
    font-size: 13.4px; font-weight: 700;
  }
  .flu-switch { position: relative; display: inline-block; width: 30.2px; height: 16.6px; cursor: pointer; }
  .flu-switch input { opacity: 0; width: 0; height: 0; }
  .flu-slider { position: absolute; inset: 0; background-color: #94a3b8; border-radius: 16.6px; transition: 0.2s; }
  .flu-slider:before {
    position: absolute; content: ""; height: 12.2px; width: 12.2px; left: 2.2px; bottom: 2.2px;
    background-color: white; border-radius: 50%; transition: 0.2s;
  }
  .flu-switch input:checked + .flu-slider { background: linear-gradient(90deg, #3b82f6, #6366f1); }
  .flu-switch input:checked + .flu-slider:before { transform: translateX(13.7px); }
  .modal-backdrop {
    position: fixed; inset: 0; background: rgba(7, 10, 20, 0.65); backdrop-filter: blur(5.8px);
    display: none; align-items: center; justify-content: center; z-index: 400;
  }
  .modal-card {
    width: 547.2px; max-width: 95vw; max-height: 90vh; border-radius: 15.8px;
    box-shadow: 0 18px 46.8px rgba(0,0,0,0.4); display: flex; flex-direction: column; overflow: hidden;
  }
  .modal-head {
    padding: 13px 17.3px; border-bottom: 1px solid rgba(148, 163, 184, 0.25);
    display: flex; align-items: center; justify-content: space-between; font-size: 17.3px;
  }
  .modal-body { padding: 17.3px; overflow-y: auto; flex: 1; }
  .form-row { margin-bottom: 11.5px; }
  .form-row label { display: block; font-size: 13px; font-weight: 700; margin-bottom: 4.3px; opacity: 0.85; }
  .form-input { width: 100%; padding: 7.9px 10.1px; border-radius: 7.2px; font-size: 13.8px; outline: none; }
  .btn {
    background: linear-gradient(135deg, #3b82f6 0%, #6366f1 55%, #8b5cf6 100%);
    color: #fff; border: none; padding: 7.9px 15.8px; border-radius: 7.2px; font-size: 13.4px; cursor: pointer; font-weight: 700;
  }
  .btn-outline { background: rgba(148, 163, 184, 0.15); color: inherit; border: 1px solid rgba(148, 163, 184, 0.4); }
  .btn-danger { background: #ef4444; color: #fff; }
  .btn-sm { padding: 5px 10.1px; font-size: 12.1px; border-radius: 5.8px; }
  .glass-panel { padding: 15.8px; border-radius: 13px; }
  .toast {
    position: fixed; bottom: 17.3px; right: 17.3px;
    background: linear-gradient(90deg, #1e293b, #312e81); color: #fff;
    padding: 9.4px 15.8px; border-radius: 8.6px; font-size: 13px; font-weight: 600;
    display: none; z-index: 999; box-shadow: 0 7.2px 18px rgba(0,0,0,0.35);
  }

  /* === FORCE ALIGNMENT CSS === */
  .flu-sidebar { padding-top: 8.6px !important; }
  .sb-brand { height: 36px !important; margin-bottom: 15.8px !important; }
  .top-header { height: 51.8px !important; min-height: 51.8px !important; padding: 0 20.2px !important; margin: 0 !important; }
  .work-body { padding: 0 20.2px 11.5px 20.2px !important; }
  .filter-bar { padding: 8.6px 13px !important; margin-top: 0 !important; }
  #nav_ico_streams { height: 33.1px !important; margin-top: 0 !important; }
  .add-plus-btn { height: 33.1px !important; }

  .out-grid-caption {
    font-size: 12.1px !important;
    font-weight: 700 !important;
    opacity: 0.85 !important;
    margin: 8.6px 0 4.3px 100.8px !important;
  }
  .out-grid-row {
    display: grid !important;
    grid-template-columns: 89.3px 1fr !important;
    align-items: center !important;
    gap: 11.5px !important;
    margin-bottom: 10.1px !important;
  }
  .eo-switch {
    width: 89.3px !important;
    height: 31.7px !important;
    display: grid !important;
    grid-template-columns: 1fr 1fr !important;
    border-radius: 5.8px !important;
    overflow: hidden !important;
  }
  .eo-btn {
    width: 100% !important;
    height: 100% !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
  }
  .out-proto-box {
    display: grid !important;
    grid-template-columns: 77.8px 1fr auto !important;
    align-items: center !important;
    gap: 10.1px !important;
    height: 37.4px !important;
    padding: 0 5.8px 0 8.6px !important;
    border-radius: 8.6px !important;
  }
  .out-copy-btn {
    height: 27.4px !important;
    padding: 0 13px !important;
    border-radius: 6.5px !important;
    border: none !important;
    font-size: 12.5px !important;
    font-weight: 700 !important;
    cursor: pointer !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    gap: 5px !important;
    color: #ffffff !important;
    white-space: nowrap !important;
    transition: transform 0.12s ease, box-shadow 0.15s ease, opacity 0.15s ease !important;
  }
  body.theme-2 .out-copy-btn {
    background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%) !important;
    box-shadow: 0 2.9px 8.6px rgba(37, 99, 235, 0.28) !important;
  }
  body.theme-4 .out-copy-btn {
    background: linear-gradient(135deg, #3b82f6 0%, #6366f1 55%, #8b5cf6 100%) !important;
    box-shadow: 0 2.9px 10.1px rgba(99, 102, 241, 0.42) !important;
  }
  .out-copy-btn:hover { transform: translateY(-1px) !important; opacity: 0.94 !important; }
  .out-copy-btn:active { transform: translateY(0) !important; }
  .proto-green-tag {
    width: 77.8px !important;
    height: 20.2px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
  }


  * { text-shadow: none !important; }
  .copy-ico-btn {
    background: linear-gradient(135deg, #2563eb, #4f46e5) !important;
    color: #ffffff !important; opacity: 1 !important;
    height: 25.9px !important; padding: 0 11.5px !important;
    border-radius: 5.8px !important; font-size: 12.1px !important;
    font-weight: 700 !important; display: inline-flex !important;
    align-items: center !important; justify-content: center !important;
    gap: 4.3px !important; width: auto !important;
  }
  .out-proto-box {
    grid-template-columns: 77.8px 1fr auto !important;
  }


  /* === AQUA REDESIGN OVERRIDES (THEME 2) === */
  body.theme-2 { background: linear-gradient(135deg, #d6ebfe 0%, #b4dcfc 100%); color: #0f172a; }
  body.theme-2 .flu-sidebar { background: #133c7a; border-right: none; box-shadow: 2.9px 0 17.3px rgba(0,0,0,0.15); }
  body.theme-2 .flu-sidebar .sb-logo-title, body.theme-2 .flu-sidebar .sb-chev { color: #ffffff !important; }
  body.theme-2 .flu-sidebar .sb-item { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-item:hover { background: rgba(255, 255, 255, 0.1); color: #ffffff; }
  body.theme-2 .flu-sidebar .sb-item.active { background: #2563eb; color: #ffffff; box-shadow: 0 2.9px 10.1px rgba(0,0,0,0.25); }
  body.theme-2 .flu-sidebar .sb-seg { background: rgba(0,0,0,0.25); border: none; }
  body.theme-2 .flu-sidebar .sb-seg-btn { color: #93c5fd; }
  body.theme-2 .flu-sidebar .sb-seg-btn.active { background: #3b82f6; color: #ffffff; box-shadow: none; }
  body.theme-2 .sb-divider { border-top: 1px solid rgba(255,255,255,0.1); }
  
  body.theme-2 .filter-bar { background: rgba(255, 255, 255, 0.35); border: 1px solid rgba(255, 255, 255, 0.6); box-shadow: 0 2.9px 11.5px rgba(0,0,0,0.02); }
  body.theme-2 .add-plus-btn { background: #133c7a; box-shadow: 0 2.9px 8.6px rgba(19,60,122,0.3); border-radius: 5.8px; }
  body.theme-2 .status-filter-pill { background: #ffffff; border-radius: 5.8px; border: 1px solid #bae6fd; font-size: 10.1px; }
  body.theme-2 .status-filter-pill.active { background: #133c7a; color: #ffffff; border-color: #133c7a; }
  body.theme-2 .status-filter-pill.active b { color: #ffffff !important; }
  
  body.theme-2 .table-glass-wrap { background: transparent; border: none; box-shadow: none; }
  body.theme-2 .streams-table { border-collapse: separate; border-spacing: 0 7.2px; }
  body.theme-2 .streams-table thead th { background: transparent; border: none; color: #1e3a8a; font-weight: 800; padding-bottom: 2.9px; }
  body.theme-2 .streams-table tbody tr { background: #ffffff; box-shadow: 0 2.9px 8.6px rgba(0,0,0,0.03); transition: transform 0.1s ease; }
  body.theme-2 .streams-table tbody tr:hover { transform: translateY(-2px); box-shadow: 0 4.3px 11.5px rgba(0,0,0,0.08); }
  body.theme-2 .streams-table tbody td { border: none; padding: 13px 11.5px; }
  body.theme-2 .streams-table tbody td:first-child { border-radius: 8.6px 0 0 8.6px; }
  body.theme-2 .streams-table tbody td:last-child { border-radius: 0 8.6px 8.6px 0; }
  
  body.theme-2 .st-status-Online { background: #d1fae5; color: #065f46; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #a7f3d0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Waiting { background: #fef3c7; color: #92400e; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fde68a; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Disabled { background: #f1f5f9; color: #475569; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #e2e8f0; font-size: 9.7px; margin-top: 5.8px; }
  body.theme-2 .st-status-Error { background: #fee2e2; color: #b91c1c; display: inline-block; padding: 2.2px 8.6px; border-radius: 999px; border: 1px solid #fecaca; font-size: 9.7px; margin-top: 5.8px; }
  
  body.theme-2 .sq-btn { border-radius: 50%; background: #e0f2fe; color: #0284c7; border: 1px solid #bae6fd; width: 27.4px; height: 27.4px; }
  body.theme-2 .sq-btn:hover { background: #bae6fd; }
  body.theme-2 .buf-toggle-group { background: #e0f2fe; border: 1px solid #bae6fd; border-radius: 5.8px; }
  body.theme-2 .buf-mode-btn.active { background: #133c7a; color: #ffffff !important; box-shadow: none; }
  
  body.theme-2 .top-header .status-capsule { background: transparent; border: none; box-shadow: none; font-size: 10.1px; color: #1e3a8a; }
  body.theme-2 .top-header .status-capsule b { color: #2563eb; }
  body.theme-2 .search-wrap input { background: #ffffff; border: 1px solid #bae6fd; }
</style>

</head>
<body class="theme-__DEFAULT_THEME__">
<div class="bg-number-4">Aria</div>
<div class="toast" id="toast_box">OK</div>

<aside class="flu-sidebar">
  <div class="sb-brand">
    <div class="sb-brand-left">
      <svg class="sb-star" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="og_sb-star" x1="15%" y1="10%" x2="85%" y2="45%">
      <stop offset="0%" stop-color="#00F0FF"/>
      <stop offset="100%" stop-color="#0284C7"/>
    </linearGradient>
    <linearGradient id="bg_sb-star" x1="85%" y1="35%" x2="30%" y2="95%">
      <stop offset="0%" stop-color="#4285F4"/>
      <stop offset="100%" stop-color="#1D4ED8"/>
    </linearGradient>
    <linearGradient id="pg_sb-star" x1="30%" y1="95%" x2="25%" y2="15%">
      <stop offset="0%" stop-color="#8B5CF6"/>
      <stop offset="100%" stop-color="#4F46E5"/>
    </linearGradient>
  </defs>
  <g style="transform-origin: 50px 50px; animation: spinStar 14s linear infinite;">
    <path d="M 16,22 A 44,44 0 0,1 91,34 C 88,44 79,50 70,50 L 36,29 C 28,24 21,21 16,22 Z" fill="url(#og_sb-star)"/>
    <path d="M 91,34 A 44,44 0 0,1 27,88 C 29,78 32,71 36,71 L 70,50 C 79,45 88,41 91,34 Z" fill="url(#bg_sb-star)"/>
    <path d="M 27,88 A 44,44 0 0,1 16,22 C 25,21 36,25 36,29 L 36,71 C 36,79 31,85 27,88 Z" fill="url(#pg_sb-star)"/>
  </g>
  <polygon points="37,29 37,71 71,50" fill="#ffffff"/>
</svg>
      <span class="sb-logo-title">StreamFlow <span>Aria</span></span>
    </div>
    <span class="sb-chev">&laquo;</span>
  </div>

  <nav class="sb-nav">
    <div class="sb-item active" id="nav_ico_streams" onclick="showSection('streams')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v14l11-7-11-7z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_streams">Потоки</span>
    </div>
    <div class="sb-item" id="nav_ico_pulse" onclick="showSection('pulse')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M5 9.2h3V19H5V9.2zM10.6 5h2.8v14h-2.8V5zm5.6 8H19v6h-2.8v-6z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_pulse">Телеметрия Pulse</span>
    </div>
    <div class="sb-item" id="nav_ico_config" onclick="showSection('config')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M19.14 12.94c.04-.3.06-.61.06-.94 0-.32-.02-.64-.07-.94l2.03-1.58a.49.49 0 0 0 .12-.61l-1.92-3.32a.49.49 0 0 0-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54a.484.484 0 0 0-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.05.3-.09.63-.09.94s.02.64.07.94l-2.03 1.58a.49.49 0 0 0-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_config">Шаблоны и конфиг</span>
    </div>
    <div class="sb-item" id="nav_ico_cluster" onclick="showSection('cluster')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2L8.5 5.5 12 9l3.5-3.5L12 2zm7 7l-3.5 3.5L19 16l3.5-3.5L19 9zM5 9l-3.5 3.5L5 16l3.5-3.5L5 9zm7 7l-3.5 3.5L12 23l3.5-3.5L12 16z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_cluster">Кластер / Пиры</span>
    </div>
    <div class="sb-item" id="nav_ico_iptv" onclick="showSection('iptv')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 1.99-.9 1.99-2L23 5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_iptv">Плейлисты M3U</span>
    </div>
    <div class="sb-item" id="nav_ico_sessions" onclick="showSection('sessions')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_sessions">Сессии зрителей</span>
    </div>
    <div class="sb-item" id="nav_ico_tester" onclick="showSection('tester')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M18 4l2 4h-3l-2-4h-2l2 4h-3l-2-4H8l2 4H7L5 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4h-4z"/></svg></span>
      <span class="sb-lbl" id="tab_nav_tester">Плеер потоков</span>
    </div>
  </nav>

  <div class="sb-bottom">
    <div class="sb-divider"></div>
    <div class="sb-item" onclick="setAppTheme(APP_THEME === '2' ? '4' : '2')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 3a9 9 0 1 0 9 9c0-.46-.04-.92-.1-1.36a5.389 5.389 0 0 1-4.4 2.26 5.403 5.403 0 0 1-3.14-9.8c-.44-.06-.9-.1-1.36-.1z"/></svg></span>
      <div class="sb-lbl" onclick="event.stopPropagation()">
        <div class="sb-seg">
          <button class="sb-seg-btn" id="btn_theme_2" onclick="setAppTheme('2')">❄️ Glass</button>
          <button class="sb-seg-btn" id="btn_theme_4" onclick="setAppTheme('4')">🌌 Space</button>
        </div>
      </div>
    </div>
    <div class="sb-item" onclick="setAppLang(APP_LANG === 'ru' ? 'en' : 'ru')">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zm6.93 6h-2.95a15.65 15.65 0 0 0-1.38-3.56A8.03 8.03 0 0 1 18.92 8zM12 4.04c.83 1.2 1.48 2.53 1.91 3.96h-3.82c.43-1.43 1.08-2.76 1.91-3.96zM4.26 14C4.1 13.36 4 12.69 4 12s.1-1.36.26-2h3.38c-.08.66-.14 1.32-.14 2s.06 1.34.14 2H4.26zm.82 2h2.95c.32 1.25.78 2.45 1.38 3.56A7.987 7.987 0 0 1 5.08 16zm2.95-8H5.08a7.987 7.987 0 0 1 4.33-3.56A15.65 15.65 0 0 0 8.03 8zM12 19.96c-.83-1.2-1.48-2.53-1.91-3.96h3.82c-.43 1.43-1.08 2.76-1.91 3.96zM14.34 14H9.66c-.09-.66-.16-1.32-.16-2s.07-1.35.16-2h4.68c.09.65.16 1.32.16 2s-.07 1.34-.16 2zm.25 5.56c.6-1.11 1.06-2.31 1.38-3.56h2.95a8.03 8.03 0 0 1-4.33 3.56zM16.36 14c.08-.66.14-1.32.14-2s-.06-1.34-.14-2h3.38c.16.64.26 1.31.26 2s-.1 1.36-.26 2h-3.38z"/></svg></span>
      <div class="sb-lbl" onclick="event.stopPropagation()">
        <div class="sb-seg">
          <button class="sb-seg-btn" id="btn_lang_ru" onclick="setAppLang('ru')">Русский (RU)</button>
          <button class="sb-seg-btn" id="btn_lang_en" onclick="setAppLang('en')">English (EN)</button>
        </div>
      </div>
    </div>
    <div class="sb-item sb-logout" onclick="doLogout()">
      <span class="sb-ico"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.58L17 17l5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z"/></svg></span>
      <span class="sb-lbl" id="i18n_logout_lbl">Выход</span>
    </div>
  </div>
</aside>

<div class="main-wrap">
  <div class="top-header">
    <div class="brand-row">
      <svg class="star-logo" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="og_star-logo" x1="15%" y1="10%" x2="85%" y2="45%">
      <stop offset="0%" stop-color="#00F0FF"/>
      <stop offset="100%" stop-color="#0284C7"/>
    </linearGradient>
    <linearGradient id="bg_star-logo" x1="85%" y1="35%" x2="30%" y2="95%">
      <stop offset="0%" stop-color="#4285F4"/>
      <stop offset="100%" stop-color="#1D4ED8"/>
    </linearGradient>
    <linearGradient id="pg_star-logo" x1="30%" y1="95%" x2="25%" y2="15%">
      <stop offset="0%" stop-color="#8B5CF6"/>
      <stop offset="100%" stop-color="#4F46E5"/>
    </linearGradient>
  </defs>
  <g style="transform-origin: 50px 50px; animation: spinStar 14s linear infinite;">
    <path d="M 16,22 A 44,44 0 0,1 91,34 C 88,44 79,50 70,50 L 36,29 C 28,24 21,21 16,22 Z" fill="url(#og_star-logo)"/>
    <path d="M 91,34 A 44,44 0 0,1 27,88 C 29,78 32,71 36,71 L 70,50 C 79,45 88,41 91,34 Z" fill="url(#bg_star-logo)"/>
    <path d="M 27,88 A 44,44 0 0,1 16,22 C 25,21 36,25 36,29 L 36,71 C 36,79 31,85 27,88 Z" fill="url(#pg_star-logo)"/>
  </g>
  <polygon points="37,29 37,71 71,50" fill="#ffffff"/>
</svg>
      <div class="page-title" id="main_header_title">
        StreamFlow <span class="grad">Aria</span>
      </div>
    </div>
    <div class="header-right">
      <div class="status-capsule">
        <span><span id="cap_lbl_streams">ПОТОКИ</span>: <b id="cap_streams">0 / 0</b></span>
        <span class="sep">|</span>
        <span><span id="cap_lbl_clients">ЗРИТЕЛИ</span>: <b id="cap_clients">0</b></span>
        <span class="sep">|</span>
        <span>IN: <b id="cap_in">0.00 MBPS</b></span>
        <span class="sep">|</span>
        <span>OUT: <b id="cap_out">0.00 MBPS</b></span>
        <span class="sep">|</span>
        <span>UP: <b id="cap_up">0D 00:00:00</b></span>
      </div>
    </div>
  </div>

  <div class="work-body">
    <!-- ==================== 1. ГЛАВНЫЙ СПИСОК ПОТОКОВ ==================== -->
    <div id="sec_streams">
      <div class="filter-bar">
        <button class="add-plus-btn" onclick="openStreamEditor(null)">+ <span id="i18n_add_btn">Добавить поток</span></button>
        <div class="search-wrap">
          <span class="s-ico">&#128269;</span>
          <input type="text" id="text_filter" placeholder="Поиск по имени или адресу источника..." oninput="renderStreamsTable()">
        </div>
        <div class="filter-controls">
          <button class="icon-btn" onclick="reloadAllStreams()">&#8635; <span id="i18n_reload_all">Перегрузить все</span></button>
          <div class="icon-btn" style="padding:0 7.2px; gap:4.3px;">
            <span style="font-size: 12.1px;opacity:0.85;" id="i18n_buf_all_lbl">Буфер всех:</span>
            <button class="buf-mode-btn" id="i18n_buf_all_30" onclick="setStreamBufferMode('__all__', '30s')">30 сек</button>
            <button class="buf-mode-btn" id="i18n_buf_all_norm" onclick="setStreamBufferMode('__all__', 'normal')">Обычный</button>
          </div>
          <select class="icon-btn" id="sort_by" onchange="renderStreamsTable()">
            <option value="name" id="i18n_sort_name">Сортировка: Имя</option>
            <option value="status" id="i18n_sort_status">Сортировка: Статус</option>
            <option value="bitrate" id="i18n_sort_br">Сортировка: Битрейт</option>
            <option value="clients" id="i18n_sort_cl">Сортировка: Зрители</option>
          </select>
          <span style="font-size: 12.5px; font-weight:600;" id="i18n_filter_lbl">Статус:</span>
          <div class="status-filter-pill" id="f_pill_Online" onclick="toggleStatusFilter('Online')">
            <span class="dot-ico" style="background:#10b981;"></span><span>Online (<b id="cnt_Online">0</b>)</span>
          </div>
          <div class="status-filter-pill" id="f_pill_Waiting" onclick="toggleStatusFilter('Waiting')">
            <span class="dot-ico" style="background:#f59e0b;"></span><span>Waiting (<b id="cnt_Waiting">0</b>)</span>
          </div>
          <div class="status-filter-pill" id="f_pill_Disabled" onclick="toggleStatusFilter('Disabled')">
            <span class="dot-ico" style="background:#94a3b8;"></span><span>Disabled (<b id="cnt_Disabled">0</b>)</span>
          </div>
          <div class="status-filter-pill" id="f_pill_Error" onclick="toggleStatusFilter('Error')">
            <span class="dot-ico" style="background:#ef4444;"></span><span>Error (<b id="cnt_Error">0</b>)</span>
          </div>
        </div>
      </div>
      <div class="pagination-row">
        <div><span id="i18n_per_page">На странице</span> <select id="per_page_top" onchange="syncPerPage(this.value)"><option value="25">25</option><option value="50">50</option><option value="100" selected>100</option><option value="500">500</option></select></div>
        <div id="range_label_top">0 из 0</div>
      </div>
      <div class="table-glass-wrap">
        <table class="streams-table">
          <thead>
            <tr>
              <th style="width:30.2px;"><input type="checkbox" onclick="toggleAllCheckboxes(this)"></th>
              <th style="width:18%;" id="th_stream">Канал (Stream)</th>
              <th style="width:30%;" id="th_input">Источник (Input)</th>
              <th style="width:16%;" id="th_transcode">Буфер</th>
              <th style="width:12%;" id="th_dvr">Архив DVR</th>
              <th style="width:24%;" id="th_output">Выход (Output)</th>
            </tr>
          </thead>
          <tbody id="streams_tbody"></tbody>
        </table>
      </div>
    </div>

    <!-- ==================== 2. ПОЛНОЭКРАННАЯ СТРАНИЦА ПОТОКА (СКРИНЫ 1, 2, 3) ==================== -->
    <div id="sec_stream_page" style="display:none;">
      <div class="sp-tabs-bar">
        <div class="sp-folder-tab active" id="sptab_overview" onclick="switchStreamPageTab('overview')">Overview</div>
        <div class="sp-folder-tab" id="sptab_input" onclick="switchStreamPageTab('input')">Input</div>
        <div class="sp-folder-tab" id="sptab_dvr" onclick="switchStreamPageTab('dvr')">DVR</div>
        <div class="sp-folder-tab" id="sptab_output" onclick="switchStreamPageTab('output')">Output</div>
        <div class="sp-folder-tab" id="sptab_auth" onclick="switchStreamPageTab('auth')">Auth</div>
        <div class="sp-folder-tab" id="sptab_sessions" onclick="switchStreamPageTab('sessions')">Play sessions</div>
      </div>

      <div class="sp-scroll-area">
        <!-- ВКЛАДКА 1: OVERVIEW (Скриншот 1) -->
        <div id="sp_pane_overview">
          <div class="sp-card">
            <div style="font-size: 14.7px; font-weight:800; margin-bottom:10.1px;" id="i18n_sp_desc_head">Описание и параметры канала (Description)</div>
            <div style="display:grid; grid-template-columns: 1fr 1fr 1.2fr 1fr 0.8fr; gap:14.4px; align-items:end;">
              <div>
                <label id="i18n_lbl_name" style="font-size: 11.2px; opacity:0.65; font-weight:700;" id="i18n_lbl_name">Name (ID)</label>
                <div style="display:flex; align-items:center; gap:4.3px;">
                  <input type="text" id="f_name" class="sp-line-input" placeholder="southpark">
                  <span class="info-badge">i</span>
                </div>
              </div>
              <div>
                <label id="i18n_lbl_title" style="font-size: 11.2px; opacity:0.65; font-weight:700;" id="i18n_lbl_title">Title</label>
                <div style="display:flex; align-items:center; gap:4.3px;">
                  <input type="text" id="f_title" class="sp-line-input" placeholder="South Park TV">
                  <span class="info-badge">i</span>
                </div>
              </div>
              <div>
                <label id="i18n_lbl_desc" style="font-size: 11.2px; opacity:0.65; font-weight:700;" id="i18n_lbl_desc">Description</label>
                <input type="text" id="f_desc" class="sp-line-input" placeholder="Описание потока...">
              </div>
              <div>
                <label style="font-size: 11.2px; opacity:0.65; font-weight:700;" id="i18n_f_qual_lbl">Quality Profile</label>
                <select id="f_quality" class="sp-line-input">
                  <option value="best" id="i18n_opt_q_best">Auto Best (4K / 1080p)</option>
                  <option value="1080p">1080p (Full HD)</option>
                  <option value="720p">720p (HD)</option>
                  <option value="480p" id="i18n_opt_q_sd">480p / 360p (SD)</option>
                </select>
              </div>
              <div>
                <label id="i18n_lbl_ram" style="font-size: 11.2px; opacity:0.65; font-weight:700;" id="i18n_lbl_ram">RAM Buffer</label>
                <select id="f_buf_mode" class="sp-line-input">
                  <option value="30s">30s Anti-Freeze</option>
                  <option value="normal">Normal (6s)</option>
                </select>
              </div>
            </div>
          </div>

          <div class="sp-grid-2">
            <!-- Левая колонка: управление + встроенный плеер (Скриншот 1) -->
            <div class="sp-card sp-subcard">
              <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:7.2px;">
                <div style="display:flex; gap:7.2px;">
                  <button class="btn btn-sm" id="sp_btn_disable" onclick="toggleCurrentStreamEnable()" style="background:#1D4ED8;">Disable</button>
                  <button class="btn-pink-danger btn-sm" onclick="reloadStream(EDITING_NAME)">&#8635; Restart</button>
                </div>
                <div class="sp-mode-group">
                  <button class="sp-mode-btn active" id="sp_m_stream" onclick="setEditorMode('stream')">&#10004; Static</button>
                  <button class="sp-mode-btn" id="sp_m_ondemand" onclick="setEditorMode('ondemand')">&#9711; On Demand</button>
                </div>
                <input type="hidden" id="f_mode" value="stream">
                <input type="checkbox" id="f_enabled" checked style="display:none;">
              </div>

              <div class="player-checkerboard">
                <video id="sp_overview_video" controls autoplay muted style="width:100%; height:100%; object-fit:contain;"></video>
              </div>

              <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8.6px;">
                <div class="player-proto-tabs">
                  <button class="player-proto-tab active" id="ov_p_hls" onclick="playOverviewStream('hls')">HLS</button>
                  <button class="player-proto-tab" id="ov_p_mse" onclick="playOverviewStream('mse')">MSE</button>
                  
                  <button class="player-proto-tab" id="ov_p_dvr" onclick="playOverviewStream('dvr')">DVR</button>
                </div>
                <a class="more-link" onclick="switchStreamPageTab('output')" id="i18n_embed_link_lbl">Embed HTML player on your website</a>
              </div>
            </div>

            <!-- Правая колонка: статус, Input/Output media info и живой график потока (Скриншот 1) -->
            <div>
              <div class="sp-card sp-subcard">
                <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8.6px;">
                  <span class="sp-pill-online" id="sp_ov_status_pill">Online</span>
                  <div style="display:flex; align-items:center; gap:13px; font-size: 13.4px; font-weight:700;">
                    <span>&#127916; <span id="sp_ov_res">FHD</span></span>
                    <span>&#128202; <span id="sp_ov_br">0 kbit/s</span></span>
                    <span>&#128336; <span id="sp_ov_up">0m</span></span>
                  </div>
                </div>

                <div class="media-info-grid">
                  <div class="media-info-subcard">
                    <div class="mi-title">&#10142; Input media info</div>
                    <div id="sp_mi_in_box"><div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div></div>
                  </div>
                  <div class="media-info-subcard">
                    <div class="mi-title">&#11013; Output media info</div>
                    <div id="sp_mi_out_box"><div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div></div>
                  </div>
                </div>
              </div>

              <div class="sp-card sp-subcard">
                <div style="display:flex; align-items:center; justify-content:center; gap:14.4px; margin-bottom:7.2px; font-size: 12.5px; font-weight:700;">
                  <span><i class="leg-dot" style="background:#ef4444;"></i>In</span>
                  <span><i class="leg-dot" style="background:#10b981;"></i>Out</span>
                  <span><i class="leg-dot" style="background:#2563eb;"></i>Clients</span>
                  <span><i class="leg-dot" style="background:#6366f1;"></i>media_push</span>
                </div>
                <canvas id="canvas_stream_ov" style="width:100%; height:140.4px; display:block;"></canvas>
              </div>
            </div>
          </div>
        </div>

        <!-- ВКЛАДКА 2: INPUT С ВЫБОРОМ USER-AGENT (Скриншот 2) -->
        <div id="sp_pane_input" style="display:none;">
          <div class="sp-card">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8.6px;">
              <span class="info-badge">i</span>
              <span style="font-size: 12.1px; opacity:0.75;" id="i18n_inp_prio_hint">Источники проверяются сверху вниз по порядку приоритета (Failover)</span>
            </div>

            <!-- Динамический список URL 1, URL 2... со статусом online, стрелками, Options и корзиной -->
            <div id="sp_inputs_list_box"></div>

            <!-- Поле добавления New URL как на скриншоте 2 -->
            <div style="display:flex; align-items:center; gap:10.1px; padding:13px 0 7.2px 63.4px;">
              <input type="text" id="sp_new_url_inp" class="sp-line-input" placeholder="New URL (http://, https://, hls://, tshttp://, udp://, rtsp://) — нажмите Enter или + Добавить" onkeydown="if(event.key==='Enter'){event.preventDefault();addNewInputUrlRow();}">
              <button class="btn btn-outline btn-sm" onclick="addNewInputUrlRow()">+ Add URL</button>
            </div>

            <!-- НАСТРОЙКА USER-AGENT ДЛЯ КАЖДОГО ПОТОКА -->
            <div style="margin-top:18.7px; padding:13px 14.4px; border-radius:10.1px; border:1.5px solid rgba(59,130,246,0.35); background:rgba(59,130,246,0.05);">
              <div style="font-size: 13.8px; font-weight:800; margin-bottom:8.6px; display:flex; align-items:center; gap:5.8px;">
                <span>&#127760; User-Agent (Заголовок подключения к источнику для этого потока)</span>
              </div>
              <div style="display:grid; grid-template-columns: 1fr 1.6fr; gap:11.5px; align-items:end;">
                <div>
                  <label style="font-size: 11.7px; opacity:0.75; font-weight:700;">Готовый профиль (Preset)</label>
                  <select id="f_ua_preset" class="form-input" onchange="applyUaPreset(this.value)">
                    <option value="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36">Chrome 124 (Windows 10) — По умолчанию</option>
                    <option value="VLC/3.0.20 LibVLC/3.0.20">VLC Media Player (3.0.20)</option>
                    <option value="TiviMate/4.7.0 (Linux; Android 11) ExoPlayerLib/2.18.1">TiviMate / ExoPlayer (Android TV)</option>
                    <option value="Lavf/60.16.100">FFmpeg / Lavf</option>
                    <option value="Flussonic">Flussonic Media Server</option>
                    <option value="SmartTV/1.0 (Tizen 6.0; Samsung)">Samsung Smart TV</option>
                    <option value="custom">Свой User-Agent (ввести вручную справа)...</option>
                  </select>
                </div>
                <div>
                  <label style="font-size: 11.7px; opacity:0.75; font-weight:700;">Активный User-Agent потока</label>
                  <input type="text" id="f_user_agent" class="form-input" placeholder="Mozilla/5.0 ...">
                </div>
              </div>
            </div>

            <!-- Поля со скриншота 2: Publication, Fallback, Timeouts, DVB Subtitles -->
            <div style="margin-top:18.7px;">
              <div style="font-size: 13px; font-weight:700; margin-bottom:5.8px;">
                Publication <span style="opacity:0.6; font-weight:500;">Allow to publish to the stream</span>
              </div>
              <div style="display:flex; gap:15.8px; font-size: 13px;">
                <label style="cursor:pointer;"><input type="radio" name="sp_pub" value="enabled"> enabled</label>
                <label style="cursor:pointer;"><input type="radio" name="sp_pub" value="disabled" checked> disabled</label>
              </div>
            </div>

            <div style="margin-top:17.3px;">
              <input type="text" id="f_backup" class="sp-line-input" placeholder="Fallback video: if all your sources become dead, an mp4 file on this VOD/path will be played">
            </div>

            <div style="display:grid; grid-template-columns: 1fr 1fr 1fr auto auto; gap:14.4px; align-items:center; margin-top:17.3px;">
              <div>
                <label style="font-size: 11.2px; opacity:0.65;">Timeout (sec)</label>
                <input type="number" id="f_source_timeout" class="sp-line-input" value="30">
              </div>
              <div>
                <label style="font-size: 11.2px; opacity:0.65;">On-demand Idle Timeout (sec)</label>
                <input type="number" id="f_clients_timeout" class="sp-line-input" value="45">
              </div>
              <div>
                <label style="font-size: 11.2px; opacity:0.65;">Audio timeout</label>
                <input type="number" class="sp-line-input" placeholder="auto">
              </div>
              <label style="font-size: 13px; display:flex; align-items:center; gap:4.3px; cursor:pointer;">
                <input type="checkbox" id="sp_chk_dvr_quick" onchange="document.getElementById('f_dvr_hours').value = this.checked ? 24 : 0"> DVR
              </label>
            </div>

            <div style="margin-top:18.7px;">
              <div style="font-size: 13px; font-weight:700; margin-bottom:5.8px;">Convert DVB subtitles to WebVTT</div>
              <div style="display:flex; gap:15.8px; font-size: 13px;">
                <label style="cursor:pointer;"><input type="radio" name="sp_sub" checked> disable</label>
                <label style="cursor:pointer;"><input type="radio" name="sp_sub"> add</label>
                <label style="cursor:pointer;"><input type="radio" name="sp_sub"> replace</label>
              </div>
            </div>
          </div>
        </div>

        <!-- ВКЛАДКА 3: DVR -->
        <div id="sp_pane_dvr" style="display:none;">
          <div class="sp-card">
            <div class="form-row">
              <label id="i18n_f_dvr_lbl">Глубина архива DVR в часах (0 = архив на диск выключен)</label>
              <input type="number" id="f_dvr_hours" class="form-input" value="0">
            </div>
          </div>
        </div>

        <!-- ВКЛАДКА 4: OUTPUT (Точно как на скриншоте 3) -->
        <div id="sp_pane_output" style="display:none;">
          <div class="sp-card">
            <div class="out-grid-caption">Apple HLS standard URL. All extra tracks in distinct playlists</div>
            <div class="out-grid-row">
              <div class="eo-switch">
                <button class="eo-btn" onclick="toggleEoBtn(this, 'except')">Except</button>
                <button class="eo-btn active-only" onclick="toggleEoBtn(this, 'only')">Only</button>
              </div>
              <div class="out-proto-box">
                <span class="proto-green-tag">&#10004; HLS</span>
                <input id="mo_hls" class="out-proto-url" readonly>
                <button type="button" class="out-copy-btn" onclick="copyText('mo_hls')">&#128203; <span class="i18n-copy-btn">Копировать</span></button>
              </div>
            </div>

            <div class="out-grid-caption">Non-Apple devices standard URL. All tracks in a single playlist</div>
            <div class="out-grid-row">
              <div class="eo-switch">
                <button class="eo-btn" onclick="toggleEoBtn(this, 'except')">Except</button>
                <button class="eo-btn active-only" onclick="toggleEoBtn(this, 'only')">Only</button>
              </div>
              <div class="out-proto-box">
                <span class="proto-green-tag">&#10004; HLS</span>
                <input id="mo_hls_video" class="out-proto-url" readonly>
                <button type="button" class="out-copy-btn" onclick="copyText('mo_hls_video')">&#128203; <span class="i18n-copy-btn">Копировать</span></button>
              </div>
            </div>

            <div class="out-grid-row">
              <div class="eo-switch">
                <button class="eo-btn" onclick="toggleEoBtn(this, 'except')">Except</button>
                <button class="eo-btn active-only" onclick="toggleEoBtn(this, 'only')">Only</button>
              </div>
              <div class="out-proto-box">
                <span class="proto-green-tag">&#10004; LL-HLS</span>
                <input id="mo_llhls" class="out-proto-url" readonly>
                <button type="button" class="out-copy-btn" onclick="copyText('mo_llhls')">&#128203; <span class="i18n-copy-btn">Копировать</span></button>
              </div>
            </div>

            <div class="out-grid-row">
              <div class="eo-switch">
                <button class="eo-btn active-except" onclick="toggleEoBtn(this, 'except')">Except</button>
                <button class="eo-btn" onclick="toggleEoBtn(this, 'only')">Only</button>
              </div>
              <div class="out-proto-box">
                <span class="proto-green-tag">&#10004; EMBED</span>
                <input id="mo_embed" class="out-proto-url" readonly>
                <button type="button" class="out-copy-btn" onclick="copyText('mo_embed')">&#128203; <span class="i18n-copy-btn">Копировать</span></button>
              </div>
            </div>

            <div class="out-grid-row">
              <div class="eo-switch">
                <button class="eo-btn" onclick="toggleEoBtn(this, 'except')">Except</button>
                <button class="eo-btn active-only" onclick="toggleEoBtn(this, 'only')">Only</button>
              </div>
              <div class="out-proto-box">
                <span class="proto-green-tag">&#10004; MPEG-TS</span>
                <input id="mo_ts" class="out-proto-url" readonly>
                <button type="button" class="out-copy-btn" onclick="copyText('mo_ts')">&#128203; <span class="i18n-copy-btn">Копировать</span></button>
              </div>
            </div>



            <hr style="margin:15.8px 0; border:none; border-top:1px solid rgba(148,163,184,0.3);">
            <div class="form-row">
              <label id="i18n_f_push_lbl">Push live video / SRT Server (например: srt://62.60.250.90:54333, udp://..., rtmp://...)</label>
              <textarea id="f_pushes" class="form-input" style="height:61.2px; font-family:monospace;" placeholder="srt://62.60.250.90:54333"></textarea>
            </div>
          </div>
        </div>

        <div id="sp_pane_auth" style="display:none;">
          <div class="sp-card">
            <div class="form-row">
              <label id="i18n_f_tok_lbl">Static Security Token (?token=...)</label>
              <input type="text" id="f_token" class="form-input">
            </div>
            <div class="form-row">
              <label>HTTP Auth Backend (on_play URL)</label>
              <input type="text" id="f_on_play" class="form-input">
            </div>
          </div>
        </div>

        <!-- ВКЛАДКА 6: PLAY SESSIONS ЭТОГО ПОТОКА -->
        <div id="sp_pane_sessions" style="display:none;">
          <div class="sp-card">
            <table class="streams-table">
              <thead>
                <tr><th>IP</th><th>Protocol</th><th>User-Agent</th><th>Duration</th><th>MB</th><th>Bitrate</th><th>Action</th></tr>
              </thead>
              <tbody id="sp_single_sessions_tbody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- Нижняя панель кнопок Delete Stream и Save (как на скриншотах 1, 2, 3) -->
      <div class="sp-footer-bar">
        <div style="display:flex; gap:8.6px;">
          <button class="btn-pink-danger" id="btn_modal_delete" onclick="deleteStreamModal()">Delete Stream</button>
          <button class="btn" onclick="saveStreamModal()" id="i18n_save_btn">Save</button>
        </div>
        <button class="btn btn-outline" onclick="showSection('streams')" id="i18n_cancel_btn">&larr; Back to Streams</button>
      </div>
    </div>

    <!-- ==================== 3. PULSE (3 ГРАФИКА В 1 РЯД) ==================== -->
    <div id="sec_pulse" style="display:none;">
      <div class="glass-panel" style="margin-bottom:13px;">
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8.6px; margin-bottom:5.8px;">
          <h3 style="font-size: 19px;" id="i18n_pulse_head">Pulse — Мониторинг нагрузки и трафика StreamFlow Aria</h3>
          <div style="display:flex; gap:13px; font-size: 13px; font-weight:700;">
            <span>CPU: <b id="pu_cpu" style="color:#3b82f6;">0%</b></span>
            <span>RAM: <b id="pu_ram" style="color:#8b5cf6;">0%</b></span>
            <span>RAM Buffer (/dev/shm): <b id="pu_shm" style="color:#10b981;">0 MB</b></span>
            <span>DVR Disk: <b id="pu_disk">0%</b></span>
          </div>
        </div>
        <div class="pulse-row-3">
          <div class="glass-panel pulse-chart-card">
            <div class="pulse-chart-head">
              <div class="pulse-chart-title" id="i18n_ch_net_title">Сетевой трафик (IN &amp; OUT)</div>
              <div class="pulse-legend">
                <span><i class="leg-dot" style="background:#3b82f6;"></i><span id="i18n_leg_in">Вход (IN)</span>: <b id="chart_lbl_in">0.00</b></span>
                <span><i class="leg-dot" style="background:#10b981;"></i><span id="i18n_leg_out">Выход (OUT)</span>: <b id="chart_lbl_out">0.00</b></span>
              </div>
            </div>
            <canvas id="canvas_net" class="pulse-canvas" height="180"></canvas>
          </div>
          <div class="glass-panel pulse-chart-card">
            <div class="pulse-chart-head">
              <div class="pulse-chart-title" id="i18n_ch_cpu_title">Нагрузка на процессор (CPU)</div>
              <div class="pulse-legend">
                <span><i class="leg-dot" style="background:#6366f1;"></i>CPU: <b id="chart_lbl_cpu">0%</b></span>
              </div>
            </div>
            <canvas id="canvas_cpu" class="pulse-canvas" height="180"></canvas>
          </div>
          <div class="glass-panel pulse-chart-card">
            <div class="pulse-chart-head">
              <div class="pulse-chart-title" id="i18n_ch_ram_title">Память RAM и буфер (/dev/shm)</div>
              <div class="pulse-legend">
                <span><i class="leg-dot" style="background:#8b5cf6;"></i>RAM: <b id="chart_lbl_ram">0%</b></span>
                <span><i class="leg-dot" style="background:#10b981;"></i><span id="i18n_leg_buf">Буфер</span>: <b id="chart_lbl_shm">0 MB</b></span>
              </div>
            </div>
            <canvas id="canvas_ram" class="pulse-canvas" height="180"></canvas>
          </div>
        </div>
      </div>
    </div>

    <div id="sec_sessions" style="display:none;">
      <div class="glass-panel">
        <h3 style="margin-bottom:11.5px; font-size: 18.1px;" id="i18n_sess_head">Активные сессии просмотра (Play Sessions)</h3>
        <table class="streams-table">
          <thead>
            <tr>
              <th id="th_s_stream">Канал</th><th id="th_s_ip">IP клиента</th><th id="th_s_proto">Протокол</th>
              <th>User-Agent</th><th id="th_s_dur">Время</th><th id="th_s_mb">Отдано МБ</th>
              <th id="th_s_br">Битрейт</th><th id="th_s_act">Действие</th>
            </tr>
          </thead>
          <tbody id="sessions_tbody"></tbody>
        </table>
      </div>
    </div>

    <div id="sec_config" style="display:none;">

      <div class="glass-panel" style="margin-bottom:13px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10.1px;">
          <h3 style="font-size: 18.1px;">Listeners & Access (Порты и доступ)</h3>
          <button class="btn" onclick="saveGuiConf()">Сохранить настройки</button>
        </div>
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:14.4px;">
          <div>
            <label style="font-weight:700;display:block;margin-bottom:5.8px;">HTTP Port</label>
            <input type="number" id="gui_port" class="form-input">
          </div>
          <div>
            <label style="font-weight:700;display:block;margin-bottom:5.8px;">Admin UI username</label>
            <input type="text" id="gui_user" class="form-input" style="margin-bottom:8.6px;">
            <label style="font-weight:700;display:block;margin-bottom:5.8px;">Admin UI password</label>
            <input type="password" id="gui_pass" class="form-input" placeholder="(оставьте пустым для сохранения старого)">
          </div>
        </div>
      </div>
      <div class="glass-panel">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10.1px;">
          <h3 style="font-size: 18.1px;">/etc/flustream/flustream.conf</h3>
          <button class="btn" onclick="saveRawConf()" id="i18n_save_conf">Сохранить и применить</button>
        </div>
        <textarea id="raw_conf_editor" style="width:100%; height:345.6px; background:#090d18; color:#60a5fa; font-size: 13px; padding:11.5px; border-radius:8.6px; border:1px solid rgba(99,102,241,0.35);"></textarea>
      </div>
    </div>

    <div id="sec_iptv" style="display:none;">
      <div class="glass-panel">
        <h3 style="margin-bottom:11.5px; font-size: 18.1px;" id="i18n_m3u_head">Генератор плейлистов M3U (StreamFlow Aria)</h3>
        <div style="display:flex; gap:10.1px; flex-wrap:wrap;">
          <a class="btn" href="/playlist.m3u?format=hls" target="_blank" style="text-decoration:none;" id="i18n_dl_hls">Скачать M3U (HLS .m3u8)</a>
          <a class="btn btn-outline" href="/playlist.m3u?format=mpegts" target="_blank" style="text-decoration:none;" id="i18n_dl_ts">Скачать M3U (HTTP MPEG-TS)</a>
          
        </div>
      </div>
    </div>

    <div id="sec_cluster" style="display:none;">
      <div class="glass-panel">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:11.5px; flex-wrap:wrap; gap:7.2px;">
          <div>
            <h3 style="font-size: 18.1px;" id="i18n_cl_head">Объединение в кластер (Flussonic &amp; StreamFlow Aria Cluster)</h3>
            <p style="font-size: 12.5px; opacity:0.8; margin-top:2.9px;" id="i18n_cl_sub">
              Подключайте внешние серверы Flussonic или StreamFlow Aria по директивам <code>cluster_key</code>, <code>source</code> и <code>peer</code>
            </p>
          </div>
          <button class="btn" onclick="saveAndSyncCluster()" id="i18n_cl_sync_btn">&#8635; Сохранить и синхронизировать кластер</button>
        </div>
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:13px; margin-bottom:13px;">
          <div class="form-row">
            <label id="i18n_cl_key_lbl">Секретный ключ кластера (cluster_key)</label>
            <input type="text" id="cl_key" class="form-input" value="flustream_secret">
          </div>
          <div class="form-row">
            <label id="i18n_cl_ep_lbl">Ваш API-адрес для подключения другого сервера:</label>
            <input type="text" id="cl_my_endpoint" class="form-input" readonly onclick="this.select()">
          </div>
        </div>
        <div class="form-row">
          <label id="i18n_cl_src_lbl">Удаленные серверы-источники — Директива <code>source</code></label>
          <textarea id="cl_sources_hosts" class="form-input" style="height:57.6px; font-family:monospace;"></textarea>
        </div>
        <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10.1px; margin-bottom:11.5px;">
          <div class="form-row">
            <label id="i18n_cl_proto_lbl">Протокол забора потоков</label>
            <select id="cl_proto" class="form-input">
              <option value="hls" selected>HLS (/index.m3u8)</option>
              <option value="mpegts">HTTP MPEG-TS (/mpegts)</option>
            </select>
          </div>
          <div class="form-row">
            <label>Whitelist <code>only</code></label>
            <input type="text" id="cl_only" class="form-input" placeholder="*">
          </div>
          <div class="form-row">
            <label>Blacklist <code>except</code></label>
            <input type="text" id="cl_except" class="form-input" placeholder="test_stream">
          </div>
        </div>
        <div class="form-row">
          <label id="i18n_cl_peer_lbl">Соседние узлы пиринга — Директива <code>peer</code></label>
          <textarea id="cl_peers" class="form-input" style="height:39.6px; font-family:monospace;"></textarea>
        </div>
        <hr style="margin:14.4px 0; border:none; border-top:1px solid rgba(148,163,184,0.3);">
        <h3 style="font-size: 16.8px; margin-bottom:7.2px;" id="i18n_bulk_head">Быстрый массовый импорт каналов (M3U или список ссылок)</h3>
        <div style="display:grid; grid-template-columns: 2fr 1fr auto; gap:8.6px; margin-bottom:8.6px; align-items:end;">
          <div class="form-row" style="margin-bottom:0;">
            <label id="i18n_bulk_url_lbl">Ссылка на M3U / M3U8 плейлист (необязательно)</label>
            <input type="text" id="bulk_m3u_url" class="form-input" placeholder="http://ip:port/playlist.m3u8">
          </div>
          <div class="form-row" style="margin-bottom:0;">
            <label id="i18n_bulk_mode_lbl">Режим каналов</label>
            <select id="bulk_mode" class="form-input">
              <option value="ondemand" id="i18n_bulk_opt_od">По запросу (On demand)</option>
              <option value="stream" id="i18n_bulk_opt_st">Постоянный (24/7)</option>
            </select>
          </div>
          <button class="btn" onclick="runBulkImport()" id="i18n_bulk_btn">+ Импортировать каналы</button>
        </div>
        <div class="form-row">
          <label id="i18n_bulk_txt_lbl">Или вставьте список ссылок на потоки / содержимое файла .m3u:</label>
          <textarea id="bulk_m3u_text" class="form-input" style="height:86.4px; font-family:monospace;"></textarea>
        </div>
      </div>
    </div>

    <div id="sec_tester" style="display:none;">
      <div class="glass-panel">
        <h3 style="margin-bottom:10.1px; font-size: 18.1px;" id="i18n_test_head">Встроенный плеер проверки потоков (HLS .m3u8 и MPEG-DASH .mpd)</h3>
        <div style="display:flex; gap:7.2px; margin-bottom:11.5px;">
          <input type="text" id="tester_url" class="form-input" value="https://dash.akamaized.net/akamai/bbb_30fps/bbb_30fps.mpd">
          <button class="btn" onclick="runTesterPlayer()" id="i18n_test_btn">&#9654; Запустить</button>
        </div>
        <video id="tester_video" controls style="width:100%; max-height:331.2px; background:#000; border-radius:8.6px;"></video>
      </div>
    </div>
  </div>
</div>

<!-- БЫСТРЫЙ ПЛЕЕР ИЗ ТАБЛИЦЫ -->
<div class="modal-backdrop" id="player_modal">
  <div class="modal-card" style="width:532.8px;">
    <div class="modal-head">
      <strong id="play_modal_title">StreamFlow Aria Player</strong>
      <div style="display:flex; gap:5.8px; align-items:center;">
        <button class="btn btn-sm" id="pm_btn_hls" onclick="switchPopupProto('hls')">HLS (.m3u8)</button>
        
        <button class="btn btn-outline btn-sm" onclick="closePlayerModal()">&#10005;</button>
      </div>
    </div>
    <div class="modal-body" style="background:#000; padding:0;">
      <video id="popup_video" controls autoplay style="width:100%; height:302.4px; display:block;"></video>
    </div>
  </div>
</div>

<script>
const I18N = {
  ru: {
    t_streams: "Потоки",
    t_conf: "Шаблоны и конфиг",
    t_m3u: "Плейлисты M3U",
    t_sess: "Сессии зрителей",
    t_play: "Плеер потоков",
    t_pulse: "Телеметрия Pulse",
    t_cluster: "Кластер / Пиры",
    logout_lbl: "Выход",
    cap_streams: "ПОТОКИ",
    cap_clients: "ЗРИТЕЛИ",
    add_btn: "Добавить поток",
    reload_all: "Перегрузить все",
    buf_all_lbl: "Буфер всех:",
    buf_30: "30 сек",
    buf_norm: "Обычный",
    per_page: "На странице",
    of_word: "из",
    filter_ph: "Поиск по имени или адресу источника...",
    sort_name: "Сортировка: Имя",
    sort_status: "Сортировка: Статус",
    sort_br: "Сортировка: Битрейт",
    sort_cl: "Сортировка: Зрители",
    filter_lbl: "Статус:",
    th_stream: "Канал (Stream)",
    th_input: "Источник (Input)",
    th_transcode: "Буфер",
    th_dvr: "Архив DVR",
    th_output: "Выход (Output)",
    always_started: "Постоянный (Static)",
    on_demand: "По запросу (On demand)",
    buf_label: "Буфер RAM:",
    dvr_off: "Архив выключен",
    dvr_on: "Включен",
    clients_w: "Зрителей:",
    push_sum: "Отправка (Push): активно",
    more: "Ссылки",
    mbps_unit: "Мбит/с",
    kbps_unit: "кбит/с",
    pulse_head: "Pulse — Мониторинг нагрузки и трафика StreamFlow Aria",
    ch_net_title: "Сетевой трафик Интернета (IN & OUT)",
    leg_in: "Вход (IN)",
    leg_out: "Выход (OUT)",
    ch_cpu_title: "Нагрузка на процессор (CPU Load)",
    ch_ram_title: "Память RAM и буфер (/dev/shm)",
    leg_buf: "Буфер",
    sess_head: "Активные сессии просмотра (Play Sessions)",
    th_s_stream: "Канал",
    th_s_ip: "IP клиента",
    th_s_proto: "Протокол",
    th_s_dur: "Время",
    th_s_mb: "Отдано МБ",
    th_s_br: "Битрейт",
    th_s_act: "Действие",
    no_sess: "Нет активных сессий просмотра",
    m3u_head: "Генератор плейлистов M3U (StreamFlow Aria)",
    dl_hls: "Скачать M3U (HLS .m3u8)",
    dl_ts: "Скачать M3U (HTTP MPEG-TS)",
    dl_mpd: "Скачать M3U (MPEG-DASH .mpd)",
    cl_head: "Объединение в кластер (Flussonic & StreamFlow Aria Cluster)",
    cl_sub: "Подключайте внешние серверы Flussonic или StreamFlow Aria по директивам cluster_key, source и peer",
    cl_sync_btn: "↻ Сохранить и синхронизировать кластер",
    cl_key_lbl: "Секретный ключ кластера (cluster_key)",
    cl_ep_lbl: "Ваш API-адрес для подключения другого сервера:",
    cl_src_lbl: "Удаленные серверы-источники — Директива source",
    cl_proto_lbl: "Протокол забора потоков",
    cl_peer_lbl: "Соседние узлы пиринга — Директива peer",
    bulk_head: "Быстрый массовый импорт каналов (M3U или список ссылок)",
    bulk_url_lbl: "Ссылка на M3U / M3U8 плейлист (необязательно)",
    bulk_mode_lbl: "Режим каналов",
    bulk_opt_od: "По запросу (On demand)",
    bulk_opt_st: "Постоянный (24/7)",
    bulk_btn: "+ Импортировать каналы",
    bulk_txt_lbl: "Или вставьте список ссылок на потоки / содержимое файла .m3u:",
    test_head: "Встроенный плеер проверки потоков (HLS .m3u8 и MPEG-DASH .mpd)",
    test_btn: "▶ Запустить",
    sptab_overview: "Overview (Обзор)",
    sptab_input: "Input (Вход)",
    sptab_dvr: "DVR (Архив)",
    sptab_output: "Output (Выход)",
    sptab_auth: "Auth (Защита)",
    sptab_sessions: "Play sessions (Сессии)",
    save_conf: "Сохранить и применить",
    cancel_btn: "← Назад к потокам",
    save_btn: "Сохранить (Save)",
    reloading: "Перезагрузка потока",
    copied: "Ссылка скопирована!",
    copy_btn: "Копировать",
    toast_buf_norm: "Включен обычный буфер (6 сек)",
    toast_buf_30: "Включен буфер 30 сек", tbl_up: "Время работы", tbl_br: "Битрейт", btn_disable: "Выключить", btn_enable: "Включить", lbl_name: "Имя (ID)", lbl_title: "Название", lbl_desc: "Описание", lbl_qual: "Профиль качества", lbl_ram: "RAM Буфер"
  },
  en: {
    t_streams: "Streams",
    t_conf: "Templates & Config",
    t_m3u: "M3U Playlists",
    t_sess: "Play Sessions",
    t_play: "Stream Player",
    t_pulse: "Pulse Telemetry",
    t_cluster: "Cluster / Peers",
    logout_lbl: "Logout",
    cap_streams: "STREAMS",
    cap_clients: "CLIENTS",
    add_btn: "Add Stream",
    reload_all: "Reload All",
    buf_all_lbl: "All Buffer:",
    buf_30: "30s",
    buf_norm: "Normal",
    per_page: "Per page",
    of_word: "of",
    filter_ph: "Filter by stream name or input URL...",
    sort_name: "Sort By: Name",
    sort_status: "Sort By: Status",
    sort_br: "Sort By: Bitrate",
    sort_cl: "Sort By: Clients",
    filter_lbl: "Status:",
    th_stream: "Stream",
    th_input: "Input",
    th_transcode: "Buffer",
    th_dvr: "DVR Archive",
    th_output: "Output",
    always_started: "Always started (Static)",
    on_demand: "On demand",
    buf_label: "RAM Buffer:",
    dvr_off: "Archive disabled",
    dvr_on: "Enabled",
    clients_w: "Clients watching:",
    push_sum: "Push summary: running",
    more: "Links",
    mbps_unit: "Mbit/s",
    kbps_unit: "kbit/s",
    pulse_head: "Pulse — StreamFlow Aria Load & Traffic Telemetry",
    ch_net_title: "Internet Traffic (IN & OUT Realtime)",
    leg_in: "Inbound (IN)",
    leg_out: "Outbound (OUT)",
    ch_cpu_title: "Processor Load (CPU %)",
    ch_ram_title: "RAM Usage & Buffer (/dev/shm)",
    leg_buf: "Buffer",
    sess_head: "Active Viewers (Play Sessions)",
    th_s_stream: "Stream",
    th_s_ip: "Client IP",
    th_s_proto: "Protocol",
    th_s_dur: "Duration",
    th_s_mb: "Sent MB",
    th_s_br: "Bitrate",
    th_s_act: "Action",
    no_sess: "No active play sessions",
    m3u_head: "M3U Playlist Generator (StreamFlow Aria)",
    dl_hls: "Download M3U (HLS .m3u8)",
    dl_ts: "Download M3U (HTTP MPEG-TS)",
    dl_mpd: "Download M3U (MPEG-DASH .mpd)",
    cl_head: "Cluster Federation (Flussonic & StreamFlow Aria Cluster)",
    cl_sub: "Connect external Flussonic or StreamFlow Aria servers via cluster_key, source, and peer directives",
    cl_sync_btn: "↻ Save & Synchronize Cluster",
    cl_key_lbl: "Cluster Secret Key (cluster_key)",
    cl_ep_lbl: "Your API Endpoint for Remote Peers:",
    cl_src_lbl: "Remote Source Servers — Directive source",
    cl_proto_lbl: "Stream Fetch Protocol",
    cl_peer_lbl: "Neighbor Peering Nodes — Directive peer",
    bulk_head: "Fast Bulk Channel Import (M3U or URL List)",
    bulk_url_lbl: "Direct M3U / M3U8 Playlist URL (optional)",
    bulk_mode_lbl: "Channel Mode",
    bulk_opt_od: "On demand",
    bulk_opt_st: "Always started (24/7)",
    bulk_btn: "+ Import Channels",
    bulk_txt_lbl: "Or paste stream URLs / .m3u file content here:",
    test_head: "Built-in Stream Tester (HLS .m3u8 & MPEG-DASH .mpd)",
    test_btn: "▶ Play Stream",
    sptab_overview: "Overview",
    sptab_input: "Input",
    sptab_dvr: "DVR",
    sptab_output: "Output",
    sptab_auth: "Auth",
    sptab_sessions: "Play sessions",
    save_conf: "Save & Apply",
    cancel_btn: "← Back to Streams",
    save_btn: "Save",
    reloading: "Reloading stream",
    copied: "Copied to clipboard!",
    copy_btn: "Copy",
    toast_buf_norm: "Normal buffer enabled (6s)",
    toast_buf_30: "30s buffer enabled", tbl_up: "Uptime", tbl_br: "Bitrate", btn_disable: "Disable", btn_enable: "Enable", lbl_name: "Name (ID)", lbl_title: "Title", lbl_desc: "Description", lbl_qual: "Quality Profile", lbl_ram: "RAM Buffer"
  }
};

let STATE = { streams: [], sessions: [], pulse_history: [] };
let STATUS_FILTER = null;
let CURRENT_SECTION = 'streams';
let EDITING_NAME = null;
let CURRENT_SP_TAB = 'overview';
let EDIT_INPUTS_ARR = [];
let STREAM_OV_HISTORY = {};
let PLAYING_NAME = null;
let OPEN_MORE_SET = new Set();
let ovHls = null, ovDash = null, popHls = null, popDash = null, testHls = null, testDash = null;
const NL = String.fromCharCode(10);

let APP_THEME = localStorage.getItem('flustream_theme_id') || '__DEFAULT_THEME__';
if (APP_THEME !== '2' && APP_THEME !== '4') APP_THEME = '2';
let APP_LANG = localStorage.getItem('flustream_lang') || '__DEFAULT_LANG__';
if (APP_LANG !== 'ru' && APP_LANG !== 'en') APP_LANG = 'ru';

function t(key) {
  return (I18N[APP_LANG] && I18N[APP_LANG][key]) || I18N.ru[key] || key;
}

function setElText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function setAppTheme(themeId, syncServer = true) {
  APP_THEME = (themeId === '4') ? '4' : '2';
  document.body.classList.remove('theme-2', 'theme-4');
  document.body.classList.add('theme-' + APP_THEME);
  document.getElementById('btn_theme_2').classList.toggle('active', APP_THEME === '2');
  document.getElementById('btn_theme_4').classList.toggle('active', APP_THEME === '4');
  localStorage.setItem('flustream_theme_id', APP_THEME);
  renderPulseCharts();
  renderStreamOverviewChart();
  if (syncServer) {
    fetch('/api/v3/ui_prefs', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({theme: APP_THEME, lang: APP_LANG})
    }).catch(()=>{});
  }
}


function setAppLang(langId, syncServer = true) {
  APP_LANG = (langId === 'en') ? 'en' : 'ru';
  document.getElementById('btn_lang_ru').classList.toggle('active', APP_LANG === 'ru');
  document.getElementById('btn_lang_en').classList.toggle('active', APP_LANG === 'en');
  localStorage.setItem('flustream_lang', APP_LANG);

  const mapIds = [
    ['i18n_add_btn', 'add_btn'],
    ['tab_nav_streams', 't_streams'],
    ['tab_nav_config', 't_conf'],
    ['tab_nav_iptv', 't_m3u'],
    ['tab_nav_sessions', 't_sess'],
    ['tab_nav_tester', 't_play'],
    ['tab_nav_pulse', 't_pulse'],
    ['tab_nav_cluster', 't_cluster'],
    ['i18n_logout_lbl', 'logout_lbl'],
    ['cap_lbl_streams', 'cap_streams'],
    ['cap_lbl_clients', 'cap_clients'],
    ['i18n_reload_all', 'reload_all'],
    ['i18n_buf_all_lbl', 'buf_all_lbl'],
    ['i18n_buf_all_30', 'buf_30'],
    ['i18n_buf_all_norm', 'buf_norm'],
    ['i18n_per_page', 'per_page'],
    ['i18n_sort_name', 'sort_name'],
    ['i18n_sort_status', 'sort_status'],
    ['i18n_sort_br', 'sort_br'],
    ['i18n_sort_cl', 'sort_cl'],
    ['i18n_filter_lbl', 'filter_lbl'],
    ['th_stream', 'th_stream'],
    ['th_input', 'th_input'],
    ['th_transcode', 'th_transcode'],
    ['th_dvr', 'th_dvr'],
    ['th_output', 'th_output'],
    ['i18n_pulse_head', 'pulse_head'],
    ['i18n_ch_net_title', 'ch_net_title'],
    ['i18n_leg_in', 'leg_in'],
    ['i18n_leg_out', 'leg_out'],
    ['i18n_ch_cpu_title', 'ch_cpu_title'],
    ['i18n_ch_ram_title', 'ch_ram_title'],
    ['i18n_leg_buf', 'leg_buf'],
    ['i18n_sess_head', 'sess_head'],
    ['th_s_stream', 'th_s_stream'],
    ['th_s_ip', 'th_s_ip'],
    ['th_s_proto', 'th_s_proto'],
    ['th_s_dur', 'th_s_dur'],
    ['th_s_mb', 'th_s_mb'],
    ['th_s_br', 'th_s_br'],
    ['th_s_act', 'th_s_act'],
    ['i18n_m3u_head', 'm3u_head'],
    ['i18n_dl_hls', 'dl_hls'],
    ['i18n_dl_ts', 'dl_ts'],
    ['i18n_dl_mpd', 'dl_mpd'],
    ['i18n_cl_head', 'cl_head'],
    ['i18n_cl_sub', 'cl_sub'],
    ['i18n_cl_sync_btn', 'cl_sync_btn'],
    ['i18n_cl_key_lbl', 'cl_key_lbl'],
    ['i18n_cl_ep_lbl', 'cl_ep_lbl'],
    ['i18n_cl_src_lbl', 'cl_src_lbl'],
    ['i18n_cl_proto_lbl', 'cl_proto_lbl'],
    ['i18n_cl_peer_lbl', 'cl_peer_lbl'],
    ['i18n_bulk_head', 'bulk_head'],
    ['i18n_bulk_url_lbl', 'bulk_url_lbl'],
    ['i18n_bulk_mode_lbl', 'bulk_mode_lbl'],
    ['i18n_bulk_opt_od', 'bulk_opt_od'],
    ['i18n_bulk_opt_st', 'bulk_opt_st'],
    ['i18n_bulk_btn', 'bulk_btn'],
    ['i18n_bulk_txt_lbl', 'bulk_txt_lbl'],
    ['i18n_test_head', 'test_head'],
    ['i18n_test_btn', 'test_btn'],
    ['sptab_overview', 'sptab_overview'],
    ['sptab_input', 'sptab_input'],
    ['sptab_dvr', 'sptab_dvr'],
    ['sptab_output', 'sptab_output'],
    ['sptab_auth', 'sptab_auth'],
    ['sptab_sessions', 'sptab_sessions'],
    ['i18n_save_conf', 'save_conf'],
    ['i18n_cancel_btn', 'cancel_btn'],
    ['i18n_save_btn', 'save_btn'],
    ['i18n_lbl_name', 'lbl_name'],
    ['i18n_lbl_title', 'lbl_title'],
    ['i18n_lbl_desc', 'lbl_desc'],
    ['i18n_f_qual_lbl', 'lbl_qual'],
    ['i18n_lbl_ram', 'lbl_ram']
  ];
  mapIds.forEach(([id, k]) => setElText(id, t(k)));
  document.getElementById('text_filter').placeholder = t('filter_ph');
  document.querySelectorAll('.copy-btn-lbl').forEach(el => el.textContent = (APP_LANG === 'en' ? 'Copy' : 'Копировать'));
  document.querySelectorAll('.i18n-copy-btn').forEach(el => { el.textContent = t('copy_btn'); });

  renderStreamsTable();
  renderSessionsTable();
  renderPulseCharts();
  updateTopHeaderBreadcrumb();
  if (syncServer) {
    fetch('/api/v3/ui_prefs', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({theme: APP_THEME, lang: APP_LANG})
    }).catch(()=>{});
  }
}

function toast(msg) {
  const el = document.getElementById('toast_box');
  el.textContent = msg;
  el.style.display = 'block';
  setTimeout(() => { el.style.display = 'none'; }, 2600);
}

function fmtUpCapsule(sec) {
  const d = Math.floor(sec / 86400);
  const h = String(Math.floor((sec % 86400) / 3600)).padStart(2, '0');
  const m = String(Math.floor((sec % 3600) / 60)).padStart(2, '0');
  const s = String(sec % 60).padStart(2, '0');
  return `${d}D ${h}:${m}:${s}`;
}

function fmtStreamUptime(sec) {
  if (!sec || sec <= 0) return '0m';
  const mo = Math.floor(sec / (86400 * 30));
  const d = Math.floor((sec % (86400 * 30)) / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (mo > 0) return `${mo}mo ${d}d`;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m ${sec % 60}s`;
}

function stopOverviewPlayer() {
  try { if (ovHls) { ovHls.destroy(); ovHls = null; } } catch(e){}
  try { if (ovDash) { ovDash.reset(); ovDash = null; } } catch(e){}
  const v = document.getElementById('sp_overview_video');
  if (v) v.pause();
}

function updateTopHeaderBreadcrumb() {
  const h = document.getElementById('main_header_title');
  if (CURRENT_SECTION === 'stream_page') {
    const sName = document.getElementById('f_name').value || EDITING_NAME || 'New Stream';
    const tabTitle = CURRENT_SP_TAB.charAt(0).toUpperCase() + CURRENT_SP_TAB.slice(1);
    h.innerHTML = `<span class="bc-crumb"><span class="bc-link" onclick="showSection('streams')">Streams</span> <span class="bc-sep">&gt;</span> <span class="bc-link" onclick="switchStreamPageTab('overview')">${sName}</span> ${CURRENT_SP_TAB !== 'overview' ? `<span class="bc-sep">&gt;</span> <span>${tabTitle}</span>` : ''}</span>`;
  } else {
    h.innerHTML = `StreamFlow <span class="grad">Aria</span>`;
  }
}

function showSection(sec) {
  if (sec !== 'stream_page') {
    stopOverviewPlayer();
  }
  CURRENT_SECTION = sec;
  ['streams','stream_page','pulse','sessions','config','iptv','tester','cluster'].forEach(s => {
    const box = document.getElementById('sec_' + s);
    if (box) box.style.display = (s === sec) ? ((s === 'streams' || s === 'stream_page') ? 'flex' : 'block') : 'none';
    const ico = document.getElementById('nav_ico_' + s);
    if (ico) ico.classList.toggle('active', s === sec || (s === 'streams' && sec === 'stream_page'));
  });
  updateTopHeaderBreadcrumb();
  if (sec === 'pulse') {
    setTimeout(renderPulseCharts, 40);
  }
  if (sec === 'config') {
    document.getElementById('raw_conf_editor').value = STATE.raw_conf || '';
    document.getElementById('gui_port').value = STATE.http_port || 8080;
    document.getElementById('gui_user').value = STATE.admin_user || 'admin';
  }
  if (sec === 'cluster') {
    document.getElementById('cl_key').value = STATE.cluster_key || 'flustream_secret';
    document.getElementById('cl_my_endpoint').value = window.location.origin + '/streamer/api/v3/streams';
    document.getElementById('cl_peers').value = (STATE.peers || []).join(NL);
    const srcs = STATE.cluster_sources || [];
    document.getElementById('cl_sources_hosts').value = srcs.map(x => x.host).join(NL);
    if (srcs.length > 0) {
      document.getElementById('cl_proto').value = srcs[0].proto || 'hls';
      document.getElementById('cl_only').value = srcs[0].only || '';
      document.getElementById('cl_except').value = srcs[0].except || '';
    }
  }
}

async function pollStatus() {
  try {
    const r = await fetch('/api/v3/status');
    if (r.status === 401) { window.location.reload(); return; }
    STATE = await r.json();

    const onlineCnt = STATE.streams.filter(s => s.runtime.status === 'Online').length;
    const waitingCnt = STATE.streams.filter(s => s.runtime.status === 'Waiting').length;
    const disabledCnt = STATE.streams.filter(s => s.runtime.status === 'Disabled').length;
    const errorCnt = STATE.streams.filter(s => s.runtime.status === 'Error').length;

    document.getElementById('cap_streams').textContent = `${onlineCnt} / ${STATE.streams.length}`;
    document.getElementById('cap_clients').textContent = STATE.total_clients || 0;
    document.getElementById('cap_in').textContent = `${Number(STATE.total_in_mbps || 0).toFixed(2)} MBPS`;
    document.getElementById('cap_out').textContent = `${Number(STATE.total_out_mbps || 0).toFixed(2)} MBPS`;
    document.getElementById('cap_up').textContent = fmtUpCapsule(STATE.uptime_sec || 0);

    document.getElementById('cnt_Online').textContent = onlineCnt;
    document.getElementById('cnt_Waiting').textContent = waitingCnt;
    document.getElementById('cnt_Disabled').textContent = disabledCnt;
    document.getElementById('cnt_Error').textContent = errorCnt;

    document.getElementById('pu_cpu').textContent = `${STATE.cpu_percent}%`;
    document.getElementById('pu_ram').textContent = `${STATE.ram_percent}%`;
    document.getElementById('pu_shm').textContent = `${STATE.shm_used_mb} MB`;
    document.getElementById('pu_disk').textContent = `${STATE.dvr_disk_percent}% (Free: ${STATE.dvr_free_gb} GB)`;

    // Записываем историю для открытого потока
    const nowTime = new Date().toTimeString().slice(0, 8);
    STATE.streams.forEach(st => {
      if (!STREAM_OV_HISTORY[st.name]) STREAM_OV_HISTORY[st.name] = [];
      const arr = STREAM_OV_HISTORY[st.name];
      arr.push({
        ts: nowTime,
        in_mb: Number(((st.runtime.bitrate_kbps || 0) / 1000).toFixed(2)),
        out_mb: Number((((st.runtime.bitrate_kbps || 0) * (st.runtime.clients || 0)) / 1000).toFixed(2)),
        clients: st.runtime.clients || 0,
        push: st.runtime.pushes_running || 0
      });
      if (arr.length > 30) arr.shift();
    });

    renderStreamsTable(true);
    renderSessionsTable();
    renderPulseCharts();
    if (CURRENT_SECTION === 'stream_page' && EDITING_NAME) {
      updateStreamPageLiveStats();
    }
  } catch (e) {}
}

function toggleStatusFilter(st) {
  STATUS_FILTER = (STATUS_FILTER === st) ? null : st;
  ['Online','Waiting','Disabled','Error'].forEach(k => {
    document.getElementById('f_pill_' + k).classList.toggle('active', STATUS_FILTER === k);
  });
  renderStreamsTable();
}

function syncPerPage(val) {
  document.getElementById('per_page_top').value = val;
  renderStreamsTable();
}

function toggleMoreBox(name) {
  if (OPEN_MORE_SET.has(name)) OPEN_MORE_SET.delete(name);
  else OPEN_MORE_SET.add(name);
  renderStreamsTable();
}

async function setStreamBufferMode(name, mode) {
  await fetch('/api/v3/streams/buffer_mode', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name, buffer_mode: mode})
  });
  toast(mode === 'normal' ? t('toast_buf_norm') : t('toast_buf_30'));
  pollStatus();
}

function drawSmoothChart(canvasId, seriesList, maxValFixed, unitSuffix = '') {
  const c = document.getElementById(canvasId);
  if (!c) return;
  const rect = c.getBoundingClientRect();
  if (rect.width === 0) return;
  const dpr = window.devicePixelRatio || 1;
  c.width = rect.width * dpr;
  c.height = rect.height * dpr;
  const ctx = c.getContext('2d');
  ctx.scale(dpr, dpr);

  const W = rect.width, H = rect.height;
  const padL = 52, padR = 16, padT = 18, padB = 28;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const isDark = document.body.classList.contains('theme-4');

  ctx.clearRect(0, 0, W, H);

  let maxV = maxValFixed || 5;
  if (!maxValFixed) {
    seriesList.forEach(s => {
      s.data.forEach(v => { if (v > maxV) maxV = v; });
    });
    maxV = Math.max(4, Math.ceil(maxV * 1.25));
  }

  ctx.strokeStyle = isDark ? 'rgba(148, 163, 184, 0.16)' : 'rgba(15, 23, 42, 0.08)';
  ctx.fillStyle = isDark ? '#94a3b8' : '#475569';
  ctx.font = '600 8.3px Inter, sans-serif';
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 4]);
  for (let i = 0; i <= 4; i++) {
    const y = padT + (plotH * i / 4);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(W - padR, y);
    ctx.stroke();
    const valLbl = (maxV * (4 - i) / 4).toFixed(maxValFixed ? 0 : 1) + unitSuffix;
    ctx.fillText(valLbl, 4, y + 4);
  }
  ctx.setLineDash([]);

  seriesList.forEach(s => {
    const pts = s.data;
    if (!pts || pts.length === 0) return;
    const coords = pts.map((v, i) => ({
      x: padL + (pts.length === 1 ? plotW : (i / (pts.length - 1)) * plotW),
      y: padT + plotH - Math.min(1, Math.max(0, v / maxV)) * plotH
    }));

    const grad = ctx.createLinearGradient(0, padT, 0, padT + plotH);
    grad.addColorStop(0, s.fillTop);
    grad.addColorStop(1, 'rgba(0,0,0,0)');

    ctx.beginPath();
    ctx.moveTo(coords[0].x, padT + plotH);
    ctx.lineTo(coords[0].x, coords[0].y);
    for (let i = 0; i < coords.length - 1; i++) {
      const xc = (coords[i].x + coords[i + 1].x) / 2;
      ctx.bezierCurveTo(xc, coords[i].y, xc, coords[i + 1].y, coords[i + 1].x, coords[i + 1].y);
    }
    ctx.lineTo(coords[coords.length - 1].x, padT + plotH);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();

    ctx.save();
    ctx.beginPath();
    ctx.moveTo(coords[0].x, coords[0].y);
    for (let i = 0; i < coords.length - 1; i++) {
      const xc = (coords[i].x + coords[i + 1].x) / 2;
      ctx.bezierCurveTo(xc, coords[i].y, xc, coords[i + 1].y, coords[i + 1].x, coords[i + 1].y);
    }
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 2.8;
    ctx.shadowColor = s.color;
    ctx.shadowBlur = 0;
    ctx.stroke();
    ctx.restore();

    const lastPt = coords[coords.length - 1];
    ctx.beginPath();
    ctx.arc(lastPt.x, lastPt.y, 4, 0, Math.PI * 2);
    ctx.fillStyle = s.color;
    ctx.fill();
    ctx.lineWidth = 2;
    ctx.strokeStyle = '#ffffff';
    ctx.stroke();
  });
}

function renderPulseCharts() {
  const hist = STATE.pulse_history || [];
  if (!hist.length) return;
  const last = hist[hist.length - 1];

  const elIn = document.getElementById('chart_lbl_in');
  if (elIn) {
    elIn.textContent = `${last.in_mbps.toFixed(2)} ${t('mbps_unit')}`;
    document.getElementById('chart_lbl_out').textContent = `${last.out_mbps.toFixed(2)} ${t('mbps_unit')}`;
    document.getElementById('chart_lbl_cpu').textContent = `${last.cpu}%`;
    document.getElementById('chart_lbl_ram').textContent = `${last.ram}%`;
    document.getElementById('chart_lbl_shm').textContent = `${last.shm_mb} MB`;
  }

  drawSmoothChart('canvas_net', [
    { data: hist.map(x => x.in_mbps), color: '#3b82f6', fillTop: 'rgba(59, 130, 246, 0.36)' },
    { data: hist.map(x => x.out_mbps), color: '#10b981', fillTop: 'rgba(16, 185, 129, 0.30)' }
  ], null, '');

  drawSmoothChart('canvas_cpu', [
    { data: hist.map(x => x.cpu), color: '#6366f1', fillTop: 'rgba(99, 102, 241, 0.36)' }
  ], 100, '%');

  drawSmoothChart('canvas_ram', [
    { data: hist.map(x => x.ram), color: '#8b5cf6', fillTop: 'rgba(139, 92, 246, 0.34)' }
  ], 100, '%');
}

function renderStreamsTable(fromPoll = false) {
  const sel = window.getSelection ? window.getSelection() : null;
  if (fromPoll && sel && sel.toString().length > 0) return;
  const q = (document.getElementById('text_filter').value || '').toLowerCase().trim();
  const sortBy = document.getElementById('sort_by').value;
  const perPage = parseInt(document.getElementById('per_page_top').value || '100', 10);

  let list = STATE.streams.filter(s => {
    if (STATUS_FILTER && s.runtime.status !== STATUS_FILTER) return false;
    if (q) {
      const hay = `${s.name} ${s.title || ''} ${(s.inputs || []).join(' ')}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });

  list.sort((a, b) => {
    if (sortBy === 'bitrate') return (b.runtime.bitrate_kbps || 0) - (a.runtime.bitrate_kbps || 0);
    if (sortBy === 'clients') return (b.runtime.clients || 0) - (a.runtime.clients || 0);
    if (sortBy === 'status') return a.runtime.status.localeCompare(b.runtime.status);
    return a.name.localeCompare(b.name);
  });

  const total = list.length;
  const shown = list.slice(0, perPage);
  const rangeTxt = total ? `1–${shown.length} ${t('of_word')} ${total}` : `0 ${t('of_word')} 0`;
  document.getElementById('range_label_top').textContent = rangeTxt;

  const host = window.location.origin;
  const tb = document.getElementById('streams_tbody');

  tb.innerHTML = shown.map(s => {
    const st = s.runtime.status;
    const isEnabled = s.enabled !== false;
    const modeLabel = (s.mode === 'ondemand') ? t('on_demand') : t('always_started');
    const inputs = s.inputs || [];
    const activeIdx = s.runtime.active_input_index || 0;
    const activeUrl = s.runtime.active_input || '-';

    let ttHtml = `<div style="display:flex; font-weight:800; opacity:0.6; padding-bottom:5.8px; border-bottom:1px solid currentColor; margin-bottom:5.8px; gap:11.5px;">
            <div style="flex:3;">URL</div><div style="flex:1;">Status</div><div style="flex:1;">Track</div><div style="flex:1;">Resolution</div>
          </div>`;
        inputs.forEach((inp, i) => {
          let isA = (i === activeIdx && st === 'Online');
          let stS = isA ? '<span style="color:#10b981;">&check; Active</span>' : '<span style="color:#94a3b8;">&otimes; Disabled</span>';
          let tv = s.runtime.codecs.split('/')[0] || '-';
          let ta = s.runtime.codecs.split('/')[1] || '-';
          ttHtml += `<div style="display:flex; align-items:center; padding:5.8px 0; border-bottom:1px solid rgba(148,163,184,0.15); gap:11.5px;">
            <div style="flex:3; font-family:monospace; font-size:0.9em; word-break:break-all;"><span style="background:#94a3b8;color:#fff;border-radius:50%;padding:2px 5px;font-size: 9.5px;margin-right:5.8px;font-weight:bold;">${i+1}</span>${inp}</div>
            <div style="flex:1; font-weight:700;">${stS}</div>
            <div style="flex:1; font-weight:600; opacity:0.8;">v1<br>a1</div>
            <div style="flex:1; font-weight:700; font-size:0.9em;">${isA ? tv : '-'}</div>
          </div>`;
        });
        const dotsHtml = inputs.map((inp, idx) => {
      let cls = 'inp-dot';
      if (isEnabled && idx === activeIdx) {
        if (st === 'Online') cls += ' active-online';
        else if (st === 'Waiting') cls += ' active-wait';
        else if (st === 'Error') cls += ' active-err';
      }
      return `<span class="${cls}" title="Input #${idx+1}: ${inp}" onclick="switchInputSource('${s.name}', ${idx})"></span>`;
    }).join('');

    const upStr = (st === 'Online') ? fmtStreamUptime(s.runtime.uptime) : '';
    const brStr = (st === 'Online' && s.runtime.bitrate_kbps > 0)
      ? `${(s.runtime.bitrate_kbps / 1000).toFixed(2)} ${t('mbps_unit')} (${s.runtime.bitrate_kbps} ${t('kbps_unit')})`
      : 'n/a';
    const dvrStr = (parseInt(s.dvr_hours || 0, 10) > 0) ? `${t('dvr_on')} (${s.dvr_hours}h)` : t('dvr_off');
    const tokQ = s.token ? `?token=${s.token}` : '';
    const isMoreOpen = OPEN_MORE_SET.has(s.name);
    const bufSec = s.runtime.buffer_sec || 0;
    const targetBuf = s.runtime.target_buffer_sec || 30;
    const bMode = s.runtime.buffer_mode || '30s';

    return `<tr>
      <td><input type="checkbox" class="row-chk" value="${s.name}"></td>
      <td>
        <a class="st-name" onclick="openStreamEditor('${s.name}')">${s.name}</a>
        <div class="st-sub">${s.title || s.name}</div>
        <div class="st-mode">${modeLabel}</div>
        <div class="st-status-${st}">${st}</div>
      </td>
      <td>
        <div class="inp-top">
          <div class="inp-url" title="${activeUrl}">${activeUrl}</div>
          <div class="info-tooltip-wrap">
            <span class="info-badge" onclick="openStreamEditor('${s.name}')">i</span>
            <div class="info-tooltip">${ttHtml}</div>
          </div>
        </div>
        <div class="input-dots">${dotsHtml}</div>
        <div class="inp-meta">${t('tbl_up')}: <b>${upStr}</b></div>
        <div class="inp-meta">${t('tbl_br')}: <b>${brStr}</b></div>
      </td>
      <td class="muted-cell">
        <div class="buf-toggle-group">
          <button class="buf-mode-btn ${bMode !== 'normal' ? 'active' : ''}" onclick="setStreamBufferMode('${s.name}', '30s')">${t('buf_30')}</button>
          <button class="buf-mode-btn ${bMode === 'normal' ? 'active' : ''}" onclick="setStreamBufferMode('${s.name}', 'normal')">${t('buf_norm')}</button>
        </div>
        ${st === 'Online' ? `<div style="font-size: 11.2px;margin-top:2px;opacity:0.9;">${s.runtime.codecs}</div>` : ''}
        ${st === 'Online' ? `<div><span class="buf-pill">${t('buf_label')} ${bufSec}s / ${targetBuf}s</span></div>` : ''}
      </td>
      <td class="muted-cell">${dvrStr}</td>
      <td>
        <div class="out-cell-wrap">
          <div class="out-left">
            ${isEnabled ? `<div>${t('clients_w')} <b>${s.runtime.clients || 0}</b></div>` : ''}
            <div>${t('push_sum')} <b>${s.runtime.pushes_running || 0}</b></div>
            <a class="more-link" onclick="toggleMoreBox('${s.name}')">${t('more')} ${isMoreOpen ? '&#9652;' : '&#9662;'}</a>
            <div class="more-box" style="display:${isMoreOpen ? 'block' : 'none'};">
              <div class="more-row"><span>HLS:</span><code>${host}/${s.name}/index.m3u8${tokQ}</code><button class="btn btn-outline btn-sm" onclick="copyDirect('${host}/${s.name}/index.m3u8${tokQ}')">${t('copy_btn')}</button></div>
              <div class="more-row"><span>TS:</span><code>${host}/${s.name}/mpegts${tokQ}</code><button class="btn btn-outline btn-sm" onclick="copyDirect('${host}/${s.name}/mpegts${tokQ}')">${t('copy_btn')}</button></div>
              
            </div>
          </div>
          <div class="out-actions">
            ${isEnabled ? `
              <button class="sq-btn" title="Reload (${s.name})" onclick="reloadStream('${s.name}')">&#8635;</button>
              <button class="sq-btn" title="Play (${s.name})" onclick="openPlayerModal('${s.name}')">&#9654;</button>
            ` : ''}
            <label class="flu-switch">
              <input type="checkbox" ${isEnabled ? 'checked' : ''} onchange="toggleStreamEnabled('${s.name}', this.checked)">
              <span class="flu-slider"></span>
            </label>
          </div>
        </div>
      </td>
    </tr>`;
  }).join('');
}

/* ==================== ЛОГИКА ПОЛНОЭКРАННОЙ СТРАНИЦЫ ПОТОКА ==================== */

function openStreamEditor(name) {
  const defaultUa = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
  if (!name) {
    EDITING_NAME = null;
    const nextNum = STATE.streams.length + 1;
    const defName = 'Channel_' + nextNum;
    document.getElementById('btn_modal_delete').style.display = 'none';
    document.getElementById('f_name').value = defName;
    document.getElementById('f_title').value = 'Channel ' + nextNum;
    document.getElementById('f_desc').value = '';
    setEditorMode('stream');
    document.getElementById('f_quality').value = 'best';
    document.getElementById('f_buf_mode').value = '30s';
    EDIT_INPUTS_ARR = [''];
    document.getElementById('f_user_agent').value = defaultUa;
    document.getElementById('f_ua_preset').value = defaultUa;
    document.getElementById('f_source_timeout').value = 30;
    document.getElementById('f_clients_timeout').value = 45;
    document.getElementById('f_dvr_hours').value = 0;
    document.getElementById('sp_chk_dvr_quick').checked = false;
    document.getElementById('f_pushes').value = '';
    document.getElementById('f_token').value = '';
    document.getElementById('f_on_play').value = '';
    document.getElementById('f_enabled').checked = true;
    if (document.getElementById('sp_mi_in_box')) {
      document.getElementById('sp_mi_in_box').innerHTML = '<div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div>';
      document.getElementById('sp_mi_out_box').innerHTML = '<div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div>';
    }
    document.getElementById('sp_ov_res').textContent = '-';
    document.getElementById('sp_ov_br').textContent = '0 kbit/s';
    document.getElementById('sp_ov_up').textContent = '0m';
    document.getElementById('sp_ov_status_pill').textContent = 'New';
    document.getElementById('sp_ov_status_pill').className = 'sp-pill-off';
    fillModalOutputs(defName, '');
    renderInputsListRows();
    showSection('stream_page');
    switchStreamPageTab('input');
  } else {
    const s = STATE.streams.find(x => x.name === name);
    if (!s) return;
    EDITING_NAME = s.name;
    document.getElementById('btn_modal_delete').style.display = 'inline-block';
    document.getElementById('f_name').value = s.name;
    document.getElementById('f_title').value = s.title || s.name;
    document.getElementById('f_desc').value = s.description || '';
    setEditorMode(s.mode || 'stream');
    document.getElementById('f_quality').value = s.quality || 'best';
    document.getElementById('f_buf_mode').value = s.buffer_mode || (s.runtime && s.runtime.buffer_mode) || '30s';
    EDIT_INPUTS_ARR = (s.inputs && s.inputs.length) ? [...s.inputs] : [];
    const uaVal = s.user_agent || defaultUa;
    document.getElementById('f_user_agent').value = uaVal;
    const presetSel = document.getElementById('f_ua_preset');
    const hasMatch = Array.from(presetSel.options).some(o => o.value === uaVal);
    presetSel.value = hasMatch ? uaVal : 'custom';

    document.getElementById('f_source_timeout').value = s.source_timeout || 30;
    document.getElementById('f_clients_timeout').value = s.clients_timeout || 45;
    document.getElementById('f_dvr_hours').value = s.dvr_hours || 0;
    document.getElementById('sp_chk_dvr_quick').checked = (parseInt(s.dvr_hours || 0, 10) > 0);
    document.getElementById('f_pushes').value = (s.pushes || []).join(NL);
    document.getElementById('f_token').value = s.token || '';
    document.getElementById('f_on_play').value = s.on_play || '';
    document.getElementById('f_enabled').checked = s.enabled !== false;
    fillModalOutputs(s.name, s.token);
    renderInputsListRows();
    showSection('stream_page');
    switchStreamPageTab('overview');
    updateStreamPageLiveStats();
    playOverviewStream('hls');
  }
}

function setEditorMode(m) {
  document.getElementById('f_mode').value = m;
  document.getElementById('sp_m_stream').classList.toggle('active', m !== 'ondemand');
  document.getElementById('sp_m_ondemand').classList.toggle('active', m === 'ondemand');
}

function toggleCurrentStreamEnable() {
  const chk = document.getElementById('f_enabled');
  chk.checked = !chk.checked;
  if (EDITING_NAME) {
    toggleStreamEnabled(EDITING_NAME, chk.checked);
  }
  updateStreamPageLiveStats();
}

function applyUaPreset(val) {
  if (val !== 'custom') {
    document.getElementById('f_user_agent').value = val;
  } else {
    document.getElementById('f_user_agent').focus();
  }
}

function renderInputsListRows() {
  const box = document.getElementById('sp_inputs_list_box');
  const s = EDITING_NAME ? STATE.streams.find(x => x.name === EDITING_NAME) : null;
  const activeIdx = (s && s.runtime) ? (s.runtime.active_input_index || 0) : 0;
  const isOnline = (s && s.runtime && s.runtime.status === 'Online');

  if (!EDIT_INPUTS_ARR.length) {
    box.innerHTML = `<div style="padding:10.1px 0; opacity:0.6;">Нет добавленных источников. Введите ссылку в поле New URL ниже.</div>`;
    return;
  }

  box.innerHTML = EDIT_INPUTS_ARR.map((url, idx) => {
    const isAct = (idx === activeIdx && isOnline);
    return `<div class="inp-row-item">
      <span class="inp-state-badge ${isAct ? '' : 'standby'}" onclick="if(EDITING_NAME) switchInputSource(EDITING_NAME, ${idx})" style="cursor:pointer;">
        ${isAct ? 'online' : (idx === activeIdx ? 'active' : 'standby')}
      </span>
      <div class="inp-arrows">
        <button class="arr-btn" onclick="moveInputUrl(${idx}, -1)" title="Up">&#9650;</button>
        <button class="arr-btn" onclick="moveInputUrl(${idx}, 1)" title="Down">&#9660;</button>
      </div>
      <div style="flex:1;">
        <div style="font-size: 10.4px; opacity:0.6; font-weight:700;">URL ${idx + 1}</div>
        <input type="text" class="sp-line-input" value="${url.replace(/"/g, '&quot;')}" oninput="EDIT_INPUTS_ARR[${idx}] = this.value">
      </div>
      <a class="more-link" style="text-decoration:none;" onclick="document.getElementById('f_user_agent').focus()">Options</a>
      <button class="copy-ico-btn" style="color:#e11d48;" onclick="removeInputUrlRow(${idx})" title="Delete URL">&#128465;</button>
    </div>`;
  }).join('');
}

function addNewInputUrlRow() {
  const inp = document.getElementById('sp_new_url_inp');
  const v = inp.value.trim();
  if (!v) return;
  EDIT_INPUTS_ARR.push(v);
  inp.value = '';
  renderInputsListRows();
}

function moveInputUrl(idx, dir) {
  const target = idx + dir;
  if (target < 0 || target >= EDIT_INPUTS_ARR.length) return;
  const tmp = EDIT_INPUTS_ARR[idx];
  EDIT_INPUTS_ARR[idx] = EDIT_INPUTS_ARR[target];
  EDIT_INPUTS_ARR[target] = tmp;
  renderInputsListRows();
}

function removeInputUrlRow(idx) {
  EDIT_INPUTS_ARR.splice(idx, 1);
  renderInputsListRows();
}

function switchStreamPageTab(pane) {
  CURRENT_SP_TAB = pane;
  ['overview','input','dvr','output','auth','sessions'].forEach(p => {
    const el = document.getElementById('sp_pane_' + p);
    if (el) el.style.display = (p === pane) ? 'block' : 'none';
    const tb = document.getElementById('sptab_' + p);
    if (tb) tb.classList.toggle('active', p === pane);
  });
  updateTopHeaderBreadcrumb();
  if (pane === 'overview') {
    setTimeout(renderStreamOverviewChart, 40);
  } else {
    stopOverviewPlayer();
  }
}

function playOverviewStream(proto) {
  stopOverviewPlayer();
  ['hls','mse','dvr'].forEach(k => {
    const b = document.getElementById('ov_p_' + k);
    if (b) b.classList.toggle('active', k === proto);
  });
  const name = document.getElementById('f_name').value.trim() || EDITING_NAME;
  if (!name) return;
  const tok = document.getElementById('f_token').value.trim();
  const q = tok ? `?token=${tok}` : '';
  const v = document.getElementById('sp_overview_video');

  {
    const url = (proto === 'mse') ? `/${name}/video.m3u8${q}` : `/${name}/index.m3u8${q}`;
    if (typeof Hls !== 'undefined' && Hls.isSupported()) {
      ovHls = new Hls({ liveSyncDurationCount: 3, maxBufferLength: 45 });
      ovHls.loadSource(url);
      ovHls.attachMedia(v);
      ovHls.on(Hls.Events.MANIFEST_PARSED, () => v.play().catch(()=>{}));
    } else {
      v.src = url;
      v.play().catch(()=>{});
    }
  }
}

function updateStreamPageLiveStats() {
  const s = STATE.streams.find(x => x.name === EDITING_NAME);
  if (!s) return;
  const st = s.runtime.status;
  const isEn = s.enabled !== false;

  const btnDis = document.getElementById('sp_btn_disable');
  btnDis.textContent = isEn ? t('btn_disable') : t('btn_enable');
  btnDis.style.background = isEn ? '#1D4ED8' : '#10b981';

  const pill = document.getElementById('sp_ov_status_pill');
  pill.textContent = st;
  pill.className = (st === 'Online') ? 'sp-pill-online' : (st === 'Waiting' ? 'sp-pill-wait' : 'sp-pill-off');

  const codecs = s.runtime.codecs || '';
  const is4k = codecs.includes('3840') || codecs.includes('2160');
  const isFhd = codecs.includes('1920') || codecs.includes('1080');
  const isHd = codecs.includes('1280') || codecs.includes('720');
  document.getElementById('sp_ov_res').textContent = !codecs ? '-' : (is4k ? '4K UHD' : (isFhd ? 'FHD' : (isHd ? 'HD' : 'SD')));
  document.getElementById('sp_ov_br').textContent = !codecs ? '0 kbit/s' : `${s.runtime.bitrate_kbps || 0}kbit/s`;
  document.getElementById('sp_ov_up').textContent = !codecs ? '0m' : fmtStreamUptime(s.runtime.uptime);

  const inBox = document.getElementById('sp_mi_in_box');
  const outBox = document.getElementById('sp_mi_out_box');
  if (inBox && outBox) {
    if (st !== 'Online' || !codecs || codecs.includes('Pass-through')) {
        inBox.innerHTML = '<div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div>';
        outBox.innerHTML = '<div style="opacity:0.5;font-size:10.4px;margin-top:5.8px;">No data</div>';
    } else {
        const parts = codecs.split('/').map(x => x.trim()).filter(Boolean);
        let inHtml = '', outHtml = '';
        let vIdx = 1, aIdx = 1;
        parts.forEach(p => {
            let lower = p.toLowerCase();
            if (lower.match(/\d+x\d+/)) {
                inHtml += `<span class="mi-track-v" style="margin-bottom:4.3px;">v${vIdx} ${lower}</span>`;
                outHtml += `<span class="mi-track-v" style="margin-bottom:4.3px;">v${vIdx} ${lower} (Copy)</span>`;
                vIdx++;
            } else {
                inHtml += `<span class="mi-track-a" style="margin-bottom:4.3px;">a${aIdx} ${lower}</span>`;
                outHtml += `<span class="mi-track-a" style="margin-bottom:4.3px;">a${aIdx} ${lower} (-c copy)</span>`;
                aIdx++;
            }
        });
        inBox.innerHTML = inHtml;
        outBox.innerHTML = outHtml;
    }
  }

  const activeIdx = (s && s.runtime) ? (s.runtime.active_input_index || 0) : 0;
  const isOnline = (st === 'Online');
  document.querySelectorAll('#sp_inputs_list_box .inp-row-item').forEach((row, idx) => {
    const badge = row.querySelector('.inp-state-badge');
    if (badge) {
      const isAct = (idx === activeIdx && isOnline);
      badge.className = 'inp-state-badge ' + (isAct ? '' : 'standby');
      badge.textContent = isAct ? 'online' : (idx === activeIdx ? 'active' : 'standby');
    }
  });
  renderStreamOverviewChart();

  // Сессии только этого потока
  const stb = document.getElementById('sp_single_sessions_tbody');
  const fSess = STATE.sessions.filter(x => x.stream === EDITING_NAME);
  if (!fSess.length) {
    stb.innerHTML = `<tr><td colspan="7" style="text-align:center;opacity:0.6;">${t('no_sess')}</td></tr>`;
  } else {
    stb.innerHTML = fSess.map(x => `<tr>
      <td>${x.ip}</td><td>${x.proto}</td><td>${x.user_agent}</td>
      <td>${fmtStreamUptime(x.duration)}</td><td>${(x.bytes_sent/1048576).toFixed(2)} MB</td>
      <td>${x.mbits} ${t('mbps_unit')}</td>
      <td><button class="btn btn-danger btn-sm" onclick="kickSession('${x.id}')">Kick</button></td>
    </tr>`).join('');
  }
}

function renderStreamOverviewChart() {
  if (CURRENT_SECTION !== 'stream_page' || CURRENT_SP_TAB !== 'overview' || !EDITING_NAME) return;
  const hist = STREAM_OV_HISTORY[EDITING_NAME] || [];
  if (!hist.length) return;
  drawSmoothChart('canvas_stream_ov', [
    { data: hist.map(x => x.in_mb), color: '#ef4444', fillTop: 'rgba(239, 68, 68, 0.22)' },
    { data: hist.map(x => x.out_mb), color: '#10b981', fillTop: 'rgba(16, 185, 129, 0.22)' },
    { data: hist.map(x => x.clients), color: '#2563eb', fillTop: 'rgba(37, 99, 235, 0.15)' }
  ], null, 'Mb');
}

function fillModalOutputs(name, token) {
  const host = window.location.origin;
  const q = token ? `?token=${token}` : '';
  const setVal = (id, v) => { const el = document.getElementById(id); if (el) el.value = v; };
  setVal('mo_hls', `${host}/${name}/index.m3u8${q}`);
  setVal('mo_hls_video', `${host}/${name}/video.m3u8${q}`);
  setVal('mo_llhls', `${host}/${name}/index.ll.m3u8${q}`);
  setVal('mo_embed', `${host}/${name}/embed.html${q}`);
  setVal('mo_ts', `${host}/${name}/mpegts${q}`);
  
}

function toggleEoBtn(btn, mode) {
  const parent = btn.parentElement;
  const btns = parent.querySelectorAll('.eo-btn');
  btns.forEach(b => b.classList.remove('active-only', 'active-except'));
  btn.classList.add(mode === 'only' ? 'active-only' : 'active-except');
}

async function saveStreamModal() {
  const pendingNewUrl = document.getElementById('sp_new_url_inp').value.trim();
  if (pendingNewUrl) {
    EDIT_INPUTS_ARR.push(pendingNewUrl);
    document.getElementById('sp_new_url_inp').value = '';
  }
  const cleanInputs = EDIT_INPUTS_ARR.map(x => x.trim()).filter(Boolean);
  const payload = {
    name: document.getElementById('f_name').value.trim(),
    title: document.getElementById('f_title').value.trim(),
    description: document.getElementById('f_desc').value.trim(),
    mode: document.getElementById('f_mode').value,
    quality: document.getElementById('f_quality').value,
    buffer_mode: document.getElementById('f_buf_mode').value,
    user_agent: document.getElementById('f_user_agent').value.trim(),
    enabled: document.getElementById('f_enabled').checked,
    inputs: cleanInputs,
    source_timeout: parseInt(document.getElementById('f_source_timeout').value || '30', 10),
    clients_timeout: parseInt(document.getElementById('f_clients_timeout').value || '45', 10),
    dvr_hours: parseInt(document.getElementById('f_dvr_hours').value || '0', 10),
    pushes: document.getElementById('f_pushes').value.split(NL).map(x=>x.trim()).filter(Boolean),
    token: document.getElementById('f_token').value.trim(),
    on_play: document.getElementById('f_on_play').value.trim()
  };
  const r = await fetch('/api/v3/streams', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  if (!r.ok) {
    alert(await r.text());
    return;
  }
  EDITING_NAME = payload.name;
  fillModalOutputs(payload.name, payload.token);
  toast('Saved: ' + payload.name);
  await pollStatus();
  renderInputsListRows();
}

async function deleteStreamModal() {
  if (!EDITING_NAME || !confirm(`Delete ${EDITING_NAME}?`)) return;
  await fetch(`/api/v3/streams/${EDITING_NAME}`, { method: 'DELETE' });
  showSection('streams');
  pollStatus();
}

async function reloadStream(name) {
  if (!name) return;
  await fetch(`/api/v3/streams/${name}/restart`, { method: 'POST' });
  toast(`${t('reloading')}: ${name}`);
  pollStatus();
}

async function reloadAllStreams() {
  for (const s of STATE.streams) {
    if (s.enabled !== false) {
      await fetch(`/api/v3/streams/${s.name}/restart`, { method: 'POST' });
    }
  }
  toast('OK');
  pollStatus();
}

async function toggleStreamEnabled(name, enabled) {
  await fetch(`/api/v3/streams/${name}/toggle`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled})
  });
  pollStatus();
}

async function switchInputSource(name, index) {
  await fetch(`/api/v3/streams/${name}/switch_input`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({index})
  });
  pollStatus();
}

function openPlayerModal(name) {
  PLAYING_NAME = name;
  document.getElementById('player_modal').style.display = 'flex';
  switchPopupProto('hls');
}

function switchPopupProto(proto) {
  if (!PLAYING_NAME) return;
  try { if (popHls) { popHls.destroy(); popHls = null; } } catch(e){}
  try { if (popDash) { popDash.reset(); popDash = null; } } catch(e){}
  const v = document.getElementById('popup_video');
  const s = STATE.streams.find(x => x.name === PLAYING_NAME);
  const q = (s && s.token) ? `?token=${s.token}` : '';
  const isNormal = s && s.runtime && s.runtime.buffer_mode === 'normal';

  document.getElementById('pm_btn_hls').className = (proto === 'hls') ? 'btn btn-sm' : 'btn btn-outline btn-sm';
  document.getElementById('pm_btn_mpd').className = (proto === 'mpd') ? 'btn btn-sm' : 'btn btn-outline btn-sm';
  document.getElementById('play_modal_title').textContent = `StreamFlow Aria (${proto.toUpperCase()}): ${PLAYING_NAME}`;

  {
    const url = `/${PLAYING_NAME}/index.m3u8${q}`;
    if (typeof Hls !== 'undefined' && Hls.isSupported()) {
      popHls = new Hls({
        liveSyncDurationCount: isNormal ? 2 : 5,
        maxBufferLength: isNormal ? 15 : 60
      });
      popHls.loadSource(url);
      popHls.attachMedia(v);
      popHls.on(Hls.Events.MANIFEST_PARSED, () => v.play().catch(()=>{}));
    } else {
      v.src = url;
      v.play().catch(()=>{});
    }
  }
}

function closePlayerModal() {
  try { if (popHls) { popHls.destroy(); popHls = null; } } catch(e){}
  try { if (popDash) { popDash.reset(); popDash = null; } } catch(e){}
  document.getElementById('popup_video').pause();
  document.getElementById('player_modal').style.display = 'none';
}

function renderSessionsTable() {
  const tb = document.getElementById('sessions_tbody');
  if (!STATE.sessions.length) {
    tb.innerHTML = `<tr><td colspan="8" style="text-align:center;opacity:0.6;">${t('no_sess')}</td></tr>`;
    return;
  }
  tb.innerHTML = STATE.sessions.map(s => `<tr>
    <td><b>${s.stream}</b></td><td>${s.ip}</td><td>${s.proto}</td>
    <td>${s.user_agent}</td><td>${fmtStreamUptime(s.duration)}</td>
    <td>${(s.bytes_sent/1048576).toFixed(2)} MB</td><td>${s.mbits} ${t('mbps_unit')}</td>
    <td><button class="btn btn-danger btn-sm" onclick="kickSession('${s.id}')">Kick</button></td>
  </tr>`).join('');
}

async function kickSession(id) {
  await fetch('/api/v3/sessions/kick', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id}) });
  pollStatus();
}

async function saveGuiConf() {
  let raw = document.getElementById('raw_conf_editor').value;
  const port = document.getElementById('gui_port').value || 8080;
  const user = document.getElementById('gui_user').value || 'admin';
  const pass = document.getElementById('gui_pass').value;
  
  if(!/^\s*http\s+\d+/m.test(raw)) raw = `http ${port};\n` + raw;
  else raw = raw.replace(/^\s*http\s+\d+\s*;/m, `http ${port};`);
  
  if(pass) {
    if(!/^\s*edit_auth\s+/m.test(raw)) raw = `edit_auth ${user} ${pass};\n` + raw;
    else raw = raw.replace(/^\s*edit_auth\s+\S+\s+\S+\s*;/m, `edit_auth ${user} ${pass};`);
  } else {
    raw = raw.replace(/^\s*edit_auth\s+(\S+)/m, `edit_auth ${user}`);
  }
  
  document.getElementById('raw_conf_editor').value = raw;
  await saveRawConf();
  document.getElementById('gui_pass').value = '';
}

async function saveRawConf() {
  const raw_conf = document.getElementById('raw_conf_editor').value;
  await fetch('/api/v3/config/raw', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({raw_conf}) });
  toast('OK');
  pollStatus();
}

async function runBulkImport() {
  const m3u_url = document.getElementById('bulk_m3u_url').value.trim();
  const m3u_text = document.getElementById('bulk_m3u_text').value.trim();
  const mode = document.getElementById('bulk_mode').value;
  const r = await fetch('/api/v3/cluster/import_m3u', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({m3u_url, m3u_text, mode})
  });
  const res = await r.json();
  if (!res.ok) {
    alert('Error: ' + (res.error || 'unknown'));
    return;
  }
  toast('OK: +' + res.imported);
  document.getElementById('bulk_m3u_text').value = '';
  pollStatus();
}

async function saveAndSyncCluster() {
  const hosts = document.getElementById('cl_sources_hosts').value.split(NL).map(x=>x.trim()).filter(Boolean);
  const proto = document.getElementById('cl_proto').value;
  const only = document.getElementById('cl_only').value.trim();
  const exc = document.getElementById('cl_except').value.trim();
  const payload = {
    cluster_key: document.getElementById('cl_key').value.trim(),
    peers: document.getElementById('cl_peers').value.split(NL).map(x=>x.trim()).filter(Boolean),
    cluster_sources: hosts.map(h => ({ host: h, proto: proto, only: only, except: exc }))
  };
  const r = await fetch('/api/v3/cluster/save', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const res = await r.json();
  toast('OK: +' + (res.imported || 0));
  pollStatus();
}

function runTesterPlayer() {
  try { if (testHls) { testHls.destroy(); testHls = null; } } catch(e){}
  try { if (testDash) { testDash.reset(); testDash = null; } } catch(e){}
  const url = document.getElementById('tester_url').value.trim();
  if (!url) return;
  const v = document.getElementById('tester_video');
  if (url.toLowerCase().includes('.mpd')) {
    if (typeof dashjs !== 'undefined') {
      testDash = dashjs.MediaPlayer().create();
      testDash.initialize(v, url, true);
    }
  } else if (typeof Hls !== 'undefined' && Hls.isSupported()) {
    testHls = new Hls({liveSyncDurationCount: 4, maxBufferLength: 60});
    testHls.loadSource(url);
    testHls.attachMedia(v);
    testHls.on(Hls.Events.MANIFEST_PARSED, () => v.play().catch(()=>{}));
  } else {
    v.src = url;
    v.play().catch(()=>{});
  }
}

function fallbackCopyText(text) {
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.setAttribute('readonly', '');
  ta.style.position = 'fixed';
  ta.style.left = '-9999px';
  ta.style.top = '0';
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  ta.setSelectionRange(0, ta.value.length);
  try { document.execCommand('copy'); } catch (e) {}
  document.body.removeChild(ta);
}

function copyToClipboardRaw(text) {
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).catch(() => fallbackCopyText(text));
  } else {
    fallbackCopyText(text);
  }
}

function copyText(id) {
  const el = document.getElementById(id);
  if (!el || !el.value) return;
  copyToClipboardRaw(el.value);
  el.focus();
  el.select();
  toast(t('copied'));
}

function copyDirect(txt) {
  if (!txt) return;
  copyToClipboardRaw(txt);
  toast(t('copied'));
}

function toggleAllCheckboxes(master) {
  document.querySelectorAll('.row-chk').forEach(c => { c.checked = master.checked; });
}

async function doLogout() {
  await fetch('/api/v3/logout', { method: 'POST' });
  window.location.href = '/';
}

setAppTheme(APP_THEME, false);
setAppLang(APP_LANG, false);
setInterval(pollStatus, 2000);
pollStatus();
</script>
</body>
</html>
"""


async def handle_index_ui(req: web.Request) -> web.Response:
    return web.Response(text=render_with_prefs(INDEX_HTML), content_type="text/html")


async def on_startup(app: web.Application):
    await MANAGER.init()


async def on_cleanup(app: web.Application):
    for w in list(MANAGER.workers.values()):
        await w.stop()
    if MANAGER.http_client:
        await MANAGER.http_client.close()


def create_app() -> web.Application:
    app = web.Application(middlewares=[admin_auth_middleware])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/", handle_index_ui)
    app.router.add_get("/playlist.m3u", handle_m3u_playlist)

    app.router.add_post("/api/v3/login", api_login)
    app.router.add_post("/api/v3/logout", api_logout)
    app.router.add_post("/api/v3/ui_prefs", api_save_ui_prefs)
    app.router.add_get("/api/v3/status", api_status)
    app.router.add_post("/api/v3/streams", api_save_stream)
    app.router.add_post("/api/v3/streams/buffer_mode", api_set_buffer_mode)
    app.router.add_delete("/api/v3/streams/{name}", api_delete_stream)
    app.router.add_post("/api/v3/streams/{name}/restart", api_restart_stream)
    app.router.add_post("/api/v3/streams/{name}/toggle", api_toggle_stream)
    app.router.add_post("/api/v3/streams/{name}/switch_input", api_switch_input)
    app.router.add_post("/api/v3/sessions/kick", api_kick_session)
    app.router.add_post("/api/v3/config/raw", api_save_raw_conf)
    app.router.add_post("/api/v3/cluster/save", api_save_cluster_settings)
    app.router.add_post("/api/v3/cluster/import_m3u", api_bulk_import_m3u)
    app.router.add_get("/api/v3/cluster/streams", handle_flussonic_api_v3_streams)
    app.router.add_get("/streamer/api/v3/streams", handle_flussonic_api_v3_streams)

    app.router.add_get(r"/{name}/index-{start:\d+}-{duration:\d+|now}.m3u8", handle_dvr_catchup_m3u8)
    app.router.add_get(r"/{name}/video-{start:\d+}-{duration:\d+|now}.m3u8", handle_dvr_catchup_m3u8)
    app.router.add_get(r"/{name}/timeshift_rel-{offset:\d+}.m3u8", handle_timeshift_m3u8)
    app.router.add_get(r"/dvr_file/{name}/{rel:.+}", handle_dvr_segment_file)

    app.router.add_get("/{name}/index.m3u8", handle_live_hls)
    app.router.add_get("/{name}/video.m3u8", handle_live_hls)
    app.router.add_get("/{name}/index.ll.m3u8", handle_live_hls)
    app.router.add_get("/{name}/mpegts", handle_http_mpegts)
    app.router.add_get("/{name}/embed.html", handle_embed_html)
    app.router.add_get(r"/{name}/{seg:[^/]+\.(ts|m4s|mp4|tmp)}", handle_ram_media_chunk)
    return app


if __name__ == "__main__":
    try:
        import uvloop, asyncio
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
    port = 8080
    if STATE_JSON.exists():
        try:
            port = int(json.loads(STATE_JSON.read_text(encoding="utf-8")).get("http_port", 8080))
        except Exception:
            pass
    web.run_app(create_app(), host="0.0.0.0", port=port)



PYEOF

chmod +x /opt/flustream/server.py

# Сохранение порта, логина и пароля без потери существующих каналов
FLUSTREAM_PORT="$FLUSTREAM_PORT" FLUSTREAM_USER="$FLUSTREAM_USER" FLUSTREAM_PASS="$FLUSTREAM_PASS" \
/opt/flustream/venv/bin/python - << 'INITPY'
import json, os
from pathlib import Path

state_file = Path("/etc/flustream/streams.json")
data = {
    "http_port": int(os.environ["FLUSTREAM_PORT"]),
    "admin_user": os.environ["FLUSTREAM_USER"],
    "admin_pass": os.environ["FLUSTREAM_PASS"],
    "ui_theme": "2",
    "ui_lang": "ru",
    "dvr_root": "/var/lib/flustream/dvr",
    "dvr_disk_limit": 85,
    "global_auth_url": "",
    "cluster_key": "flustream_secret",
    "peers": [],
    "cluster_sources": [],
    "streams": []
}
if state_file.exists():
    try:
        existing = json.loads(state_file.read_text(encoding="utf-8"))
        existing["http_port"] = int(os.environ["FLUSTREAM_PORT"])
        existing["admin_user"] = os.environ["FLUSTREAM_USER"]
        existing["admin_pass"] = os.environ["FLUSTREAM_PASS"]
        # Обновляем таймаут источника до 35 сек на существующих каналах для 30-сек буфера
        pass
        data = existing
    except Exception:
        pass

state_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
INITPY

echo "[5/5] Регистрация и запуск службы flustream.service (KillMode=control-group)..."
cat << 'SVCEOF' > /etc/systemd/system/flustream.service
[Unit]
Description=StreamFlow Aria Media Server (Remux, 30s RAM Buffer, HLS, DASH, DVR)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/flustream
ExecStart=/opt/flustream/venv/bin/python /opt/flustream/server.py
KillMode=control-group
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable --now flustream.service
systemctl restart flustream.service

SERVER_IP=$(hostname -I | awk '{print $1}')
echo "================================================================"
echo " ГОТОВО! Медиасервер StreamFlow Aria успешно установлен и запущен."
echo "----------------------------------------------------------------"
echo " Веб-интерфейс StreamFlow Aria: http://${SERVER_IP}:${FLUSTREAM_PORT}"
echo " Логин администратора:         ${FLUSTREAM_USER}"
echo " Буфер защиты от зависаний:    30 секунд (в RAM /dev/shm)"
echo " Конфигурационный файл:        /etc/flustream/flustream.conf"
echo "================================================================"
