#!/usr/bin/env python3
"""
EMAY SleepO2 实时低血氧报警 -> Bark 推送

检测逻辑（针对"摘面罩后又睡着"的特征）：
  A. 反复下降：5 分钟内 >= 5 次下降（低于前 120 秒最高值 4 个点以上，持续 >= 10 秒）
  B. 深度低值：连续 10 秒低于 85%
  C. 持续低氧兜底：连续 60 秒低于 88%（不受冷却限制）
  满足任一即报警；报警后冷却 3 分钟，条件仍在则再次报警。
  另外：手指脱落 > 2 分钟、蓝牙断连 > 3 分钟会发一条静默通知；
  设备电量只够不连蓝牙撑到早上 7 点时，发静默通知、让设备停止推送并退出，留电给设备自己记录。
  具体数值以 config.ini 为准。

日志（终端、~/spo2_logs 下的文件）一律用英文，方便分享；推送到手机的内容用中文。

用法：
  pip install -r requirements.txt
  export BARK_KEY=你的key            # 可选 BARK_URL=https://你的自建服务器
  python spo2_alarm.py --test-push    # 测试推送（睡前务必跑一次）
  python spo2_alarm.py --replay EMAY_SpO2-xxxx.csv   # 用历史数据回放，看会在何时报警
  python spo2_alarm.py --replay ~/spo2_logs/xxxx.csv # 也支持本脚本自己记录的日志
  caffeinate -i python spo2_alarm.py  # 正式运行，防止 Mac 休眠

报警阈值等参数在同目录的 config.ini 里，改完直接重新运行即可生效，不用改代码。
"""
import argparse, asyncio, configparser, csv, json, os, signal, sys, time, urllib.request
from collections import deque
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATHS = [os.path.join(_HERE, "config.ini"), os.path.join(_HERE, "config.local.ini")]  # 后者不提交，覆盖前者
_CONFIG_DEFAULTS = {
    "detection": {
        "drop": "4", "min_dur": "10", "base_win": "120",
        "cluster_n": "5", "cluster_win": "300",
        "low_abs": "85", "low_dur": "10",
        "sustain_abs": "88", "sustain_dur": "60",
        "cooldown": "180",
        "jump": "5", "settle": "15", "gap": "10",
        "finger_off": "120", "link_lost": "180",
    },
    "device": {"morning": "07:00", "standalone_hours": "14", "reserve": "10", "min_battery": "20"},
    "bark": {"volume": "1", "emergency_number": "当地急救电话"},
}
_cfg = configparser.ConfigParser(inline_comment_prefixes=("#",))
_cfg.read_dict(_CONFIG_DEFAULTS)
_cfg.read(_CONFIG_PATHS, encoding="utf-8")  # 存在的依次覆盖上面的默认值；都不存在就用默认值跑

DROP = _cfg.getint("detection", "drop")             # 下降幅度（点）
MIN_DUR = _cfg.getint("detection", "min_dur")        # 下降持续秒数
BASE_WIN = _cfg.getint("detection", "base_win")       # 基线窗口（秒）
CLUSTER_N = _cfg.getint("detection", "cluster_n")      # 反复下降次数
CLUSTER_WIN = _cfg.getint("detection", "cluster_win")    # 反复下降窗口（秒）
LOW_ABS = _cfg.getint("detection", "low_abs")        # 绝对低值
LOW_DUR = _cfg.getint("detection", "low_dur")        # 绝对低值持续秒数
SUSTAIN_ABS = _cfg.getint("detection", "sustain_abs")    # 持续低氧兜底阈值
SUSTAIN_DUR = _cfg.getint("detection", "sustain_dur")    # 持续低氧兜底秒数
COOLDOWN = _cfg.getint("detection", "cooldown")       # 报警冷却（秒）
FINGER_OFF = _cfg.getint("detection", "finger_off")     # 手指脱落提醒（秒）
LINK_LOST = _cfg.getint("detection", "link_lost")      # 断连提醒（秒）
JUMP = _cfg.getint("detection", "jump")            # 相邻读数跳变超过此值视为伪读数
SETTLE = _cfg.getint("detection", "settle")          # 跳变/重新接触/数据中断后的不可信秒数
GAP = _cfg.getint("detection", "gap")             # 相邻读数间隔超过此秒数视为数据中断
MORNING = datetime.strptime(_cfg.get("device", "morning"), "%H:%M").time()   # 电量要撑到的时间
STANDALONE_HOURS = _cfg.getfloat("device", "standalone_hours")   # 满电不连蓝牙能记录几小时
BATTERY_RESERVE = _cfg.getfloat("device", "reserve")     # 安全余量（%）
MIN_BATTERY = _cfg.getfloat("device", "min_battery")     # 底线：电量 <= 此值无论如何都退出
BARK_VOLUME = _cfg.getint("bark", "volume")          # critical 推送音量 0-10
EMERGENCY_NUMBER = _cfg.get("bark", "emergency_number")   # 持续低氧报警里提示拨打的急救电话

START_TIMEOUT = 60  # 一次连接（扫描+连接+启动数据流）最多等多久
STOP_TIMEOUT = 10   # 断开连接最多等多久
STALL = 30          # 连接中多久没数据就整个重连
KICK_AFTER = 10     # 连接中多久没数据先重发 START_REALTIME
RETRY_DELAY = 10    # 重连前等待
BATTERY_POLL = 300  # 每隔多久主动请求一次电量（库只在连接时请求一次）
BATTERY_STEP = 10   # 设备按 10% 一档上报电量，实际值可能比报告的低将近一档
DAYTIME_HOURS = 12  # 离 MORNING 超过这么多小时，视为白天测试，不按撑到早上计算


def battery_needed(now):
    """不连蓝牙撑到下一个 MORNING 需要的电量（%，已含余量和读数误差）→ (需要的电量, 目标时间, 其中撑到早上的部分)"""
    target = datetime.combine(now.date(), MORNING)
    if target <= now:
        target += timedelta(days=1)
    hours = (target - now).total_seconds() / 3600
    standalone = 100 / STANDALONE_HOURS * hours if hours <= DAYTIME_HOURS else 0
    return max(standalone + BATTERY_RESERVE + BATTERY_STEP, MIN_BATTERY), target, standalone


# ---------------------------------------------------------------- 推送
def bark(title, body, critical=True, dry=False):
    if dry:
        print(f"    [dry run] {'critical' if critical else 'silent'} push not sent")
        return
    key = os.environ.get("BARK_KEY")
    if not key:
        print("!! BARK_KEY not set", file=sys.stderr); return
    server = os.environ.get("BARK_URL", "https://api.day.app").rstrip("/")
    payload = {"device_key": key, "title": title, "body": body, "group": "spo2"}
    if critical:
        payload.update(level="critical", volume=BARK_VOLUME, call="1")  # 穿透静音/专注模式，重复响铃
    else:
        payload.update(level="passive")  # 静默：不响不震，只进通知中心
    req = urllib.request.Request(server + "/push", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json; charset=utf-8",
                                          "User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except Exception as e:
        print(f"!! Bark push failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------- 检测器（与数据来源无关）
# notify(title, body, critical, t, log, safety=False)：title/body 是推送到手机的中文，log 是写日志用的英文；
# safety=True 只用于 C（持续低氧兜底），安静模式下也照常响铃
class Detector:
    def __init__(self, notify):
        self.notify = notify
        self.win = deque()            # (t, spo2) 有效读数，保留 BASE_WIN 秒
        self.events = deque()         # 下降事件开始时间
        self.below_since = None       # 当前下降段开始时间
        self.below_counted = False
        self.low_since = None
        self.sustain_since = None
        self.sustain_alarmed = False
        self.last_alarm = -1e18
        self.last_valid = None
        self.first_t = None
        self.finger_warned = False
        self.prev = None              # 上一个原始读数 (t, spo2)
        self.settle_until = -1e18

    def feed(self, t, spo2):
        if spo2 is not None and not (50 <= spo2 <= 100):
            spo2 = None
        if self.first_t is None:
            self.first_t = t
        if spo2 is not None:
            if self.prev is None or t - self.prev[0] > GAP or abs(spo2 - self.prev[1]) > JUMP:
                self.settle_until = t + SETTLE
            self.prev = (t, spo2)
            self.last_valid = t; self.finger_warned = False   # 有数值就说明手指在，不管是否在稳定期
        else:
            self.prev = None

        if spo2 is None or t < self.settle_until:
            # 只有设备真的报空读数才算脱落；蓝牙断连造成的数据空白不算
            since = self.last_valid if self.last_valid is not None else self.first_t
            if spo2 is None and t - since > FINGER_OFF and not self.finger_warned:
                n = int(t - since)
                self.notify("血氧仪无读数", f"已 {n} 秒没有有效读数，探头可能脱落", False, t,
                            f"No valid reading for {n}s - probe may be off")
                self.finger_warned = True
            self.below_since = None; self.low_since = None; self.sustain_since = None
            return

        # C. 持续低氧兜底：不需要基线，不受冷却限制，每段持续低氧报一次
        if spo2 < SUSTAIN_ABS:
            self.sustain_since = self.sustain_since or t
            if not self.sustain_alarmed and t - self.sustain_since >= SUSTAIN_DUR - 1:
                self.sustain_alarmed = True
                self.last_alarm = t
                n = int(t - self.sustain_since) + 1
                self.notify("血氧持续偏低",
                            f"血氧已持续 {n} 秒低于 {SUSTAIN_ABS}%（当前 {spo2}%），"
                            f"不像平时的周期性下降。坐起来清醒呼吸几分钟，仍然偏低或胸闷气短请打"
                            f"{' ' if EMERGENCY_NUMBER[:1].isdigit() else ''}{EMERGENCY_NUMBER}", True, t,
                            f"SpO2 below {SUSTAIN_ABS}% for {n}s (now {spo2}%) - sustained low, not the usual cyclic dip",
                            safety=True)
        else:
            self.sustain_since = None; self.sustain_alarmed = False

        self.win.append((t, spo2))
        while self.win and self.win[0][0] < t - BASE_WIN:
            self.win.popleft()
        if len(self.win) < 30:        # 基线还没建立
            return
        base = max(v for _, v in self.win)

        # A. 反复下降
        if spo2 <= base - DROP:
            if self.below_since is None:
                self.below_since, self.below_counted = t, False
            if not self.below_counted and t - self.below_since >= MIN_DUR - 1:
                self.events.append(self.below_since); self.below_counted = True
        else:
            self.below_since = None
        while self.events and self.events[0] < t - CLUSTER_WIN:
            self.events.popleft()

        # B. 持续低值
        self.low_since = (self.low_since or t) if spo2 < LOW_ABS else None

        reason = None
        if len(self.events) >= CLUSTER_N:
            reason = (f"{CLUSTER_WIN // 60} 分钟内 {len(self.events)} 次血氧下降",
                      f"{len(self.events)} dips within {CLUSTER_WIN // 60} min")
        elif self.low_since is not None and t - self.low_since >= LOW_DUR - 1:
            n = int(t - self.low_since) + 1
            reason = (f"血氧 {spo2}% 已持续 {n} 秒低于 {LOW_ABS}%",
                      f"SpO2 below {LOW_ABS}% for {n}s")
        if reason and t - self.last_alarm >= COOLDOWN:
            self.last_alarm = t
            self.notify("血氧反复下降", f"{reason[0]}（当前 {spo2}%）。检查面罩是否戴好、机器是否在运行", True, t,
                        f"{reason[1]} (now {spo2}%) - check the mask and that the machine is running")


# ---------------------------------------------------------------- 回放模式
def replay(path):
    fired = []
    def notify(title, body, critical, t, log, safety=False):
        ts = datetime.fromtimestamp(t).strftime("%H:%M:%S")
        fired.append(critical); print(f"{ts}  {'ALERT ' if critical else 'NOTICE'} {log}")
    det = Detector(notify)
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if "time" in row:         # 本脚本 ~/spo2_logs 的日志
                t = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S").timestamp()
                v = row["spo2"].strip()
            else:                     # EMAY app 导出
                t = datetime.strptime(f"{row['Date']} {row['Time']}", "%m/%d/%Y %I:%M:%S %p").timestamp()
                v = row.get("SpO2(%)", "").strip()
            det.feed(t, int(float(v)) if v else None)
    print(f"{len(fired)} fired ({sum(fired)} critical)")


# ---------------------------------------------------------------- 实时模式
def parse_battery(raw):
    # GET_BATTERY 的回复：F6 <电量%> <校验>；校验和其他帧一样是 sum & 0x7F
    if len(raw) == 3 and raw[0] == 0xF6 and raw[2] == sum(raw[:2]) & 0x7F and raw[1] <= 100:
        return raw[1]
    return None


# 设备会把多帧打包进一个通知、或让一帧跨两个通知（重连时补发缓存尤其如此），
# 而 SDK 只认"一个通知正好一帧读数"。这里按帧头长度自己拼接切分。
FRAME_LEN = [                # (帧头, 整帧长度含校验)，长帧头放前面
    (b"\xeb\x01\x05", 8),    # 血氧读数
    (b"\xeb\x01\x07", 8),    # 手指不在探头上：EB 01 07 7F 7F 7F 00 cks，每秒一帧
    (b"\xeb\x7f", 3),        # STOP_REALTIME 确认
    (b"\xf6", 3),            # 电量
    (b"\xf9", 10),           # HELLO 回复（设备时钟）
    (b"\xfe", 13),           # DEVICE_STATE 回复
    (b"\x00", 1),            # 确认
]


class FrameStream:
    def __init__(self):
        self.buf = b""

    def feed(self, data):
        """追加一个通知的数据，返回切出的完整帧；认不出或校验错的字节逐个丢出，便于记录和重新同步"""
        self.buf += bytes(data)
        frames = []
        while self.buf:
            n = self._frame_len()
            if n is None or len(self.buf) < max(n, 1):
                break                                   # 帧还不完整，等下一个通知
            if n == 0 or (n > 1 and self.buf[n - 1] != sum(self.buf[:n - 1]) & 0x7F):
                frames.append(self.buf[:1]); self.buf = self.buf[1:]
                continue
            frames.append(self.buf[:n]); self.buf = self.buf[n:]
        return frames

    def _frame_len(self):
        for head, n in FRAME_LEN:
            k = min(len(head), len(self.buf))
            if self.buf[:k] == head[:k]:
                return n if len(self.buf) >= len(head) else None
        return 0


def install_frame_hook(stream, on_frames):
    # 一个通知正好一帧读数时交给 SDK 照常处理；其余情况 SDK 会整包丢掉，改由 on_frames 处理
    import emay_sleepo2.client as emay_client
    orig = emay_client.parse_reading

    def wrapped(raw):
        if not stream.buf:
            r = orig(raw)
            if r is not None:
                return r
        on_frames(bytes(raw), stream.feed(raw))
        return None
    emay_client.parse_reading = wrapped


async def live(address=None, dry=False, quiet=False):
    from emay_sleepo2 import EMAYClient, Reading, Status
    from emay_sleepo2.protocol import GET_BATTERY, HEARTBEAT, START_REALTIME, STOP_REALTIME, parse_reading

    main_task = asyncio.current_task()
    state = {"last": time.time(), "link_warned": False, "mid_line": False, "battery": None, "kicked_at": None,
             "finger_off": False, "exit_reason": None}

    def now():
        return f"{datetime.now():%H:%M:%S}"

    def println(text):
        if state["mid_line"]:
            print(); state["mid_line"] = False
        print(f"{now()}  {text}", flush=True)

    def notify(title, body, critical, t, log, safety=False):
        # 安静模式：A/B 报警只记录、发静默通知；C（持续低氧兜底）照常响铃
        wake = critical and (safety or not quiet)
        tag = "ALERT " if wake else "ALERT (quiet mode, silent push)" if critical else "NOTICE"
        println(f"{tag} {log}")
        bark(title, body, wake, dry)

    det = Detector(notify)
    logdir = os.path.expanduser("~/spo2_logs"); os.makedirs(logdir, exist_ok=True)
    stamp = f"{datetime.now():%Y%m%d_%H%M%S}"
    logf = open(os.path.join(logdir, f"{stamp}.csv"), "a", buffering=1)
    logf.write("time,spo2,pulse,battery\n")

    # SDK 处理不了的通知（电量、设备状态、打包的多条读数等）原样记下来，再拆帧处理
    framef = open(os.path.join(logdir, f"{stamp}_frames.log"), "a", buffering=1)
    stream = FrameStream()
    warned_garbage = False

    def frame_log(text):
        framef.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}\n")

    def set_finger_off(off):
        if off != state["finger_off"]:
            state["finger_off"] = off
            msg = ("Finger off the probe (device still reports every second; not reconnecting)" if off
                   else "Finger back on the probe")
            println(msg)
            frame_log(f"   {msg}")

    def on_battery(batt):
        need, target, standalone = battery_needed(datetime.now())
        if batt != state["battery"]:
            basis = (f"{standalone:.0f}% to last until {target:%H:%M} without Bluetooth" if standalone
                     else f"daytime, more than {DAYTIME_HOURS}h before {target:%H:%M}")
            println(f"Device battery {batt}% (stops at <= {need:.0f}% right now: {basis}"
                    f" + {BATTERY_RESERVE:.0f}% reserve + {BATTERY_STEP}% reading step, floor {MIN_BATTERY:.0f}%)")
        state["battery"] = batt
        if batt <= need and state["exit_reason"] is None:
            # 剩下的电只够设备自己撑到早上：停止推送、退出，把电留给设备记录。监测类问题不叫醒人，所以用静默通知
            state["exit_reason"] = f"battery {batt}% <= {need:.0f}% needed to last until {target:%H:%M}"
            notify("血氧仪电量不够连着蓝牙到早上，监测已停止",
                   f"电量 {batt}%，不连蓝牙撑到 {target:%H:%M} 约需 {need:.0f}%（含余量）。"
                   f"已断开蓝牙，设备会继续自己记录，但这次监测不会再报警",
                   False, time.time(),
                   f"Battery {batt}% <= {need:.0f}% needed to last until {target:%H:%M} without Bluetooth: "
                   f"stopping so the device can keep recording on its own")
            main_task.cancel()

    def on_frames(raw, frames):
        nonlocal warned_garbage
        if not frames or any(f[:3] != b"\xeb\x01\x07" for f in frames):
            frame_log(f"<- {raw.hex(' ')}")         # 手指离开时每秒一帧，只记开始和结束，不逐帧记
        n_read = 0
        for f in frames:
            if f[:3] == b"\xeb\x01\x05":
                r = parse_reading(f)
                if r is not None:
                    n_read += 1
                    on_reading(r)
            elif f[:3] == b"\xeb\x01\x07":
                set_finger_off(True)
                on_reading(Reading(spo2=None, pulse=None))  # 连接是活的；空读数让检测器走"探头脱落"逻辑
            elif (batt := parse_battery(f)) is not None:
                on_battery(batt)
            elif len(f) == 1 and f != b"\x00":
                frame_log(f"   unrecognized byte: {f.hex()}")
                if not warned_garbage:
                    warned_garbage = True
                    println(f"Received unrecognized data (see {stamp}_frames.log)")
        if n_read > 1:
            frame_log(f"   split {n_read} readings out of one notification")
        if n_read >= 3:
            println(f"Device flushed {n_read} buffered readings")

    install_frame_hook(stream, on_frames)

    cmd_names = {HEARTBEAT: "HEARTBEAT", GET_BATTERY: "GET_BATTERY",
                 START_REALTIME: "START_REALTIME", STOP_REALTIME: "STOP_REALTIME"}

    def instrument_writes(client):
        # 库的心跳写入失败会被静默吞掉、卡住也没有超时；这里加超时并把失败记进 frames.log
        orig = client.write_gatt_char

        async def write(char, data, response=True):
            try:
                return await asyncio.wait_for(orig(char, data, response=response), STOP_TIMEOUT)
            except Exception as e:
                frame_log(f"   {cmd_names.get(bytes(data), bytes(data).hex(' '))} write failed: {str(e) or type(e).__name__}")
                raise
        client.write_gatt_char = write

    async def send_cmd(emay, cmd):
        frame_log(f"-> {cmd_names[cmd]}")
        try:
            await emay._client.write_gatt_char(emay._write_char, cmd, response=True)
        except Exception:
            pass  # instrument_writes 已经记录

    def on_reading(r):
        t = time.time()
        state["last"] = t; state["link_warned"] = False
        if r.spo2 is not None:
            set_finger_off(False)
        if state["kicked_at"] is not None:
            msg = f"Data resumed ({t - state['kicked_at']:.0f}s after resending START_REALTIME)"
            println(msg)
            frame_log(f"   {msg}")
            state["kicked_at"] = None
        batt = state["battery"]
        logf.write(f"{datetime.now():%Y-%m-%d %H:%M:%S},{r.spo2 if r.spo2 is not None else ''},"
                   f"{r.pulse if r.pulse is not None else ''},{batt if batt is not None else ''}\n")
        spo2_str = f"{r.spo2}%" if r.spo2 is not None else "--"
        pulse_str = f"{r.pulse} bpm" if r.pulse is not None else "--"
        batt_str = f"  battery {batt}%" if batt is not None else ""
        print(f"\r{now()}  SpO2 {spo2_str:>4}  pulse {pulse_str:>7}{batt_str}\033[K", end="", flush=True)
        state["mid_line"] = True
        det.feed(t, r.spo2)

    def check_link():
        gap = time.time() - state["last"]
        if gap > LINK_LOST and not state["link_warned"]:
            notify("血氧监测断连", f"已 {int(gap)} 秒收不到数据，报警暂时失效", False, time.time(),
                   f"No data for {int(gap)}s - alarms are not working")
            state["link_warned"] = True

    async def teardown(emay):
        try:
            await asyncio.wait_for(emay.stop(), STOP_TIMEOUT)
        except Exception:
            # stop() 卡住时直接断开底层连接，否则设备一直被占用、扫描不到
            client = emay._client
            if client is not None:
                try:
                    await asyncio.wait_for(client.disconnect(), STOP_TIMEOUT)
                except Exception:
                    pass

    # Ctrl+C 由 asyncio.run 转成取消；关终端窗口（SIGHUP）和 kill（SIGTERM）也按取消处理，
    # 这样退出前能通知设备停止推送，否则设备会一直处于推送状态、白白耗电
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(sig, main_task.cancel)

    if quiet:
        println(f"Quiet mode: dip / deep-low alerts are silent pushes; only the sustained-low safety alert "
                f"(below {SUSTAIN_ABS}% for {SUSTAIN_DUR}s) will ring")

    emay = None
    try:
        while True:
            # 重连由这个循环负责，关掉库自带的重连，避免两边同时连
            emay = EMAYClient(auto_reconnect=False)
            emay.on_reading = on_reading
            emay.on_status_change = lambda s: println(f"status: {s}")
            stream.buf = b""
            try:
                println("Connecting...")
                await asyncio.wait_for(emay.start(address) if address else emay.start(), START_TIMEOUT)
                if emay.status != Status.STREAMING:
                    raise RuntimeError(f"connect failed ({emay.status.name})")
                println("Connected, monitoring")
                frame_log("-> connected (the SDK start sequence already sent GET_BATTERY)")
                instrument_writes(emay._client)
                connected_at = last_poll = time.time()
                state["kicked_at"] = None
                while True:
                    await asyncio.sleep(5)
                    check_link()
                    if emay.status != Status.STREAMING:
                        raise RuntimeError(f"Bluetooth disconnected ({emay.status.name})")
                    idle = time.time() - max(state["last"], connected_at)
                    if idle > STALL:
                        raise RuntimeError(f"no data for {STALL}s")
                    if idle > KICK_AFTER and state["kicked_at"] is None:
                        # 连接还在但设备停了推送：先重发开始推送，比整个重连快、也省电
                        state["kicked_at"] = time.time()
                        println(f"No data for {idle:.0f}s, resending START_REALTIME")
                        await send_cmd(emay, START_REALTIME)
                    if time.time() - last_poll >= BATTERY_POLL:
                        last_poll = time.time()
                        await send_cmd(emay, GET_BATTERY)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                println(f"Connection lost: {str(e) or type(e).__name__}, reconnecting in {RETRY_DELAY}s")
                await teardown(emay)
                check_link()
                await asyncio.sleep(RETRY_DELAY)
    except asyncio.CancelledError:
        reason = state["exit_reason"] or "interrupted"
        if emay is not None:
            println(f"Exiting ({reason}): stopping the device stream and disconnecting "
                    f"(press Ctrl+C again to force quit)")
            frame_log(f"-> exiting ({reason}): sending STOP_REALTIME and disconnecting")
            await teardown(emay)
        raise


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", help="用 EMAY 导出的 CSV 或 ~/spo2_logs 日志回放")
    ap.add_argument("--test-push", action="store_true", help="发一条测试推送")
    ap.add_argument("--address", help="指定设备地址（可选）")
    ap.add_argument("--dry", action="store_true", help="实时模式但只打印不推送")
    ap.add_argument("--quiet", action="store_true",
                    help="安静模式：反复下降/深度低值只发静默通知，只有持续低氧兜底会响铃")
    a = ap.parse_args()
    if a.test_push:
        bark("测试：血氧反复下降", "如果你听到了这条，报警通道正常", True)
    elif a.replay:
        replay(a.replay)
    else:
        if not a.dry and not os.environ.get("BARK_KEY"):
            sys.exit("!! BARK_KEY not set, alerts cannot be pushed. Run `source ~/.zshrc` or open a new terminal first")
        try:
            asyncio.run(live(a.address, a.dry, a.quiet))
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
