import os, time, json, socket
import requests
import paho.mqtt.client as mqtt

DEBUG = os.getenv("DEBUG", "false").lower() == "true"

def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# ===================== Config =====================
BASE = "https://admin.easycharging-tech.com/prod-api"

# Credenciais EVOWATT
ACCOUNT       = os.getenv("EC_ACCOUNT", "")
PASSWORD      = os.getenv("EC_PASSWORD", "")
FCM_TOKEN     = os.getenv("EC_FCM_TOKEN", "")
MONETARY_UNIT = os.getenv("EC_MONETARY_UNIT", "R$")
MONETARY_CODE = os.getenv("EC_MONETARY_CODE", "BRL")
COUNTRY_CODE  = os.getenv("EC_COUNTRY_CODE", "BR")
DEVICE_HINT   = os.getenv("EC_DEVICE_ID")  # id longo OU deviceNum (SN)
POLL_SEC      = int(os.getenv("POLL_SEC", "15"))
HISTORY_SEC   = int(os.getenv("HISTORY_SEC", "900"))  # 15 min
LANG          = os.getenv("EC_LANG", "pt_BR")

# MQTT
MQTT_HOST     = os.getenv("MQTT_HOST", "mqtt")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
DISCOVERY     = os.getenv("DISCOVERY_PREFIX", "homeassistant")
MQTT_BASE     = os.getenv("MQTT_BASE", "evowatt")

# ================== HTTP session ==================
S = requests.Session()
S.headers.update({
    "user-agent": "Dart/3.5 (dart:io)",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip",
    "content-type": "application/json; charset=utf-8",
    "content-language": LANG,              # pt_BR por padrão (igual ao app)
    "app": "evowatt.charging",
    "app-type": "1",
    "app-version": "2.1.0",
    "brand-sign": "EV",
})
_last_login = 0
TOKEN_TTL   = 60 * 30  # reloga a cada 30 min

def _ts():
    S.headers["timestamp"] = str(int(time.time() * 1000))

def _preflight():
    _ts()
    try:
        S.get(f"{BASE}/api/sys/v2/getResource", timeout=20)
    except Exception:
        pass

def login(force: bool=False):
    """Autentica e injeta Bearer token."""
    global _last_login
    if not force and (time.time() - _last_login) < TOKEN_TTL and S.headers.get("Authorization"):
        return
    if not ACCOUNT or not PASSWORD:
        raise RuntimeError("Defina EC_ACCOUNT e EC_PASSWORD")
    _preflight()
    body = {
        "num": ACCOUNT,
        "password": PASSWORD,
        "fcmToken": FCM_TOKEN,
        "monetaryUnit": MONETARY_UNIT,
        "monetaryCode": MONETARY_CODE,
        "countryCode": COUNTRY_CODE,
    }
    _ts()
    r = S.post(f"{BASE}/api/sys/v1/pass_login", json=body, timeout=20)
    data = r.json() if "application/json" in r.headers.get("content-type","") else {}
    token = (data.get("data") or {}).get("token") or data.get("token")
    if not token:
        raise RuntimeError(f"Falha login: {r.status_code} {r.text[:200]}")
    S.headers["Authorization"] = f"Bearer {token}"
    S.headers["token"] = token
    _last_login = time.time()

def api_get(path, params=None):
    login()
    _ts()
    url = f"{BASE}{path}"
    r = S.get(url, params=params or {}, timeout=20)
    if r.status_code == 401:
        login(force=True); _ts()
        r = S.get(url, params=params or {}, timeout=20)
    return r.json()

def _safe_json(resp):
    """Tenta extrair JSON sem levantar exceção."""
    try:
        return resp.json()
    except Exception:
        return None

def _log_http(prefix: str, resp, body=None):
    """Log consistente para depurar chamadas HTTP."""
    j = _safe_json(resp)
    if isinstance(j, dict):
        code = j.get("code")
        msg = j.get("msg") or j.get("message") or ""
        debug(f"{prefix}: HTTP {resp.status_code} code={code} msg={msg}")
    else:
        debug(f"{prefix}: HTTP {resp.status_code} body={str(resp.text)[:200]}")
    if body is not None:
        try:
            debug(f"{prefix}: payload={json.dumps(body, ensure_ascii=False)[:300]}")
        except Exception:
            debug(f"{prefix}: payload={str(body)[:300]}")

def api_post(path: str, body: dict, timeout=20):
    """
    POST com retry em 401 (igual api_get).
    Retorna (resp, json_dict_or_none).
    """
    login(); _ts()
    url = f"{BASE}{path}"
    r = S.post(url, json=body, timeout=timeout)

    if r.status_code == 401:
        login(force=True); _ts()
        r = S.post(url, json=body, timeout=timeout)

    return r, _safe_json(r)

def pick_device(devs):
    if not devs: return None
    if not DEVICE_HINT: return devs[0]
    for d in devs:
        if d.get("id") == DEVICE_HINT or d.get("deviceNum") == DEVICE_HINT:
            return d
    return devs[0]

# ================== MQTT helpers ==================
client = mqtt.Client(
    client_id=f"evowatt-{socket.gethostname()}",
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)
if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

def pub(topic, payload, retain=False):
    client.publish(topic, payload, qos=1, retain=retain)

# será definida após bootstrap
state_topic = None

def discovery_sensor(obj_id, name, state_tpl, unit=None, dev_class=None, state_class=None,
                     icon=None, avail_t=None, dev=None, uniq=None, entity_cat=None,
                     state_topic_param=None):
    cfg_topic = f"{DISCOVERY}/sensor/{uniq or obj_id}/config"
    cfg = {
        "name": name,
        "state_topic": state_topic_param or state_topic,  # override quando necessário
        "value_template": state_tpl,
        "unique_id": uniq or obj_id,
        "availability_topic": avail_t,
        "device": dev,
    }
    if unit: cfg["unit_of_measurement"] = unit
    if dev_class: cfg["device_class"] = dev_class
    if state_class: cfg["state_class"] = state_class
    if icon: cfg["icon"] = icon
    if entity_cat: cfg["entity_category"] = entity_cat
    pub(cfg_topic, json.dumps(cfg, ensure_ascii=False), retain=True)

def discovery_binary(obj_id, name, val_tpl, dev=None, avail_t=None, uniq=None, dev_class=None):
    cfg_topic = f"{DISCOVERY}/binary_sensor/{uniq or obj_id}/config"
    cfg = {
        "name": name,
        "state_topic": state_topic,
        "value_template": val_tpl,
        "payload_on": "ON",
        "payload_off": "OFF",
        "unique_id": uniq or obj_id,
        "availability_topic": avail_t,
        "device": dev,
    }
    if dev_class: cfg["device_class"] = dev_class
    pub(cfg_topic, json.dumps(cfg, ensure_ascii=False), retain=True)

def discovery_number(obj_id, name, cmd_topic, state_tpl, min_v, max_v, step, unit,
                     dev=None, avail_t=None, uniq=None, icon=None):
    cfg_topic = f"{DISCOVERY}/number/{uniq or obj_id}/config"
    cfg = {
        "name": name,
        "unique_id": uniq or obj_id,
        "command_topic": cmd_topic,
        "state_topic": state_topic,
        "value_template": state_tpl,
        "min": min_v, "max": max_v, "step": step,
        "unit_of_measurement": unit,
        "availability_topic": avail_t,
        "device": dev,
    }
    if icon: cfg["icon"] = icon
    pub(cfg_topic, json.dumps(cfg, ensure_ascii=False), retain=True)

def discovery_button(obj_id, name, cmd_topic, dev=None, avail_t=None, uniq=None, icon=None, entity_cat=None):
    cfg_topic = f"{DISCOVERY}/button/{uniq or obj_id}/config"
    cfg = {
        "name": name,
        "unique_id": uniq or obj_id,
        "command_topic": cmd_topic,
        "availability_topic": avail_t,
        "device": dev,
    }
    if icon: cfg["icon"] = icon
    if entity_cat: cfg["entity_category"] = entity_cat
    pub(cfg_topic, json.dumps(cfg, ensure_ascii=False), retain=True)

# ================== EV control & history ==================
def set_property(product_code: str, device_num: str, prop_id: str, value):
    """POST /api/mqtt/v2/sendPropertyDevice to set properties (e.g., charg-current)."""
    body = {
        "productCode": product_code,
        "deviceNum": device_num,
        "id": prop_id,
        "value": str(value),
    }
    r, j = api_post("/api/mqtt/v2/sendPropertyDevice", body, timeout=20)

    ok = (r.status_code == 200) and isinstance(j, dict) and j.get("code") == 200
    if not ok:
        _log_http("set_property FAIL", r, body)
    return ok

def end_charging(product_code: str, device_num: str):
    """Encerra a sessão de carregamento de forma determinística (usa chargingId)."""
    snap = charging_snapshot(device_num) or {}
    charging_id = snap.get("id")

    if not charging_id:
        debug(f"end_charging: sem sessão ativa (chargingId ausente) deviceNum={device_num}")
        return False

    body = {
        "productCode": product_code,
        "deviceNum": device_num,
        "chargingId": str(charging_id),
    }
    r, j = api_post("/api/mqtt/v2/sendEndCharging", body, timeout=20)

    ok = (r.status_code == 200) and isinstance(j, dict) and j.get("code") == 200
    if not ok:
        _log_http("end_charging FAIL", r, body)
    return ok

def charging_snapshot(device_num: str):
    """GET /api/user/v2/getChargingId -> snapshot 'ao vivo' da sessão atual."""
    j = api_get("/api/user/v2/getChargingId", {"deviceNum": device_num}) or {}
    return (j.get("data") or {})  # id, ts, voltage, amount(kWh), current, power(kW), durationTime(s)

def charging_timeseries(device_num: str):
    """GET /api/user/v2/getDataDuringCharging -> série temporal durante a sessão."""
    j = api_get("/api/user/v2/getDataDuringCharging", {"deviceNum": device_num}) or {}
    return j.get("data") or []

def user_daily_chart():
    login(); _ts()
    url = f"{BASE}/api/user/v2/userChargRecordChart"
    r = S.get(url, params={"pageNum":1, "pageSize":10000, "type":1}, timeout=20)
    j = r.json() if r.ok else {}
    if isinstance(j, dict) and j.get("code") == 200:
        return j.get("data") or []
    return []

def user_records_by_date(date_str: str):
    login(); _ts()
    url = f"{BASE}/api/user/v2/userChargRecordList"
    r = S.get(url, params={"pageNum":1, "pageSize":20, "type":1, "date":date_str}, timeout=20)
    j = r.json() if r.ok else {}
    if isinstance(j, dict) and j.get("code") == 200:
        return (j.get("data") or {}).get("list") or []
    return []

def parse_chart_date(s: str):
    try:
        y, m, d = s.split(".")
        return (int(y), int(m), int(d))
    except Exception:
        return (0, 0, 0)

def publish_history(base: str, avail_t: str):
    """Publica histórico diário, dia atual e última sessão."""
    try:
        chart = user_daily_chart()

        pub(f"{base}/history/daily", json.dumps(chart, ensure_ascii=False), retain=True)

        if not chart:
            pub(avail_t, "online", retain=True)
            return

        latest = max(chart, key=lambda x: parse_chart_date(str(x.get("date", ""))))
        debug(f"Publicando histórico para o dia mais recente: {latest.get('date')}")

        today = {
            "date": latest.get("date", ""),
            "kwh": float(latest.get("degreesNum") or 0),
            "brl": float(latest.get("billSum") or 0),
            "count": int(latest.get("count") or 0),
        }
        pub(f"{base}/today", json.dumps(today, ensure_ascii=False), retain=True)

        recs = user_records_by_date(latest["date"])

        if recs:
            last = max(
                recs,
                key=lambda x: int(str(x.get("endTimeStamp") or "0")[:10])
            )

            last_payload = {
                "id": last.get("id"),
                "kwh": float(last.get("degrees") or 0),
                "brl": float(last.get("bill") or 0),
                "duration_s": int(last.get("duration") or 0),
                "start_ts": int(str(last.get("startTimeStamp") or "0")[:10]),
                "end_ts": int(str(last.get("endTimeStamp") or "0")[:10]),
                "date": last.get("date"),
            }
            debug(f"Última sessão publicada: {last_payload}")
            pub(f"{base}/last_session", json.dumps(last_payload, ensure_ascii=False), retain=True)

        pub(avail_t, "online", retain=True)

    except Exception as e:
        print("publish_history error:", e)
        pub(avail_t, "offline", retain=True)

def interpret_status(d: dict, snap: dict | None = None):
    """Deriva flags 'plugged' e 'active' de getDeviceInfo (+ snapshot opcional)."""
    try:
        gun = int(d.get("gunStatus") or 0)
    except Exception:
        gun = 0
    try:
        chs = int(d.get("chargeStatus") or 0)  # 0=idle, 1/2=carregando (variável por fw)
    except Exception:
        chs = 0

    plugged = (gun == 1)

    live_power = float((snap or {}).get("power") or 0.0)
    live_current = float((snap or {}).get("current") or 0.0)

    # ativo se fw reporta 1/2 OU se há potência/corrente significativa
    active = (chs in (1, 2)) or (plugged and (live_power > 0.05 or live_current > 0.5))

    status_text = (
        "Carregando" if active else
        ("Pronto (plugado)" if plugged else "Parado / desconectado")
    )
    return plugged, active, status_text


# ======================= Main =======================
if __name__ == "__main__":
    # MQTT connect
    bridge_avail = f"{MQTT_BASE}/bridge/availability"
    client.will_set(bridge_avail, "offline", retain=True)
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()
    pub(bridge_avail, "online", retain=True)
    debug(f"MQTT conectado em {MQTT_HOST}:{MQTT_PORT}")

    # EVOWATT bootstrap
    login()
    devs = api_get("/api/device/v2/getDeviceList").get("data", [])
    chosen = pick_device(devs)
    if not chosen:
        raise SystemExit("Nenhum dispositivo encontrado na conta Evowatt")

    dev_id    = chosen.get("id")
    dev_num   = chosen.get("deviceNum")
    nice_name = chosen.get("name") or dev_num
    debug(f"Dispositivo selecionado: id={dev_id} deviceNum={dev_num} nome={nice_name}")

    info         = api_get("/api/device/v2/getDeviceInfo", {"deviceId": dev_id})
    pdata        = (info.get("data") or {})
    product_code = pdata.get("productCode") or "Evowatt"
    manufacturer = "Evowatt / EasyCharging"

    node_id    = f"evowatt_{dev_num or dev_id}"
    base       = f"{MQTT_BASE}/{dev_num or dev_id}"
    avail_t    = f"{base}/availability"
    state_topic= f"{base}/state"   # JSON com todos os campos publicados
    today_topic= f"{base}/today"
    last_topic = f"{base}/last_session"
    pub(avail_t, "online", retain=True)

    device_obj = {
        "identifiers": [node_id],
        "manufacturer": manufacturer,
        "model": product_code,
        "name": f"Evowatt {nice_name}",
    }

    # --- MQTT discovery: sensores principais (state_topic) ---
    discovery_sensor(f"{node_id}_kwh",  "EVOWATT Sessão kWh",
                     "{{ value_json.degrees_kwh }}", unit="kWh",
                     dev_class="energy", state_class="total_increasing",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_kwh")
    discovery_sensor(f"{node_id}_brl",  "EVOWATT Sessão R$",
                     "{{ value_json.bill_brl }}", unit="R$",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_brl")
    discovery_sensor(f"{node_id}_dur",  "EVOWATT Sessão Duração",
                     "{{ value_json.duration_s }}", unit="s",
                     dev_class="duration", state_class="measurement",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_duration")
    discovery_sensor(f"{node_id}_setA", "EVOWATT Corrente (setpoint)",
                     "{{ value_json.setpoint_a }}", unit="A",
                     state_class="measurement",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_setA")
    discovery_sensor(f"{node_id}_net",  "EVOWATT Rede",
                     "{{ value_json.network }}",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_net", icon="mdi:wifi")
    discovery_binary(f"{node_id}_charging", "EVOWATT Carregando",
                     "{{ 'ON' if value_json.charging else 'OFF' }}",
                     dev=device_obj, avail_t=avail_t, uniq=f"{node_id}_charging", dev_class="battery_charging")
    # Cabo plugado (derivado de gunStatus)
    discovery_binary(f"{node_id}_plugged", "EVOWATT Plugado",
                 "{{ 'ON' if value_json.plugged else 'OFF' }}",
                 dev=device_obj, avail_t=avail_t, uniq=f"{node_id}_plugged", dev_class="plug")

    # Texto de estado amigável
    discovery_sensor(f"{node_id}_status_txt", "EVOWATT Estado",
                 "{{ value_json.status }}",
                 avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_status_txt", icon="mdi:ev-station")


    # --- [NOVO] Sensores de telemetria ao vivo (state_topic) ---
    discovery_sensor(f"{node_id}_now_kw", "EVOWATT Potência (kW)", "{{ value_json.now_kw }}",
                     unit="kW", state_class="measurement", avail_t=avail_t, dev=device_obj,
                     uniq=f"{node_id}_now_kw", icon="mdi:flash")
    discovery_sensor(f"{node_id}_voltage", "EVOWATT Tensão (V)", "{{ value_json.voltage_v }}",
                     unit="V", state_class="measurement", avail_t=avail_t, dev=device_obj,
                     uniq=f"{node_id}_voltage", icon="mdi:sine-wave")
    discovery_sensor(f"{node_id}_current", "EVOWATT Corrente (A)", "{{ value_json.current_a }}",
                     unit="A", state_class="measurement", avail_t=avail_t, dev=device_obj,
                     uniq=f"{node_id}_current", icon="mdi:current-ac")

    # --- MQTT discovery: number para setpoint (comando) ---
    cmd_current = f"{base}/cmd/charg_current"
    discovery_number(f"{node_id}_setA_cmd", "EVOWATT Setpoint (A)",
                     cmd_topic=cmd_current,
                     state_tpl="{{ value_json.setpoint_a }}",
                     min_v=6, max_v=32, step=1, unit="A",
                     dev=device_obj, avail_t=avail_t, uniq=f"{node_id}_setA_cmd", icon="mdi:current-ac")

    # --- MQTT discovery: botão para encerrar a sessão ---
    cmd_stop = f"{base}/cmd/stop"
    discovery_button(f"{node_id}_stop", "EVOWATT Parar Carga", cmd_topic=cmd_stop,
                     dev=device_obj, avail_t=avail_t, uniq=f"{node_id}_stop", icon="mdi:power")

    # --- MQTT discovery: sensores de histórico (tópicos específicos) ---
    discovery_sensor(f"{node_id}_today_kwh", "EVOWATT Hoje kWh",
                     "{{ value_json.kwh }}", unit="kWh",
                     dev_class="energy", state_class="measurement",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_today_kwh",
                     state_topic_param=today_topic)
    discovery_sensor(f"{node_id}_today_brl", "EVOWATT Hoje R$",
                     "{{ value_json.brl }}", unit="R$",
                     state_class="measurement", avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_today_brl",
                     icon="mdi:currency-brl", state_topic_param=today_topic)
    discovery_sensor(f"{node_id}_today_cnt", "EVOWATT Hoje sessões",
                     "{{ value_json.count }}",
                     state_class="measurement", avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_today_cnt",
                     icon="mdi:counter", state_topic_param=today_topic)

    discovery_sensor(f"{node_id}_last_kwh", "EVOWATT Última sessão kWh",
                     "{{ value_json.kwh }}", unit="kWh",
                     dev_class="energy", state_class="measurement",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_last_kwh",
                     state_topic_param=last_topic)
    discovery_sensor(f"{node_id}_last_brl", "EVOWATT Última sessão R$",
                     "{{ value_json.brl }}", unit="R$",
                     state_class="measurement", avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_last_brl",
                     icon="mdi:currency-brl", state_topic_param=last_topic)
    discovery_sensor(f"{node_id}_last_duration", "EVOWATT Última sessão duração (s)",
                     "{{ value_json.duration_s }}", unit="s",
                     dev_class="duration", state_class="measurement",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_last_duration",
                     state_topic_param=last_topic)
    discovery_sensor(f"{node_id}_last_end_ts", "EVOWATT Última sessão fim (ts)",
                     "{{ value_json.end_ts }}",
                     avail_t=avail_t, dev=device_obj, uniq=f"{node_id}_last_end_ts",
                     icon="mdi:clock-end", state_topic_param=last_topic)

    # --- comandos MQTT ---
    cmd_refresh = f"{base}/cmd/refresh_history"

    def on_message(_cli, _ud, msg):
        try:
            debug(f"CMD {msg.topic} payload={msg.payload!r}")
            if msg.topic == cmd_current:
                val = msg.payload.decode().strip()
                try:
                    val = int(float(val))
                except Exception:
                    print("Payload inválido para setpoint:", msg.payload)
                    return
                ok = set_property(product_code, dev_num, "charg-current", val)
                pub(f"{cmd_current}/ack", "OK" if ok else "ERR", retain=False)

            elif msg.topic == cmd_refresh:
                publish_history(base, avail_t)
                pub(f"{cmd_refresh}/ack", "OK", retain=False)

            elif msg.topic == cmd_stop:
                ok = end_charging(product_code, dev_num)
                pub(f"{cmd_stop}/ack", "OK" if ok else "ERR", retain=False)
                if ok:
                    time.sleep(2)
                    publish_history(base, avail_t)    

        except Exception as e:
            print("on_message error:", e)

    client.on_message = on_message
    client.subscribe(cmd_current, qos=1)
    client.subscribe(cmd_refresh, qos=1)
    client.subscribe(cmd_stop, qos=1)

    # --- histórico inicial ---
    publish_history(base, avail_t)
    last_hist = time.time()

    # --- loop principal (estado quase em tempo real) ---
    def fmt_net(n):
        return {1:"LAN", 2:"Wi-Fi", 3:"4G"}.get(n, "desconhecida")
    while True:
        try:
            # flags + setpoint
            info = api_get("/api/device/v2/getDeviceInfo", {"deviceId": dev_id})
            d = (info.get("data") or {})
            setpoint = d.get("chargCurrent")
            net      = fmt_net(d.get("networkWay"))

            # só chama snapshot se fizer sentido (economiza chamadas)
            want_snap = True
            try:
                chs = int(d.get("chargeStatus") or 0)
                gun = int(d.get("gunStatus") or 0)
                want_snap = (chs in (1, 2)) or (gun == 1)
            except Exception:
                pass

            snap = charging_snapshot(dev_num) if want_snap else {}

            plugged, active, status_text = interpret_status(d, snap)

            state = {
                "charging":   bool(active),
                "plugged":    bool(plugged),
                "status":     status_text,
                "setpoint_a": setpoint,
                "network":    net,
                "ts":         int(time.time()),
            }

            if active:
                # Durante a sessão -> snapshot com V/A/kW e kWh acumulado
                if snap:
                    state.update({
                        "voltage_v":  float(snap.get("voltage") or 0),
                        "current_a":  float(snap.get("current") or 0),
                        "now_kw":     float(snap.get("power")   or 0),
                        "degrees_kwh":float(snap.get("amount")  or 0),  # energia acumulada
                        "duration_s": int(snap.get("durationTime") or 0),
                    })
                else:
                    # fallback
                    data = api_get("/api/device/v2/deviceData", {"deviceId": dev_id})
                    x = (data.get("data") or {})
                    state.update({
                        "degrees_kwh": x.get("degreesNum"),
                        "bill_brl":    x.get("billNum"),
                        "duration_s":  x.get("durationNum"),
                        "co2_g":       x.get("carbonEmissionNum"),
                    })
            else:
                # Fora da sessão -> totais agregados
                data = api_get("/api/device/v2/deviceData", {"deviceId": dev_id})
                x = (data.get("data") or {})
                state.update({
                    "degrees_kwh": x.get("degreesNum"),
                    "bill_brl":    x.get("billNum"),
                    "duration_s":  x.get("durationNum"),
                    "co2_g":       x.get("carbonEmissionNum"),
                    "voltage_v":   None,
                    "current_a":   None,
                    "now_kw":      0.0,
                })

            pub(state_topic, json.dumps(state, ensure_ascii=False), retain=False)
            pub(avail_t, "online", retain=True)

            # refresh periódico do histórico
            if (time.time() - last_hist) >= HISTORY_SEC:
                publish_history(base, avail_t)
                last_hist = time.time()

        except Exception as e:
            pub(avail_t, "offline", retain=True)
            print("Erro no ciclo:", e)
            try:
                login(force=True)
            except Exception as e2:
                print("Erro ao relogar:", e2)

        time.sleep(POLL_SEC)
