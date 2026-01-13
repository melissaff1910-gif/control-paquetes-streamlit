# Control de Paquetes — Streamlit (simple, 100% repo)
# Guarda/lee datos desde Google Sheets. Solo requiere:
# 1) Un Google Sheet con una hoja llamada "paquetes"
# 2) En Streamlit Cloud: Secrets con:
#    - SHEET_ID: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
#    - gcp_service_account: { ... json de la cuenta de servicio ... }
#    - (opcional) HOLIDAYS: ["2026-01-01","2026-04-07"]

import streamlit as st
import pandas as pd
import json
from datetime import datetime, date, time, timedelta

# ---------------------- Parámetros por defecto ----------------------
ESTADOS_VALIDOS = ("CAMPO", "ENTREGAS", "JURIDICO", "POSTCAMPO")
ZONAS_VALIDAS   = ("URBANO", "RURAL", "MIXTO")
FASE_ORDEN      = list(ESTADOS_VALIDOS)

WORK_START = "08:00"
WORK_END   = "16:30"
WORK_HOURS = 8.5

# horas por predio (aprox)
RATIOS_H_PREDIO = {
    "CAMPO":     (5*8.5)/30.0,  # 42.5/30
    "ENTREGAS":  8.5/80.0,
    "JURIDICO":  8.5/30.0,
    "POSTCAMPO": 1.0/4.4
}
MIN_HORAS = {"CAMPO":1.0, "ENTREGAS":0.5, "JURIDICO":0.5, "POSTCAMPO":0.5}

# ---------------------- Utilidades de fecha ----------------------
def parse_fecha(s: str|None):
    if not s: return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def norm_fecha_txt(s: str|None):
    if s is None or str(s).strip()=="":
        return None
    d = parse_fecha(s)
    if not d: raise ValueError("Fecha inválida. Use AAAA-MM-DD o DD/MM/AAAA.")
    return d.strftime("%Y-%m-%d")

def validar_fechas(fent_txt, fsal_txt):
    f_ent = norm_fecha_txt(fent_txt)
    f_sal = norm_fecha_txt(fsal_txt) if fsal_txt else None
    d_ent = parse_fecha(f_ent)
    d_sal = parse_fecha(f_sal) if f_sal else None
    if d_sal and d_sal < d_ent:
        raise ValueError("La salida no puede ser antes de la entrada.")
    return f_ent, f_sal

# ---------------------- Lógica de horas ----------------------
def _t(hhmm: str): return datetime.strptime(hhmm, "%H:%M").time()
HORA_INICIO = _t(WORK_START)
HORA_FIN    = _t(WORK_END)
HOLIDAYS    = set(st.secrets.get("HOLIDAYS", []))

def is_business_day(d: date) -> bool:
    if d.weekday() >= 5:  # 5=sábado, 6=domingo
        return False
    if d.strftime("%Y-%m-%d") in HOLIDAYS:
        return False
    return True

def clamp_to_workday(dt: datetime) -> datetime:
    start = datetime.combine(dt.date(), HORA_INICIO)
    end   = datetime.combine(dt.date(), HORA_FIN)
    if dt < start: return start
    if dt > end:   return end
    return dt

def business_hours_between(d0: date, end_dt: datetime) -> float:
    if not d0 or not end_dt: return 0.0
    total = 0.0
    cur = d0
    while cur < end_dt.date():
        if is_business_day(cur):
            total += WORK_HOURS
        cur += timedelta(days=1)
    if is_business_day(end_dt.date()):
        start_dt = datetime.combine(end_dt.date(), HORA_INICIO)
        end_dt_c = clamp_to_workday(end_dt)
        delta = (end_dt_c - start_dt).total_seconds()/3600.0
        if delta > 0:
            total += min(delta, WORK_HOURS)
    return round(total, 2)

def expected_hours(fase: str, n: int) -> float:
    fase = (fase or "").upper()
    n = max(int(n or 0), 0)
    h = n * float(RATIOS_H_PREDIO.get(fase, 0.0))
    h = max(h, float(MIN_HORAS.get(fase, 0.0)))
    return round(h, 2)

def real_hours(f_ent: str|None, f_sal: str|None) -> float:
    d0 = parse_fecha(f_ent)
    end_dt = datetime.combine(parse_fecha(f_sal), HORA_FIN) if (f_sal and parse_fecha(f_sal)) else datetime.now()
    if not d0: return 0.0
    return business_hours_between(d0, end_dt)

def kpis_fila(fase, n, f_ent, f_sal):
    h_esp = expected_hours(fase, n)
    h_real = real_hours(f_ent, f_sal)
    prog = 0 if h_esp==0 else round((h_real/h_esp)*100, 1)
    return h_esp, round(h_real,2), prog, (h_real > h_esp)

# ---------------------- Google Sheets I/O ----------------------
@st.cache_resource
def _gsheet():
    import gspread
    from google.oauth2.service_account import Credentials
    sa_info = st.secrets["gcp_service_account"]
    if isinstance(sa_info, str):  # por si lo pegaron como string
        sa_info = json.loads(sa_info)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(st.secrets["SHEET_ID"])
    try:
        ws = sh.worksheet("paquetes")
    except Exception:
        ws = sh.add_worksheet(title="paquetes", rows="2000", cols="20")
        ws.append_row(["id_paquete","lote","municipio","estado","n_predios","zona","fecha_entrada","fecha_salida"])
    return ws

def load_df():
    ws = _gsheet()
    rows = ws.get_all_records()
    if not rows:
        return pd.DataFrame(columns=["id_paquete","lote","municipio","estado","n_predios","zona","fecha_entrada","fecha_salida"])
    df = pd.DataFrame(rows)
    df["n_predios"] = pd.to_numeric(df.get("n_predios", 0), errors="coerce").fillna(0).astype(int)
    for c in ["id_paquete","lote","municipio","estado","zona","fecha_entrada","fecha_salida"]:
        if c in df: df[c] = df[c].astype(str)
    return df

def save_df(df: pd.DataFrame):
    ws = _gsheet()
    ws.clear()
    header = ["id_paquete","lote","municipio","estado","n_predios","zona","fecha_entrada","fecha_salida"]
    ws.append_row(header)
    if len(df):
        values = df[header].astype(str).values.tolist()
        ws.append_rows(values)

# ---------------------- UI ----------------------
st.set_page_config(page_title="Control de Paquetes", layout="wide")
st.title("Control de Paquetes — público (Streamlit + Google Sheets)")

with st.sidebar:
    st.subheader("Jornada")
    st.info(f"{WORK_START}–{WORK_END} ({WORK_HOURS} h)")
    st.caption("Feriados (secrets): " + ", ".join(sorted(HOLIDAYS)) if HOLIDAYS else "Sin feriados cargados")
    if st.button("Recargar datos"):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.experimental_rerun()

df = load_df()

st.subheader("Filtros")
colf = st.columns([1,1,1,1,1.2])
with colf[0]:
    f_id = st.text_input("Buscar por ID (prioriza)", "")
with colf[1]:
    f_muni = st.selectbox("Municipio", [""] + sorted(df["municipio"].dropna().unique().tolist()))
with colf[2]:
    f_estado = st.selectbox("Fase", [""] + list(ESTADOS_VALIDOS))
with colf[3]:
    f_zona = st.selectbox("Zona", [""] + list(ZONAS_VALIDAS))
with colf[4]:
    f_fent = st.selectbox("Fecha de entrada", [""] + sorted(df["fecha_entrada"].dropna().unique().tolist()))

st.subheader("Formulario (CRUD)")
c1, c2, c3, c4 = st.columns(4)
with c1:
    in_id   = st.text_input("ID paquete")
    in_muni = st.text_input("Municipio")
with c2:
    in_lote   = st.text_input("Lote")
    in_estado = st.selectbox("Fase", list(ESTADOS_VALIDOS))
with c3:
    in_predios = st.number_input("# Predios", min_value=0, step=1)
    in_zona    = st.selectbox("Zona", list(ZONAS_VALIDAS))
with c4:
    in_fent = st.text_input("Fecha entrada (AAAA-MM-DD o DD/MM/AAAA)")
    in_fsal = st.text_input("Fecha salida (opcional)")

b1, b2, b3, b4, b5 = st.columns(5)

def _validar():
    if not in_id: return False, "El ID es obligatorio."
    if not in_lote: return False, "El Lote es obligatorio."
    if not in_muni: return False, "El Municipio es obligatorio."
    try:
        f_ent, f_sal = validar_fechas(in_fent, in_fsal)
    except Exception as e:
        return False, str(e)
    return True, (f_ent, f_sal)

with b1:
    if st.button("Incluir"):
        ok, data = _validar()
        if not ok:
            st.warning(data)
        else:
            f_ent, f_sal = data
            dup = (df["id_paquete"]==in_id) & (df["estado"]==in_estado) & (df["fecha_entrada"]==f_ent)
            if dup.any():
                st.warning("Ya existe ese evento (ID + fase + fecha_entrada).")
            else:
                new = pd.DataFrame([{
                    "id_paquete":in_id, "lote":in_lote, "municipio":in_muni,
                    "estado":in_estado, "n_predios":int(in_predios), "zona":in_zona,
                    "fecha_entrada":f_ent, "fecha_salida":f_sal
                }])
                df = pd.concat([df, new], ignore_index=True)
                save_df(df); st.success("Incluido."); st.experimental_rerun()

with b2:
    idx_mod = st.number_input("Idx modificar (tabla)", min_value=0, step=1, value=0, key="modi")
    if st.button("Modificar"):
        if idx_mod >= len(df):
            st.warning("Índice fuera de rango.")
        else:
            ok, data = _validar()
            if not ok:
                st.warning(data)
            else:
                f_ent, f_sal = data
                df.loc[idx_mod, ["id_paquete","lote","municipio","estado","n_predios","zona","fecha_entrada","fecha_salida"]] = \
                    [in_id, in_lote, in_muni, in_estado, int(in_predios), in_zona, f_ent, f_sal]
                save_df(df); st.success("Modificado."); st.experimental_rerun()

with b3:
    idx_del = st.number_input("Idx borrar (tabla)", min_value=0, step=1, value=0, key="borra")
    if st.button("Borrar"):
        if idx_del >= len(df):
            st.warning("Índice fuera de rango.")
        else:
            df = df.drop(index=idx_del).reset_index(drop=True)
            save_df(df); st.success("Borrado."); st.experimental_rerun()

with b4:
    idx_out = st.number_input("Idx salida hoy", min_value=0, step=1, value=0, key="salida")
    if st.button("Salida hoy"):
        if idx_out >= len(df):
            st.warning("Índice fuera de rango.")
        else:
            df.loc[idx_out, "fecha_salida"] = datetime.now().date().strftime("%Y-%m-%d")
            save_df(df); st.success("Salida marcada."); st.experimental_rerun()

with b5:
    idx_next = st.number_input("Idx siguiente fase", min_value=0, step=1, value=0, key="next")
    if st.button("Siguiente fase"):
        if idx_next >= len(df):
            st.warning("Índice fuera de rango.")
        else:
            row = df.loc[idx_next]
            fase = str(row["estado"])
            if fase not in FASE_ORDEN:
                st.warning("Fase no válida.")
            else:
                pos = FASE_ORDEN.index(fase)
                if pos == len(FASE_ORDEN)-1:
                    st.info("POSTCAMPO es la última fase.")
                else:
                    fase_next = FASE_ORDEN[pos+1]
                    f_ent = row["fecha_salida"] or datetime.now().date().strftime("%Y-%m-%d")
                    dup = (df["id_paquete"]==row["id_paquete"]) & (df["estado"]==fase_next) & (df["fecha_entrada"]==f_ent)
                    if dup.any():
                        st.warning("Ya existe la siguiente fase con esa fecha de entrada.")
                    else:
                        new = pd.DataFrame([{
                            "id_paquete":row["id_paquete"], "lote":row["lote"], "municipio":row["municipio"],
                            "estado":fase_next, "n_predios":int(row["n_predios"]), "zona":row["zona"],
                            "fecha_entrada":f_ent, "fecha_salida":None
                        }])
                        df = pd.concat([df, new], ignore_index=True)
                        save_df(df); st.success(f"Creado evento en {fase_next}."); st.experimental_rerun()

# ---------------------- Vista + KPIs ----------------------
def _pasa(r):
    if f_muni and r["municipio"] != f_muni: return False
    if f_estado and r["estado"] != f_estado: return False
    if f_zona and r["zona"] != f_zona: return False
    if f_fent and r["fecha_entrada"] != f_fent: return False
    return True

if f_id:
    view = df[df["id_paquete"]==f_id].copy()
else:
    view = df[df.apply(_pasa, axis=1)].copy()

def _kpis_row(r):
    h_esp, h_real, prog, alert = kpis_fila(r["estado"], r["n_predios"], r["fecha_entrada"], r["fecha_salida"])
    return pd.Series({"h_esp":h_esp, "h_real":h_real, "progreso":prog, "alerta":"Sí" if alert else "No"})

if len(view):
    kp = view.apply(_kpis_row, axis=1)
    view = pd.concat([view.reset_index(drop=True), kp], axis=1)
    view.insert(0, "idx", range(len(view)))

st.subheader("Eventos")
st.dataframe(
    view[["idx","id_paquete","lote","municipio","estado","n_predios","zona",
          "fecha_entrada","fecha_salida","h_esp","h_real","progreso","alerta"]] if len(view) else view,
    use_container_width=True
)

csv_bytes = (view if len(view) else df).to_csv(index=False).encode("utf-8")
st.download_button("Exportar CSV (vista)", data=csv_bytes, file_name="vista_eventos.csv", mime="text/csv")
