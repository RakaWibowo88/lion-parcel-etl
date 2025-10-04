import re
import os
import math
import json
import urllib.parse
from pathlib import Path
from statistics import mean
from typing import List, Dict, Any, Optional
import logging
import sys  
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl import connection

import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from sqlalchemy.types import Text, Float, TIMESTAMP

SCHEMA = "public"                     # <— ganti
TABLE  = "lion_parcell_bonus_test_stg"     # <— ganti

FOLDER_URL = "https://drive.google.com/drive/folders/13s_BXaxWGPk7kBACVOvuw9Vml2f7fmg5"
DOWNLOAD_DIR = Path("downloads_json")
OUT_CSV = Path("combined_table.csv")
OUT_PARQUET = Path("combined_table.parquet")

def extract_folder_id(url: str) -> str:
    parts = url.rstrip("/").split("/folders/")
    if len(parts) > 1:
        return parts[1].split("?")[0]
    qs = urllib.parse.urlparse(url).query
    return urllib.parse.parse_qs(qs).get("id", [""])[0]

def make_download_link(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def make_view_link(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view"

def list_via_embedded(folder_id: str) -> List[Dict]:
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    for a in soup.select("a"):
        data_id = a.get("data-id")
        title = (a.get("title") or a.text or "").strip()
        href = a.get("href") or ""
        if not data_id or not title:
            parent = a.find_parent(attrs={"data-id": True})
            if parent:
                data_id = parent.get("data-id")
        if not data_id or not title:
            continue

        is_folder = "/folders/" in href or "folder" in (a.get("class") or [])
        mime_type = "application/vnd.google-apps.folder" if is_folder else "file"

        items.append({
            "id": data_id,
            "name": title,
            "mimeType": mime_type,
            "webViewLink": f"https://drive.google.com/drive/folders/{data_id}" if is_folder else make_view_link(data_id),
            "downloadLink": None if is_folder else make_download_link(data_id),
        })
    return items

def _js_unescape(s: str) -> str:
    import codecs
    return codecs.decode(s, 'unicode_escape')

def list_via_drive_ivd(page_html: str) -> List[Dict]:
    m = re.search(r"window\['_DRIVE_ivd'\]\s*=\s*'(.+?)';", page_html, flags=re.DOTALL)
    if not m:
        return []
    raw = m.group(1)
    decoded = _js_unescape(raw)

    entries = []
    for mm in re.finditer(r'\[\s*"([a-zA-Z0-9_\-]{10,})"\s*,\s*\[\s*"[a-zA-Z0-9_\-]+"\s*\]\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"', decoded):
        file_id, name, mime = mm.group(1), mm.group(2), mm.group(3)
        if not name.strip():
            continue
        entries.append({
            "id": file_id,
            "name": name,
            "mimeType": mime,
            "webViewLink": f"https://drive.google.com/drive/folders/{file_id}" if mime.endswith(".folder") else make_view_link(file_id),
            "downloadLink": None if mime.endswith(".folder") else make_download_link(file_id),
        })
    uniq = {}
    for e in entries:
        uniq[(e["id"], e["name"])] = e
    return list(uniq.values())

def list_public_folder(folder_url: str) -> List[Dict]:
    folder_id = extract_folder_id(folder_url)
    if not folder_id:
        raise ValueError("Gagal ekstrak folder_id dari URL.")

    items = list_via_embedded(folder_id)
    if items:
        return items

    main_url = f"https://drive.google.com/drive/folders/{folder_id}"
    r = requests.get(main_url, timeout=30)
    r.raise_for_status()
    return list_via_drive_ivd(r.text)

def download_file(url: str, outpath: Path, timeout: int = 90) -> None:
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

def batch_download_json(files: List[Dict], download_dir: Path, max_workers: int = 6) -> List[Path]:
    targets = []
    for f in files:
        if not f.get("downloadLink"):
            continue
        if not f.get("name", "").lower().endswith(".json"):
            continue
        targets.append((f["name"], f["downloadLink"]))

    saved_paths: List[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {}
        for name, url in targets:
            outp = download_dir / name
            futs[ex.submit(download_file, url, outp)] = outp
        for fut in as_completed(futs):
            outp = futs[fut]
            try:
                fut.result()
                print(f"Downloaded: {outp.name}")
                saved_paths.append(outp)
            except Exception as e:
                print(f"Failed : {outp.name} -> {e}")
    return saved_paths

def safe_mean(values: List[float]) -> Optional[float]:
    vals = []
    for v in values or []:
        try:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            vals.append(float(v))
        except Exception:
            continue
    if not vals:
        return None
    return mean(vals)

def extract_messages(payload: Dict[str, Any]) -> str:
    msgs = payload.get("Messages", [])
    if not msgs:
        return ""
    out = []
    for m in msgs:
        if isinstance(m, str):
            out.append(m)
        elif isinstance(m, dict):
            out.append(m.get("Message") or m.get("message") or m.get("text") or json.dumps(m, ensure_ascii=False))
        else:
            out.append(str(m))
    return "; ".join([s for s in out if s])

def process_one_file(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = []
    message = extract_messages(payload)
    mdr_list = payload.get("MetricDataResults", []) or []

    for mdr in mdr_list:
        _id = mdr.get("Id")
        timestamps = mdr.get("Timestamps") or []
        values = mdr.get("Values") or []

        runtime_date = max(map(str, timestamps)) if timestamps else None

        valid = []
        for v in values:
            try:
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    valid.append(float(v))
            except Exception:
                pass
        sum_ms = sum(valid)
        cnt = len(valid)

        # load_time per-file (preview, bukan final)
        load_time = (sum_ms / cnt / 60000.0) if cnt else None

        rows.append({
            "id": _id,
            "runtime_date": runtime_date,  # nanti di-max lintas file
            "sum_ms": sum_ms,              # <— baru
            "cnt": cnt,                    # <— baru
            "load_time": load_time,        # preview
            "Message": message,
            "source_file": path.name,
        })
    return rows

def aggregate_one_row_per_id(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    # pastikan datetime (aman untuk max)
    df = df.copy()
    df["runtime_date"] = pd.to_datetime(df["runtime_date"], errors="coerce", utc=True)

    # gabungkan Message unik
    def uniq_msgs(s):
        vals = [x for x in s if isinstance(x, str) and x.strip()]
        return "; ".join(sorted(set(vals))) if vals else ""

    agg = df.groupby("id", as_index=False).agg(
        runtime_date=("runtime_date", "max"),
        sum_ms=("sum_ms", "sum"),
        cnt=("cnt", "sum"),
        Message=("Message", uniq_msgs),
    )

    # weighted average → menit
    agg["load_time"] = agg.apply(
        lambda r: (r["sum_ms"] / r["cnt"] / 60000.0) if r["cnt"] else None, axis=1
    )

    # format ISO-8601 (dengan +00:00)
    def fmt_iso(dt):
        if pd.isna(dt):
            return None
        s = dt.strftime("%Y-%m-%dT%H:%M:%S%z")  # +0000
        return s[:-2] + ":" + s[-2:]            # +00:00

    agg["runtime_date"] = agg["runtime_date"].apply(fmt_iso)
    return agg[["id", "runtime_date", "load_time", "Message"]]


def combine_json_to_table(download_dir: Path) -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    for p in sorted(download_dir.glob("*.json")):
        try:
            all_rows.extend(process_one_file(p))
        except Exception as e:
            print(f"Skip {p.name}: {e}")
    # jangan tetapkan columns agar sum_ms & cnt tidak hilang
    df = pd.DataFrame(all_rows)
    df = df[df["id"].notna()].copy()
    return df

def load(df: pd.DataFrame, knbistg_engine):
    # pastikan kolom dan tipe aman
    df = df.copy()
    df["id"] = df["id"].astype(str)
    df["runtime_date"] = pd.to_datetime(df["runtime_date"], errors="coerce", utc=True)

    # pastikan schema ada
    with knbistg_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"))

    # langsung replace table (drop & create) tiap run
    df.to_sql(
        name=TABLE,
        con=knbistg_engine,     # penting: SQLAlchemy Engine/Connection, bukan raw DBAPI
        schema=SCHEMA,
        if_exists="replace",    # <— replace full table
        index=False,
        dtype={
            "id": Text(),
            "runtime_date": TIMESTAMP(timezone=True),
            "load_time": Float(),
            "Message": Text(),
        },
        method="multi",
        chunksize=10_000,
    )

def main():
    print("== List isi folder (public) ==")
    files = list_public_folder(FOLDER_URL)
    if not files:
        print("Tidak menemukan item. Pastikan folder benar-benar public.")
        return
    only_json = [f for f in files if f.get("downloadLink") and f.get("name", "").lower().endswith(".json")]
    print(f"Total items : {len(files)} | JSON files: {len(only_json)}")
    for f in only_json:
        print(f"- {f['name']}")

    print("\nDownload JSON")
    saved = batch_download_json(only_json, download_dir=DOWNLOAD_DIR)
    if not saved:
        print("Tidak ada file JSON yang berhasil diunduh.")
        return

    print("\n== Combine (detail per file) ==")
    df_detail = combine_json_to_table(DOWNLOAD_DIR)
    if df_detail.empty:
        print("Tidak ada data valid dari JSON.")
        return

    # simpan detail (audit)
    detail_csv = OUT_CSV.with_name("combined_detail_per_file.csv")
    df_detail.to_csv(detail_csv, index=False)
    try:
        df_detail.to_parquet(OUT_PARQUET.with_name("combined_detail_per_file.parquet"), index=False)
    except Exception:
        pass
    print(f"OK -> Detail CSV : {detail_csv.resolve()}")

    print("\n== Aggregate (FINAL: 1 baris per id) ==")
    df_final = aggregate_one_row_per_id(df_detail)
    if df_final.empty:
        print("Aggregate kosong (tidak ada id).")
        return

    # simpan FINAL sesuai brief
    df_final.to_csv(OUT_CSV, index=False)
    try:
        df_final.to_parquet(OUT_PARQUET, index=False)
    except Exception:
        pass

    print(f"Selesai -> FINAL CSV: {OUT_CSV.resolve()}")
    if OUT_PARQUET.exists():
        print(f"Selesai -> FINAL PQ : {OUT_PARQUET.resolve()}")
    print("\nPreview FINAL:")
    print(df_final.to_string(index=False))
    engine = connection.dwh_create_db_connection()  # SQLAlchemy engine ke KNBISTG
    load(df_final, engine)
    print("Data final berhasil di-load ke database.")

if __name__ == "__main__":
    main()
