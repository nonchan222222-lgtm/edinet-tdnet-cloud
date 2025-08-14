#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
EDINET（有価証券報告書）とTDnet（決算短信）を一括ダウンロード。
- 会社名 or 証券コードの指定に対応（--codes）。
- 保存先は --out で指定、未指定なら .env の OUTPUT_DIR があれば優先、無ければ ./downloads。
- EDINET: 公式API v2（APIキー必須、.env の EDINET_API_KEY）。
- TDnet: 非公式のRSS/Atom（やのしんTDnet WEB-API）を利用（キー不要）。

使い方例：
  python edinet_tdnet_downloader.py --codes 7203,6758 --start 2025-08-01 --end 2025-08-14
  python edinet_tdnet_downloader.py --codes トヨタ自動車,ソニー --start 2025-08-01 --end 2025-08-14 --out C:\\data\\gpt_analysis
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import feedparser
import requests
from dateutil.parser import isoparse
from dotenv import load_dotenv
from tqdm import tqdm

# ---- 定数 ----
EDINET_LIST_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
EDINET_GET_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"

# 有報（本則：ordinanceCode=010 & formCode=030000）、四半期：043000/043001
EDINET_FILTERS = {
    "yuho": lambda r: str(r.get("ordinanceCode")) == "010" and str(r.get("formCode")) == "030000",
    "quarter": lambda r: str(r.get("ordinanceCode")) == "010" and str(r.get("formCode")) in {"043000", "043001"},
}

EDINET_TYPE_MAP = {"xbrl": 1, "pdf": 2, "csv": 5}
SAFE_NAME = re.compile(r"[^\w\-\.\u3000-\u9FFF]+", re.UNICODE)
CODE4 = re.compile(r"^\d{4}$")

# ---- オプション定義 ----
@dataclass
class Options:
    start: dt.date
    end: dt.date
    out: Path
    edinet_filetypes: List[str]
    include_yuho: bool
    include_quarter: bool
    tdnet: bool
    tdnet_limit: int
    sec_codes: Optional[set]
    name_pats: Optional[List[re.Pattern]]
    api_key: Optional[str]
    max_retries: int = 3
    sleep_sec: float = 0.6


# ---- ユーティリティ ----

def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def safe_filename(name: str) -> str:
    name = SAFE_NAME.sub("_", name).strip("._ ")
    return name[:180]


def http_get(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None,
             max_retries: int = 3, timeout: int = 60) -> requests.Response:
    last_exc: Optional[Exception] = None
    for _ in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code in {429, 500, 502, 503, 504}:
                time.sleep(1.5)
                continue
            r.raise_for_status()
        except Exception as e:
            last_exc = e
            time.sleep(1.5)
    if last_exc:
        raise last_exc
    raise RuntimeError("HTTPリクエスト失敗")


def parse_codes_names(arg: Optional[str]) -> Tuple[Optional[set], Optional[List[re.Pattern]]]:
    if not arg:
        return None, None
    sec_codes, name_pats = set(), []
    for raw in arg.split(","):
        tok = raw.strip()
        if not tok:
            continue
        if CODE4.match(tok):
            sec_codes.add(tok)
        else:
            # 大文字小文字・全半角差異を避けるため単純に部分一致（日本語名もOK）
            name_pats.append(re.compile(re.escape(tok), re.IGNORECASE))
    return (sec_codes or None), (name_pats or None)


# ---- 引数処理 ----

def parse_args() -> Options:
    p = argparse.ArgumentParser(description="EDINET + TDnet ダウンローダー（銘柄指定対応）")
    p.add_argument("--start", required=True, help="開始日 YYYY-MM-DD")
    p.add_argument("--end", required=True, help="終了日 YYYY-MM-DD")
    p.add_argument("--out", default="downloads", help="保存ルートディレクトリ。省略時は .env の OUTPUT_DIR を優先")
    p.add_argument("--edinet-filetypes", default="pdf", help="カンマ区切り: pdf,csv,xbrl から選択（複数可）")
    p.add_argument("--include-yuho", default="yes", help="有価証券報告書を含める yes/no")
    p.add_argument("--include-quarter", default="no", help="四半期報告書を含める yes/no")
    p.add_argument("--tdnet", default="yes", help="TDnet（決算短信）も取得 yes/no")
    p.add_argument("--tdnet-limit", type=int, default=1000, help="TDnetフィード最大件数（recent用）")
    p.add_argument("--codes", default="", help="会社名または4桁証券コードをカンマ区切りで（例: 7203,トヨタ自動車）")
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--sleep-sec", type=float, default=0.6)

    a = p.parse_args()

    start = dt.date.fromisoformat(a.start)
    end = dt.date.fromisoformat(a.end)
    if end < start:
        raise SystemExit("--end は --start 以降の日付にしてください")

    filetypes = [s.strip().lower() for s in a.edinet_filetypes.split(",") if s.strip()]
    for ft in filetypes:
        if ft not in EDINET_TYPE_MAP:
            raise SystemExit(f"不明なファイル種別: {ft}（pdf,csv,xbrl から選択）")

    load_dotenv()  # .env 読み込み（EDINET_API_KEY, OUTPUT_DIR）
    api_key = os.getenv("EDINET_API_KEY")
    env_out = os.getenv("OUTPUT_DIR")
    out_path = Path(a.out if a.out != "downloads" or not env_out else env_out)

    sec_codes, name_pats = parse_codes_names(a.codes)

    return Options(
        start=start,
        end=end,
        out=out_path,
        edinet_filetypes=filetypes,
        include_yuho=a.include_yuho.lower() in {"yes", "y", "true", "1"},
        include_quarter=a.include_quarter.lower() in {"yes", "y", "true", "1"},
        tdnet=a.tdnet.lower() in {"yes", "y", "true", "1"},
        tdnet_limit=a.tdnet_limit,
        sec_codes=sec_codes,
        name_pats=name_pats,
        api_key=api_key,
        max_retries=a.max_retries,
        sleep_sec=a.sleep_sec,
    )


# ---- EDINET ----

def edinet_fetch_list(target_date: dt.date, api_key: str, max_retries: int) -> List[dict]:
    params = {"date": target_date.isoformat(), "type": 2, "Subscription-Key": api_key}
    r = http_get(EDINET_LIST_URL, params=params, max_retries=max_retries)
    j = r.json()
    return j.get("results", []) or []


def match_company(name: str, pats: Optional[List[re.Pattern]]) -> bool:
    if not pats:
        return False
    text = name or ""
    for pat in pats:
        if pat.search(text):
            return True
    return False


def edinet_pick(records: List[dict], include_yuho: bool, include_quarter: bool,
                sec_codes: Optional[set], name_pats: Optional[List[re.Pattern]]) -> List[dict]:
    picked: List[dict] = []
    for rec in records:
        sec = str(rec.get("secCode") or "")
        issuer = str(rec.get("filerName") or "")
        # 種別フィルタ
        ok_type = (
            (include_yuho and EDINET_FILTERS["yuho"](rec)) or
            (include_quarter and EDINET_FILTERS["quarter"](rec))
        )
        if not ok_type:
            continue
        # 銘柄フィルタ（指定があれば OR 条件でマッチ）
        if sec_codes or name_pats:
            if not ((sec_codes and sec in sec_codes) or match_company(issuer, name_pats)):
                continue
        picked.append(rec)
    return picked


def edinet_download(doc: dict, out_dir: Path, api_key: str, filetypes: List[str],
                    sleep_sec: float, max_retries: int) -> List[Path]:
    saved: List[Path] = []
    doc_id = doc.get("docID")
    issuer = doc.get("filerName") or ""
    sec = str(doc.get("secCode") or "")
    for ft in filetypes:
        t = EDINET_TYPE_MAP[ft]
        url = EDINET_GET_URL.format(doc_id=doc_id)
        params = {"type": t, "Subscription-Key": api_key}
        r = http_get(url, params=params, max_retries=max_retries)
        ext = ".zip" if ft in {"xbrl", "csv"} else ".pdf"
        base = f"{sec}-{safe_filename(issuer)}-{doc_id}-{ft}"
        path = out_dir / f"{base}{ext}"
        path.write_bytes(r.content)
        saved.append(path)
        time.sleep(sleep_sec)
    return saved


# ---- TDnet（非公式RSS/Atom） ----

def tdnet_feed_urls(start: dt.date, end: dt.date, limit: int) -> List[str]:
    if start == end:
        return [f"{TDNET_BASE}/{start:%Y%m%d}.atom"]
    else:
        return [f"{TDNET_BASE}/{start:%Y%m%d}-{end:%Y%m%d}.atom",
                f"{TDNET_BASE}/recent.atom?limit={limit}"]


def tdnet_entry_matches(e, sec_codes: Optional[set], name_pats: Optional[List[re.Pattern]]) -> bool:
    title = str(e.get("title", ""))
    summary = str(e.get("summary", ""))
    # 証券コード（4桁）らしきものを拾う
    m = re.search(r"(?<!\d)(\d{4})(?!\d)", " ".join([title, summary]))
    code = m.group(1) if m else None
    # マッチ条件：コードが指定されていて一致  または  会社名パターンがタイトル/概要にヒット
    if sec_codes and code and code in sec_codes:
        return True
    if name_pats:
        blob = title + " " + summary
        for pat in name_pats:
            if pat.search(blob):
                return True
    # 指定がない場合は全件OK（この関数が呼ばれるのはフィルタ時のみ）
    return not (sec_codes or name_pats)


def tdnet_download_dec_summary(start: dt.date, end: dt.date, out_root: Path,
                               sec_codes: Optional[set], name_pats: Optional[List[re.Pattern]],
                               sleep_sec: float) -> List[Path]:
    saved: List[Path] = []
    urls = tdnet_feed_urls(start, end, limit=1000)
    seen_links = set()
    for url in urls:
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = str(e.get("title", ""))
            if "決算短信" not in title:
                continue
            link = e.get("link") or ""
            if not link or link in seen_links:
                continue
            if not tdnet_entry_matches(e, sec_codes, name_pats):
                continue
            seen_links.add(link)
            try:
                r = http_get(link, max_retries=3)
            except Exception:
                continue
            pub_dt = None
            if e.get("published"):
                try:
                    pub_dt = isoparse(e.published).date()
                except Exception:
                    pub_dt = start
            day = pub_dt or start
            dir_day = out_root / "TDNET" / f"{day:%Y-%m-%d}"
            dir_day.mkdir(parents=True, exist_ok=True)
            fname = f"{safe_filename(title)}.pdf"
            path = dir_day / fname
            path.write_bytes(r.content)
            saved.append(path)
            time.sleep(sleep_sec)
    return saved


# ---- ログ ----

def append_log(index_csv: Path, rows: List[List[str]]):
    index_csv.parent.mkdir(parents=True, exist_ok=True)
    newfile = not index_csv.exists()
    with index_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if newfile:
            w.writerow(["source", "date", "secCode", "issuer", "docID", "desc", "files"])
        w.writerows(rows)


# ---- メイン ----

def main():
    opt = parse_args()

    # 出力ルート
    opt.out.mkdir(parents=True, exist_ok=True)
    log_csv = opt.out / "index.csv"

    # EDINET
    if opt.include_yuho or opt.include_quarter:
        if not opt.api_key:
            raise SystemExit("EDINET_API_KEY が未設定です。 .env を作成し値を設定してください。")
        for day in tqdm(list(daterange(opt.start, opt.end)), desc="EDINET list"):
            try:
                records = edinet_fetch_list(day, opt.api_key, opt.max_retries)
            except Exception as e:
                print(f"[WARN] EDINET一覧取得失敗 {day}: {e}")
                continue
            picked = edinet_pick(records, opt.include_yuho, opt.include_quarter, opt.sec_codes, opt.name_pats)
            if not picked:
                continue
            day_dir = opt.out / "EDINET" / f"{day:%Y-%m-%d}"
            day_dir.mkdir(parents=True, exist_ok=True)
            rows = []
            for rec in tqdm(picked, desc=f"EDINET dl {day}", leave=False):
                try:
                    paths = edinet_download(rec, day_dir, opt.api_key, opt.edinet_filetypes, opt.sleep_sec, opt.max_retries)
                except Exception as e:
                    print(f"[WARN] EDINETダウンロード失敗 docID={rec.get('docID')}: {e}")
                    continue
                rows.append([
                    "EDINET", day.isoformat(), str(rec.get("secCode") or ""),
                    str(rec.get("filerName") or ""), str(rec.get("docID") or ""),
                    str(rec.get("docDescription") or ""), " | ".join(str(p) for p in paths)
                ])
            if rows:
                append_log(log_csv, rows)

    # TDnet（決算短信）
    if opt.tdnet:
        try:
            saved = tdnet_download_dec_summary(opt.start, opt.end, opt.out, opt.sec_codes, opt.name_pats, opt.sleep_sec)
        except Exception as e:
            print(f"[WARN] TDnet取得でエラー: {e}")
            saved = []
        if saved:
            rows = [["TDNET", opt.start.isoformat()+"~"+opt.end.isoformat(), "", "", "", "決算短信", " | ".join(str(p) for p in saved)]]
            append_log(log_csv, rows)

    print("完了")


if __name__ == "__main__":
    main()
