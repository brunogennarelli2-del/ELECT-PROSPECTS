#!/usr/bin/env python3
"""
Prospects Elettrica → connected report generator

- Reads the Excel from a live URL (SharePoint/OneDrive/Google Drive) OR a local/network path
- Detects columns (Name/Company/Role/Sector/Email/Phone/CRM)
- Flags CRM (Yes/No/Unknown)
- Differentiates channels: Email only / Phone only / Both / Neither
- Exports multi-sheet Excel report + PNG charts

Usage examples:
  python prospects_report.py "https://company.sharepoint.com/.../Prospects%20Elettrica.xlsx" --sheet JAPAN --outdir output
  python prospects_report.py "/mnt/shared/Prospects Elettrica.xlsx" --all-sheets --outdir output
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
from io import BytesIO
import re
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# -------------------- Data loading (URL or path) --------------------
def _tweak_url_for_download(url: str) -> str:
    """
    Best-effort transform for common providers to ensure a direct binary download.
    - OneDrive/SharePoint often need ?download=1
    - Google Drive 'uc?id=' links are already binary; 'open?id=' becomes 'uc?id='
    """
    u = url
    # Google Drive open?id=FILE → uc?id=FILE
    u = re.sub(r"https://drive\.google\.com/open\?id=", "https://drive.google.com/uc?id=", u)
    # Add ?download=1 if not present for sharepoint/onedrive raw file links
    if ("sharepoint.com" in u or "1drv.ms" in u or "onedrive.live.com" in u) and "download=" not in u:
        sep = "&" if "?" in u else "?"
        u = f"{u}{sep}download=1"
    return u

def load_excel_source(path_or_url: str) -> pd.ExcelFile:
    """Return a pandas ExcelFile from a URL (live) or a local path."""
    if str(path_or_url).lower().startswith(("http://", "https://")):
        url = _tweak_url_for_download(path_or_url)
        print(f"🌐 Fetching Excel from URL: {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return pd.ExcelFile(BytesIO(r.content))
    print(f"📂 Loading Excel from local path: {path_or_url}")
    return pd.ExcelFile(path_or_url)


# -------------------- Helpers --------------------
def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in df.columns:
        low = str(c).strip().lower()
        for cand in candidates:
            if cand in low:
                return c
    return None

def to_bool_crm(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip().lower()
    if s in {"yes", "y", "true", "1"}:
        return True
    if s in {"no", "n", "false", "0"}:
        return False
    return np.nan

def channel_cat(has_email: bool, has_phone: bool) -> str:
    if has_email and has_phone:
        return "Both email & phone"
    if has_email:
        return "Email only"
    if has_phone:
        return "Phone only"
    return "Neither"

def normalize_presence(series: pd.Series) -> pd.Series:
    s = series.astype(str).fillna("").str.strip()
    empties = {"n/a", "na", "-", "none", "null", ""}
    return ~s.str.lower().isin(empties)


# -------------------- Core processing --------------------
def process_sheet(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_name    = find_col(df, ["name"])
    col_company = find_col(df, ["company", "organization"])
    col_role    = find_col(df, ["role", "title", "position"])
    col_sector  = find_col(df, ["sector", "focus"])
    col_email   = find_col(df, ["email", "e-mail"])
    col_phone   = find_col(df, ["phone", "number", "mobile", "tel"])
    col_crm     = find_col(df, ["crm"])

    # Safe accessors
    def col_or_blank(col): return df[col] if col else pd.Series([""] * len(df))
    def col_or_nan(col):   return df[col] if col else pd.Series([np.nan] * len(df))

    email_series = col_or_blank(col_email)
    phone_series = col_or_blank(col_phone)
    crm_series   = col_or_nan(col_crm)

    has_email = normalize_presence(email_series)
    has_phone = normalize_presence(phone_series)
    crm_flag  = crm_series.map(to_bool_crm)

    channel = [channel_cat(e, p) for e, p in zip(has_email, has_phone)]

    clean = pd.DataFrame({
        "Name": df[col_name] if col_name else pd.Series([np.nan]*len(df)),
        "Company": df[col_company] if col_company else pd.Series([np.nan]*len(df)),
        "Role": df[col_role] if col_role else pd.Series([np.nan]*len(df)),
        "Sector Focus": df[col_sector] if col_sector else pd.Series([np.nan]*len(df)),
        "Email": email_series.astype(str),
        "Phone": phone_series.astype(str),
        "In CRM?": crm_flag,               # True / False / NaN (Unknown)
        "Has Email": has_email,            # boolean flags
        "Has Phone": has_phone,
        "Contact Channel": channel,        # category
    })

    # Summaries
    summary_channel = (clean["Contact Channel"]
                       .value_counts(dropna=False)
                       .rename_axis("Contact Channel")
                       .reset_index(name="Count"))

    if col_crm is not None and not clean["In CRM?"].isna().all():
        summary_crm = (clean["In CRM?"]
                       .value_counts(dropna=False)
                       .rename_axis("In CRM?")
                       .reset_index(name="Count"))
        summary_cross = pd.crosstab(clean["In CRM?"], clean["Contact Channel"]).reset_index()
    else:
        summary_crm = pd.DataFrame({"In CRM?": ["Unknown"], "Count": [len(clean)]})
        summary_cross = pd.DataFrame()

    # Sector x Channel (if sector exists)
    if col_sector:
        sector_channel = pd.crosstab(clean["Sector Focus"], clean["Contact Channel"])
        sector_channel["Total"] = sector_channel.sum(axis=1)
        sector_channel = sector_channel.sort_values(by="Total", ascending=False)
    else:
        sector_channel = pd.DataFrame()

    return {
        "clean": clean,
        "summary_channel": summary_channel,
        "summary_crm": summary_crm,
        "summary_cross": summary_cross,
        "sector_channel": sector_channel,
    }


# -------------------- Output helpers --------------------
def save_charts(outdir: Path, tag: str, summary_channel: pd.DataFrame, summary_crm: pd.DataFrame, clean: pd.DataFrame):
    outdir.mkdir(parents=True, exist_ok=True)

    # Channel bar
    plt.figure()
    summary_channel.set_index("Contact Channel")["Count"].plot(kind="bar")
    plt.title("Contacts by Communication Channel")
    plt.xlabel("Channel")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(outdir / f"{tag}_channel_distribution.png")
    plt.close()

    # CRM bar
    plt.figure()
    summary_crm.set_index("In CRM?")["Count"].plot(kind="bar")
    plt.title("Contacts in CRM (Yes/No/Unknown)")
    plt.xlabel("In CRM?")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(outdir / f"{tag}_crm_distribution.png")
    plt.close()

    # Stacked: Channel vs CRM
    pivot = pd.crosstab(clean["Contact Channel"], clean["In CRM?"])
    if not pivot.empty:
        pivot.apply(pd.to_numeric, errors="coerce").fillna(0).plot(kind="bar", stacked=True)
        plt.title("Channel vs CRM Status")
        plt.xlabel("Contact Channel")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(outdir / f"{tag}_channel_vs_crm.png")
        plt.close()

def write_report(outpath: Path, results_by_sheet: dict[str, dict]):
    with pd.ExcelWriter(outpath, engine="xlsxwriter") as writer:
        for sheet, res in results_by_sheet.items():
            res["clean"].to_excel(writer, sheet_name=f"{sheet} - Cleaned", index=False)
            res["summary_channel"].to_excel(writer, sheet_name=f"{sheet} - Channel", index=False)
            res["summary_crm"].to_excel(writer, sheet_name=f"{sheet} - CRM", index=False)
            if not res["summary_cross"].empty:
                res["summary_cross"].to_excel(writer, sheet_name=f"{sheet} - CRM x Channel", index=False)
            if not res["sector_channel"].empty:
                res["sector_channel"].to_excel(writer, sheet_name=f"{sheet} - Sector x Channel")


# -------------------- CLI --------------------
def main():
    ap = argparse.ArgumentParser(description="Generate CRM/Channel report from a connected Prospects Excel (URL or path).")
    ap.add_argument("excel", help="URL or path to the Excel file (e.g., SharePoint/OneDrive link or /mnt/shared/Prospects.xlsx)")
    sc = ap.add_mutually_exclusive_group()
    sc.add_argument("--sheet", help="Process a single sheet name")
    sc.add_argument("--all-sheets", action="store_true", help="Process all sheets")
    ap.add_argument("--outdir", default="output", help="Output directory (default: output)")
    ap.add_argument("--report-name", default="Prospects_Report.xlsx", help="Report filename")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load Excel from URL or local path
    try:
        xls = load_excel_source(args.excel)
    except Exception as e:
        print(f"❌ Could not open Excel: {e}", file=sys.stderr)
        sys.exit(1)

    if args.sheet:
        sheets = [args.sheet]
    elif args.all_sheets:
        sheets = xls.sheet_names
    else:
        sheets = [xls.sheet_names[0]]

    results_by_sheet: dict[str, dict] = {}
    for sheet in sheets:
        print(f"🔎 Processing sheet: {sheet}")
        df = pd.read_excel(xls, sheet_name=sheet)
        res = process_sheet(df)
        results_by_sheet[sheet] = res
        save_charts(outdir, tag=sheet.replace(" ", "_"),
                    summary_channel=res["summary_channel"],
                    summary_crm=res["summary_crm"],
                    clean=res["clean"])

    report_path = outdir / args.report_name
    write_report(report_path, results_by_sheet)
    print(f"✅ Report written to: {report_path.resolve()}")
    print(f"📊 Charts saved in:  {outdir.resolve()}")

if __name__ == "__main__":
    main()
