#!/usr/bin/env python3
from __future__ import annotations

'''
WES KPI Dashboard Data Puller
'''
import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yaml
from openpyxl import load_workbook
from openpyxl.workbook import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "").strip()


@dataclass(frozen=True)
class CensusGeo:
    dataset: str
    for_clause: str
    in_clause: Optional[str] = None


def census_base_url(year: int, dataset: str) -> str:
    return f"https://api.census.gov/data/{year}/acs/{dataset}"


def census_get(year: int, dataset: str, variables: List[str], geo: CensusGeo) -> pd.DataFrame:
    url = census_base_url(year, dataset)
    params = {"get": ",".join(["NAME"] + variables), "for": geo.for_clause}
    if geo.in_clause:
        params["in"] = geo.in_clause
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    header, rows = data[0], data[1:]
    df = pd.DataFrame(rows, columns=header)

    for v in variables:
        if v in df.columns:
            df[v] = pd.to_numeric(df[v], errors="coerce")
    return df


def census_variables_index(year: int, dataset: str, group: Optional[str] = None) -> Dict[str, Any]:
    if group:
        url = f"https://api.census.gov/data/{year}/acs/{dataset}/groups/{group}.json"
    else:
        url = f"https://api.census.gov/data/{year}/acs/{dataset}/variables.json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def load_geo_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def expand_geos(cfg: Dict[str, Any]) -> Dict[str, List[CensusGeo]]:
    out: Dict[str, List[CensusGeo]] = {}
    for key, spec in cfg.get("geographies", {}).items():
        if "census" in spec:
            c = spec["census"]
            out[key] = [CensusGeo(dataset=c.get("dataset", "acs5"), for_clause=c["for"], in_clause=c.get("in"))]
        elif "custom" in spec:
            members: List[CensusGeo] = []
            for m in spec["custom"]["members"]:
                c = m["census"]
                members.append(CensusGeo(dataset=c.get("dataset", "acs5"), for_clause=c["for"], in_clause=c.get("in")))
            out[key] = members
        else:
            raise ValueError(f"Geo '{key}' must have 'census' or 'custom'")
    return out


# -----------------------------
# KPI pulls
# -----------------------------
B01001_VARS = {
    "age0_4": ["B01001_003E", "B01001_027E"],
    "age5_9": ["B01001_004E", "B01001_028E"],
    "age10_14": ["B01001_005E", "B01001_029E"],
}

S1101_VAR_HH_OWN_CHILDREN_U18 = "S1101_C01_005E"
B19131_GROUP = "B19131"

B14003_VARS = {
    "pub_3_4": ["B14003_004E", "B14003_032E"],
    "pub_5_9": ["B14003_005E", "B14003_033E"],
    "pub_10_14": ["B14003_006E", "B14003_034E"],
    "priv_3_4": ["B14003_013E", "B14003_041E"],
    "priv_5_9": ["B14003_014E", "B14003_042E"],
    "priv_10_14": ["B14003_015E", "B14003_043E"],
}


def pull_pipeline(year: int, geo_cfg_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    geo_members = expand_geos(load_geo_config(geo_cfg_path))
    raw_rows, out_rows = [], []

    for geo_key, members in geo_members.items():
        agg = {"geo_key": geo_key, "age0_4": 0.0, "age5_9": 0.0, "age10_14": 0.0}
        vars_needed = sorted({v for vs in B01001_VARS.values() for v in vs})
        for m in members:
            df = census_get(year, m.dataset, vars_needed, CensusGeo(m.dataset, m.for_clause, m.in_clause))
            row = df.iloc[0].to_dict()
            row.update({"geo_key": geo_key, "member_for": m.for_clause, "member_in": m.in_clause or "", "dataset": m.dataset})
            raw_rows.append(row)

            agg["age0_4"] += float(df[B01001_VARS["age0_4"]].sum(axis=1).iloc[0])
            agg["age5_9"] += float(df[B01001_VARS["age5_9"]].sum(axis=1).iloc[0])
            agg["age10_14"] += float(df[B01001_VARS["age10_14"]].sum(axis=1).iloc[0])

        out_rows.append(agg)

    return pd.DataFrame(raw_rows), pd.DataFrame(out_rows)


def pull_households(year: int, geo_cfg_path: str, subject_dataset: str = "acs5/subject") -> Tuple[pd.DataFrame, pd.DataFrame]:
    geo_members = expand_geos(load_geo_config(geo_cfg_path))
    raw_rows, out_rows = [], []

    for geo_key, members in geo_members.items():
        agg_val = 0.0
        for m in members:
            df = census_get(year, subject_dataset, [S1101_VAR_HH_OWN_CHILDREN_U18], CensusGeo(subject_dataset, m.for_clause, m.in_clause))
            row = df.iloc[0].to_dict()
            row.update({"geo_key": geo_key, "member_for": m.for_clause, "member_in": m.in_clause or "", "dataset": subject_dataset})
            raw_rows.append(row)
            agg_val += float(df[S1101_VAR_HH_OWN_CHILDREN_U18].iloc[0])
        out_rows.append({"geo_key": geo_key, "hh_own_children_u18": agg_val})

    return pd.DataFrame(raw_rows), pd.DataFrame(out_rows)


def _select_b19131_income_vars(meta: Dict[str, Any], income_labels: List[str]) -> List[str]:
    variables = meta.get("variables", {})
    selected: List[str] = []
    for var, info in variables.items():
        if not var.endswith("E"):
            continue
        label = info.get("label", "")
        if "With own children of the householder under 18 years" not in label:
            continue
        if any(il in label for il in income_labels):
            selected.append(var)
    return sorted(set(selected))


def pull_high_income(year: int, geo_cfg_path: str, dataset: str = "acs5") -> Tuple[pd.DataFrame, pd.DataFrame]:
    geo_members = expand_geos(load_geo_config(geo_cfg_path))

    meta = census_variables_index(year, dataset, group=B19131_GROUP)
    vars_150_199 = _select_b19131_income_vars(meta, ["$150,000 to $199,999"])
    vars_200p = _select_b19131_income_vars(meta, ["$200,000 or more"])
    all_vars = sorted(set(vars_150_199 + vars_200p))
    if not all_vars:
        raise RuntimeError("Could not find B19131 vars for >=150k in this year/dataset. Try another ACS vintage.")

    raw_rows, out_rows = [], []

    for geo_key, members in geo_members.items():
        agg_150, agg_200 = 0.0, 0.0
        for m in members:
            df = census_get(year, dataset, all_vars, CensusGeo(dataset, m.for_clause, m.in_clause))
            row = df.iloc[0].to_dict()
            row.update({"geo_key": geo_key, "member_for": m.for_clause, "member_in": m.in_clause or "", "dataset": dataset})
            raw_rows.append(row)

            if vars_150_199:
                agg_150 += float(df[vars_150_199].sum(axis=1).iloc[0])
            if vars_200p:
                agg_200 += float(df[vars_200p].sum(axis=1).iloc[0])

        out_rows.append({
            "geo_key": geo_key,
            "hhkids_income_150_199": agg_150,
            "hhkids_income_200_plus": agg_200,
            "hhkids_income_150_plus": agg_150 + agg_200,
        })

    return pd.DataFrame(raw_rows), pd.DataFrame(out_rows)


def pull_chooser_rate(year: int, geo_cfg_path: str, dataset: str = "acs5") -> Tuple[pd.DataFrame, pd.DataFrame]:
    geo_members = expand_geos(load_geo_config(geo_cfg_path))
    vars_needed = sorted({v for vs in B14003_VARS.values() for v in vs})

    raw_rows, out_rows = [], []
    for geo_key, members in geo_members.items():
        pub, priv = 0.0, 0.0
        for m in members:
            df = census_get(year, dataset, vars_needed, CensusGeo(dataset, m.for_clause, m.in_clause))
            row = df.iloc[0].to_dict()
            row.update({"geo_key": geo_key, "member_for": m.for_clause, "member_in": m.in_clause or "", "dataset": dataset})
            raw_rows.append(row)

            pub += float(df[B14003_VARS["pub_3_4"]].sum(axis=1).iloc[0]) + float(df[B14003_VARS["pub_5_9"]].sum(axis=1).iloc[0]) + float(df[B14003_VARS["pub_10_14"]].sum(axis=1).iloc[0])
            priv += float(df[B14003_VARS["priv_3_4"]].sum(axis=1).iloc[0]) + float(df[B14003_VARS["priv_5_9"]].sum(axis=1).iloc[0]) + float(df[B14003_VARS["priv_10_14"]].sum(axis=1).iloc[0])

        chooser = (priv / (priv + pub)) if (priv + pub) > 0 else float("nan")
        out_rows.append({
            "geo_key": geo_key,
            "public_enrolled_5_14": pub,
            "private_enrolled_5_14": priv,
            "private_chooser_rate_5_14": chooser,
        })

    return pd.DataFrame(raw_rows), pd.DataFrame(out_rows)


# -----------------------------
# DC public alternatives component (OSSE chronic absenteeism xlsx)
# -----------------------------
def pull_osse_chronic_absenteeism(url: str) -> pd.DataFrame:
    import io
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    bio = io.BytesIO(r.content)
    df = pd.read_excel(bio)
    df["source_url"] = url
    df["pulled_at_utc"] = datetime.utcnow().isoformat()
    return df


# -----------------------------
# Excel helpers
# -----------------------------
def ensure_workbook(path: str) -> Workbook:
    if os.path.exists(path):
        return load_workbook(path)
    wb = Workbook()
    if "Sheet" in wb.sheetnames:
        wb.remove(wb["Sheet"])
    return wb


def write_df(wb: Workbook, sheet_name: str, df: pd.DataFrame, freeze: str = "A2") -> None:
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        wb.remove(ws)
    ws = wb.create_sheet(sheet_name)
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)
    ws.freeze_panes = freeze
    for col in ws.columns:
        col_letter = col[0].column_letter
        max_len = 0
        for cell in col[:50]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 45)


def build_kpi_calcs(pipeline: pd.DataFrame, households: pd.DataFrame, high_income: pd.DataFrame, chooser: pd.DataFrame) -> pd.DataFrame:
    df = pipeline.merge(households, on="geo_key", how="left") \
                 .merge(high_income, on="geo_key", how="left") \
                 .merge(chooser, on="geo_key", how="left")

    return df.rename(columns={
        "age0_4": "Age 0-4 count",
        "age5_9": "Age 5-9 count",
        "age10_14": "Age 10-14 count",
        "hh_own_children_u18": "HH w/ own children <18",
        "hhkids_income_150_plus": "High-income HH w/kids (>=150k)",
        "hhkids_income_200_plus": "High-income HH w/kids (>=200k)",
        "private_chooser_rate_5_14": "Private school chooser rate (ages 5-14)",
    })


# -----------------------------
# CLI
# -----------------------------
def cmd_pull(args: argparse.Namespace) -> None:
    wb = ensure_workbook(args.out)

    if args.category in ("pipeline", "all"):
        raw, agg = pull_pipeline(args.year, args.geo)
        write_df(wb, "RAW_ACS_B01001", raw)
        write_df(wb, "KPI_Pipeline", agg)

    if args.category in ("households", "all"):
        raw, agg = pull_households(args.year, args.geo, args.subject_dataset)
        write_df(wb, "RAW_ACS_S1101", raw)
        write_df(wb, "KPI_Households", agg)

    if args.category in ("high-income", "all"):
        raw, agg = pull_high_income(args.year, args.geo)
        write_df(wb, "RAW_ACS_B19131", raw)
        write_df(wb, "KPI_HighIncome", agg)

    if args.category in ("chooser", "all"):
        raw, agg = pull_chooser_rate(args.year, args.geo)
        write_df(wb, "RAW_ACS_B14003", raw)
        write_df(wb, "KPI_Chooser", agg)

    if args.category == "public-dc":
        if not args.osse_chronic_url:
            raise SystemExit("Provide --osse-chronic-url (direct xlsx url).")
        df = pull_osse_chronic_absenteeism(args.osse_chronic_url)
        write_df(wb, "RAW_OSSE_ChronicAbs", df)

    wb.save(args.out)
    print(f"Wrote {args.out}")


def cmd_refresh(args: argparse.Namespace) -> None:
    wb = ensure_workbook(args.out)

    raw_p, kpi_p = pull_pipeline(args.year, args.geo)
    raw_h, kpi_h = pull_households(args.year, args.geo, args.subject_dataset)
    raw_i, kpi_i = pull_high_income(args.year, args.geo)
    raw_c, kpi_c = pull_chooser_rate(args.year, args.geo)

    write_df(wb, "RAW_ACS_B01001", raw_p)
    write_df(wb, "RAW_ACS_S1101", raw_h)
    write_df(wb, "RAW_ACS_B19131", raw_i)
    write_df(wb, "RAW_ACS_B14003", raw_c)

    write_df(wb, "KPI_Pipeline", kpi_p)
    write_df(wb, "KPI_Households", kpi_h)
    write_df(wb, "KPI_HighIncome", kpi_i)
    write_df(wb, "KPI_Chooser", kpi_c)

    calcs = build_kpi_calcs(kpi_p, kpi_h, kpi_i, kpi_c)
    write_df(wb, "KPI_Calcs", calcs)

    if args.osse_chronic_url:
        df = pull_osse_chronic_absenteeism(args.osse_chronic_url)
        write_df(wb, "RAW_OSSE_ChronicAbs", df)

    wb.save(args.out)
    print(f"Refreshed {args.out}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wesdash", description="Pull WES KPI datasets into an Excel workbook.")
    sub = p.add_subparsers(dest="cmd", required=True)

    pull = sub.add_parser("pull", help="Pull one category of metrics.")
    pull.add_argument("category", choices=["pipeline", "households", "high-income", "chooser", "public-dc", "all"])
    pull.add_argument("--year", type=int, default=2023)
    pull.add_argument("--geo", type=str, default="geo.yaml")
    pull.add_argument("--out", type=str, default="wes_kpi.xlsx")
    pull.add_argument("--subject-dataset", type=str, default="acs5/subject")
    pull.add_argument("--osse-chronic-url", type=str, default="")
    pull.set_defaults(func=cmd_pull)

    refresh = sub.add_parser("refresh", help="Refresh everything and rebuild KPI_Calcs.")
    refresh.add_argument("--year", type=int, default=2023)
    refresh.add_argument("--geo", type=str, default="geo.yaml")
    refresh.add_argument("--out", type=str, default="wes_kpi.xlsx")
    refresh.add_argument("--subject-dataset", type=str, default="acs5/subject")
    refresh.add_argument("--osse-chronic-url", type=str, default="")
    refresh.set_defaults(func=cmd_refresh)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
