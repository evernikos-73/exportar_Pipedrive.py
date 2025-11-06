import os
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import numpy as np

# --- Configuración global ---
PIPEDRIVE_API_KEY = os.environ["PIPEDRIVE_API_KEY"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
SPREADSHEET_ID = "1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20"

BASE_URL_V1 = "https://inprocilsa.pipedrive.com/api/v1"
BASE_URL_V2 = "https://inprocilsa.pipedrive.com/api/v2"
HEADERS = {"x-api-token": PIPEDRIVE_API_KEY}

ENDPOINTS_CONFIG = {
    "Deals": (
        "/deals",
        "cursor",
        {"include_fields": "first_won_time,products_count,activities_count,done_activities_count"},
        "Pipedrive Deals",
        BASE_URL_V2
    ),
    "Organizations": ("/organizations/collection", "cursor", {}, "Pipedrive Organizations", BASE_URL_V1),
    "Activities": ("/activities", "cursor", {}, "Pipedrive Activities", BASE_URL_V2),
    "Leads": ("/leads", "offset", {}, "Pipedrive Leads", BASE_URL_V1),
    "Users": ("/users", "offset", {}, "Pipedrive Users", BASE_URL_V1),
    "Notes": ("/notes", "offset", {}, "Pipedrive Notes", BASE_URL_V1),
}

CLEAR_RANGES = {
    "Pipedrive Deals": "A:AK",
    "Pipedrive Notes": "A:T",
    "Pipedrive Organizations": "A:AB",
    "Pipedrive Activities": "A:AA",
    "Pipedrive Users": "A:T",
    "Pipedrive Analisis": "A:J"
}

# --- Utilidades ---
def coerce_datetimes(df: pd.DataFrame, cols):
    """Convierte columnas a datetime (UTC->naive) de forma segura."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
            df[c] = df[c].dt.tz_localize(None)
    return df

def fetch_data_cursor(url, extra_params):
    all_data = []
    cursor = None
    while True:
        params = extra_params.copy()
        if cursor:
            params["cursor"] = cursor
        params["limit"] = 100
        print(f"Consultando cursor: {url} params={params}")
        response = requests.get(url, headers=HEADERS, params=params)
        print(f"Status code: {response.status_code}")
        data = response.json()
        if not data.get("success"):
            print(f"Error API: {data.get('error')}")
            break
        items = data.get("data", [])
        if not items:
            break
        all_data.extend(items)
        cursor = data.get("additional_data", {}).get("next_cursor")
        if not cursor:
            break
    return all_data

def fetch_data_offset(url, extra_params):
    all_data = []
    start = 0
    limit = 100
    while True:
        params = {"start": start, "limit": limit}
        params.update(extra_params)
        print(f"Consultando offset: {url} params={params}")
        response = requests.get(url, headers=HEADERS, params=params)
        print(f"Status code: {response.status_code}")
        data = response.json()
        if not data.get("success"):
            print(f"Error API: {data.get('error')}")
            break
        items = data.get("data", [])
        if not items:
            break
        all_data.extend(items)
        pagination = data.get("additional_data", {}).get("pagination", {})
        if not pagination.get("more_items_in_collection"):
            break
        start = pagination.get("next_start", start + limit)
    return all_data

# --- Google Sheets ---
def authenticate_google_sheets():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def update_sheet(sheet, dataframe: pd.DataFrame, clear_range: str):
    print(f" - Borrando rango: {clear_range}")
    sheet.batch_clear([clear_range])
    if dataframe is None or dataframe.empty:
        # Escribimos solo headers mínimos para dejar la hoja usable
        sheet.update([["Sin datos"]])
        return
    sheet.update([dataframe.columns.values.tolist()] + dataframe.fillna("").astype(str).values.tolist())

# --- Build analysis DF ---
def build_analysis_df(df_orgs, df_activities, df_deals, df_users):
    # Asegurar datetime en columnas clave (segunda línea de defensa)
    if not df_activities.empty:
        df_activities = coerce_datetimes(df_activities, ["due_date", "add_time", "update_time", "marked_as_done_time"])
    if not df_deals.empty:
        df_deals = coerce_datetimes(df_deals, ["add_time", "close_time", "update_time", "expected_close_date", "first_won_time"])

    partes = []
    if not df_activities.empty and "due_date" in df_activities.columns:
        partes.append(df_activities["due_date"])
    if not df_deals.empty and "add_time" in df_deals.columns:
        partes.append(df_deals["add_time"])
    if not df_deals.empty and "close_time" in df_deals.columns:
        partes.append(df_deals["close_time"])

    if not partes:
        return pd.DataFrame()

    all_dates = pd.concat(partes).dropna()
    if all_dates.empty:
        return pd.DataFrame()

    # Periodos mensuales seguros (evita .replace sobre strings)
    min_month = all_dates.dt.to_period("M").min()
    max_month = pd.Timestamp.now().to_period("M")
    _fechas = pd.period_range(min_month, max_month, freq="M").to_timestamp()

    # Columnas de mes
    if not df_activities.empty and "due_date" in df_activities.columns:
        df_activities["mes"] = df_activities["due_date"].dt.to_period("M")
    else:
        df_activities = pd.DataFrame(columns=["mes", "org_id", "owner_id", "deal_id", "done"])

    if not df_deals.empty:
        df_deals["mes_add"] = df_deals["add_time"].dt.to_period("M")
        df_deals["mes_close"] = df_deals["close_time"].dt.to_period("M")
    else:
        df_deals = pd.DataFrame(columns=["mes_add", "mes_close", "org_id", "owner_id", "status"])

    # Agrupaciones
    done_mask = df_activities.get("done", False) == True
    act_total = (df_activities[done_mask]
                 .groupby(["mes", "org_id", "owner_id"])
                 .size()
                 .reset_index(name="Actividad Total")) if not df_activities.empty else pd.DataFrame(columns=["mes","org_id","owner_id","Actividad Total"])

    act_negocios = (df_activities[done_mask & df_activities["deal_id"].notna()]
                    .groupby(["mes", "org_id", "owner_id"])
                    .size()
                    .reset_index(name="Actividad Negocios")) if not df_activities.empty and "deal_id" in df_activities.columns else pd.DataFrame(columns=["mes","org_id","owner_id","Actividad Negocios"])

    deals_creados = (df_deals.groupby(["mes_add", "org_id", "owner_id"])
                     .size()
                     .reset_index(name="Negocios creados")
                     .rename(columns={"mes_add": "mes"})) if not df_deals.empty else pd.DataFrame(columns=["mes","org_id","owner_id","Negocios creados"])

    deals_ganados = (df_deals[df_deals.get("status") == "won"]
                     .groupby(["mes_close", "org_id", "owner_id"])
                     .size()
                     .reset_index(name="Negocios ganados")
                     .rename(columns={"mes_close": "mes"})) if not df_deals.empty else pd.DataFrame(columns=["mes","org_id","owner_id","Negocios ganados"])

    deals_perdidos = (df_deals[df_deals.get("status") == "lost"]
                      .groupby(["mes_close", "org_id", "owner_id"])
                      .size()
                      .reset_index(name="Negocios perdidos")
                      .rename(columns={"mes_close": "mes"})) if not df_deals.empty else pd.DataFrame(columns=["mes","org_id","owner_id","Negocios perdidos"])

    # Unión y filtro de filas vacías
    df_analysis = pd.concat(
        [act_total, act_negocios, deals_creados, deals_ganados, deals_perdidos],
        ignore_index=True
    )

    if df_analysis.empty:
        return pd.DataFrame()

    df_analysis = df_analysis.groupby(["mes", "org_id", "owner_id"], as_index=False).sum(numeric_only=True).fillna(0)

    count_cols = ["Actividad Total", "Actividad Negocios", "Negocios creados", "Negocios ganados", "Negocios perdidos"]
    for c in count_cols:
        if c not in df_analysis.columns:
            df_analysis[c] = 0

    keep = df_analysis[count_cols].sum(axis=1) > 0
    df_analysis = df_analysis[keep]

    # MesAño a timestamp
    df_analysis["MesAño"] = df_analysis["mes"].dt.to_timestamp()
    df_analysis = df_analysis.drop(columns=["mes"])

    # Nombres
    if not df_orgs.empty and {"id","name"}.issubset(df_orgs.columns):
        orgs = df_orgs[["id", "name"]].drop_duplicates().rename(columns={"id": "OrganizationID", "name": "Organization Name"})
    else:
        orgs = pd.DataFrame(columns=["OrganizationID","Organization Name"])

    if not df_users.empty and {"id","name"}.issubset(df_users.columns):
        usuarios = df_users[["id", "name"]].drop_duplicates().rename(columns={"id": "userId", "name": "UserName"})
    else:
        usuarios = pd.DataFrame(columns=["userId","UserName"])

    df_analysis = df_analysis.merge(orgs, left_on="org_id", right_on="OrganizationID", how="left")
    df_analysis = df_analysis.merge(usuarios, left_on="owner_id", right_on="userId", how="left")

    cols = ["MesAño", "OrganizationID", "Organization Name", "userId", "UserName"] + count_cols
    for c in cols:
        if c not in df_analysis.columns:
            df_analysis[c] = np.nan

    df_analysis = df_analysis[cols].sort_values(["MesAño","OrganizationID","userId"], kind="stable").reset_index(drop=True)
    return df_analysis

# --- Main ---
def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    dataframes = {}

    for name, (endpoint, pagination_type, extra_params, sheet_name, base_url) in ENDPOINTS_CONFIG.items():
        print(f"\n🔍 Procesando endpoint: {name}")
        if pagination_type == "cursor":
            data = fetch_data_cursor(base_url + endpoint, extra_params)
        else:
            data = fetch_data_offset(base_url + endpoint, extra_params)

        if not data:
            print(f"⚠️ No se obtuvieron datos de {name}")
            df = pd.DataFrame()
            dataframes[name] = df
        else:
            df = pd.DataFrame(data)
            # Coerción de fechas por endpoint (primera línea de defensa)
            name_lower = name.lower()
            if name_lower == "activities":
                df = coerce_datetimes(df, ["due_date", "add_time", "update_time", "marked_as_done_time"])
            elif name_lower == "deals":
                df = coerce_datetimes(df, ["add_time", "close_time", "update_time", "expected_close_date", "first_won_time"])
            elif name_lower == "notes":
                df = coerce_datetimes(df, ["add_time", "update_time"])
            elif name_lower == "organizations":
                df = coerce_datetimes(df, ["add_time", "update_time"])
            elif name_lower == "leads":
                df = coerce_datetimes(df, ["add_time", "update_time"])

            dataframes[name] = df

        print(f"✅ {name}: {len(df)} registros. Actualizando hoja '{sheet_name}'...")
        clear_range = CLEAR_RANGES.get(sheet_name, "A:ZZ")
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
        update_sheet(worksheet, df, clear_range)
        print(f"Hoja '{sheet_name}' actualizada correctamente.")

    # --- Crear y subir dataframe de análisis ---
    df_analysis = build_analysis_df(
        df_orgs=dataframes.get("Organizations", pd.DataFrame()),
        df_activities=dataframes.get("Activities", pd.DataFrame()),
        df_deals=dataframes.get("Deals", pd.DataFrame()),
        df_users=dataframes.get("Users", pd.DataFrame())
    )

    print(f"\n✅ Análisis generado con {len(df_analysis)} filas")
    try:
        ws_analysis = spreadsheet.worksheet("Pipedrive Analisis")
    except gspread.exceptions.WorksheetNotFound:
        ws_analysis = spreadsheet.add_worksheet(title="Pipedrive Analisis", rows="1000", cols="50")
    update_sheet(ws_analysis, df_analysis, CLEAR_RANGES["Pipedrive Analisis"])
    print("Hoja 'Pipedrive Analisis' actualizada correctamente.")

if __name__ == "__main__":
    main()
