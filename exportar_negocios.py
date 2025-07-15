import os
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from itertools import product
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
    "Pipedrive Deals": "A:V",
    "Pipedrive Notes": "A:T",
    "Pipedrive Organizations": "A:AB",
    "Pipedrive Activities": "A:AJ",
    "Pipedrive Users": "A:T",
    "Pipedrive Analisis": "A:ZZ"
}

# --- Fetching ---
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

def update_sheet(sheet, dataframe, clear_range):
    print(f" - Borrando rango: {clear_range}")
    sheet.batch_clear([clear_range])
    sheet.update([dataframe.columns.values.tolist()] + dataframe.fillna("").astype(str).values.tolist())

# --- Normalizar fields genérico ---
def normalize_field(df, field):
    if field in df.columns:
        if isinstance(df[field].iloc[0], dict):
            df[field] = df[field].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
    elif f"{field}.id" in df.columns:
        df[field] = df[f"{field}.id"]
    else:
        df[field] = np.nan
    return df

# --- Build analysis DF ---
def build_analysis_df(df_orgs, df_activities, df_deals, df_users):
    fechas = pd.date_range("2025-01-01", "2026-12-01", freq='MS')
    orgs = df_orgs[['id', 'name']].drop_duplicates()
    orgs.columns = ['OrganizationID', 'Organization Name']
    usuarios = df_users[['id', 'name']].drop_duplicates()
    usuarios.columns = ['userId', 'UserName']

    base = pd.DataFrame(list(product(fechas, orgs['OrganizationID'], usuarios['userId'])),
                        columns=['MesAño', 'OrganizationID', 'userId'])
    base = base.merge(orgs, on='OrganizationID', how='left')
    base = base.merge(usuarios, on='userId', how='left')

    # Normalizar fields
    df_activities = normalize_field(df_activities, 'owner_id')
    df_deals = normalize_field(df_deals, 'owner_id')
    df_activities = normalize_field(df_activities, 'org_id')
    df_deals = normalize_field(df_deals, 'org_id')

    if 'done' in df_activities.columns:
        df_activities['done'] = df_activities['done'].astype(bool)
    if 'due_date' in df_activities.columns:
        df_activities['due_date'] = pd.to_datetime(df_activities['due_date'], errors='coerce')
    df_deals['add_time'] = pd.to_datetime(df_deals['add_time'], errors='coerce')
    df_deals['close_time'] = pd.to_datetime(df_deals['close_time'], errors='coerce')
    if 'status' not in df_deals.columns:
        df_deals['status'] = ""

    result = []
    for _, row in base.iterrows():
        mes = row['MesAño']
        org_id = row['OrganizationID']
        user_id = row['userId']

        act_totales = df_activities[
            (df_activities['done']) &
            (df_activities['org_id'] == org_id) &
            (df_activities['owner_id'] == user_id) &
            (df_activities['due_date'].dt.to_period('M') == mes.to_period('M'))
        ]
        deals_creados = df_deals[
            (df_deals['org_id'] == org_id) &
            (df_deals['owner_id'] == user_id) &
            (df_deals['add_time'].dt.to_period('M') == mes.to_period('M'))
        ]
        deals_ganados = df_deals[
            (df_deals['org_id'] == org_id) &
            (df_deals['owner_id'] == user_id) &
            (df_deals['status'] == 'won') &
            (df_deals['close_time'].dt.to_period('M') == mes.to_period('M'))
        ]
        deals_perdidos = df_deals[
            (df_deals['org_id'] == org_id) &
            (df_deals['owner_id'] == user_id) &
            (df_deals['status'] == 'lost') &
            (df_deals['close_time'].dt.to_period('M') == mes.to_period('M'))
        ]
        act_negocios = df_activities[
            (df_activities['done']) &
            (df_activities['deal_id'].notna()) &
            (df_activities['org_id'] == org_id) &
            (df_activities['owner_id'] == user_id) &
            (df_activities['due_date'].dt.to_period('M') == mes.to_period('M'))
        ]
        result.append({
            'MesAño': mes,
            'OrganizationID': org_id,
            'Organization Name': row['Organization Name'],
            'userId': user_id,
            'UserName': row['UserName'],
            'Actividad Total': len(act_totales),
            'Negocios creados': len(deals_creados),
            'Negocios ganados': len(deals_ganados),
            'Negocios perdidos': len(deals_perdidos),
            'Actividad Negocios': len(act_negocios)
        })
    return pd.DataFrame(result)

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
            dataframes[name] = pd.DataFrame()
            continue

        df = pd.DataFrame(data)
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
        df_orgs=dataframes["Organizations"],
        df_activities=dataframes["Activities"],
        df_deals=dataframes["Deals"],
        df_users=dataframes["Users"]
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
