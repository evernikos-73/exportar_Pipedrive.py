import os
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- Configuración ---
PIPEDRIVE_API_KEY = os.environ["PIPEDRIVE_API_KEY"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
SPREADSHEET_ID = "TU_ID_DE_GOOGLE_SHEET_AQUI"  # <- Cambia por tu ID real

# Base API v1 Pipedrive
BASE_URL_V1 = "https://inprocilsa.pipedrive.com/api/v1"
HEADERS = {"x-api-token": PIPEDRIVE_API_KEY}

# Endpoints y paginación
ENDPOINTS_CONFIG = {
    "Deals": ("/deals/collection", "cursor", {}, "Pipedrive Deals"),
    "Organizations": ("/organizations/collection", "cursor", {}, "Pipedrive Organizations"),
    "Activities": ("/activities/collection", "cursor", {}, "Pipedrive Activities"),
    "Leads": ("/leads", "offset", {}, "Pipedrive Leads"),
    "Users": ("/users", "offset", {}, "Pipedrive Users"),
    "Notes": ("/notes", "offset", {}, "Pipedrive Notes"),
}

# --- Funciones ---

def fetch_data_cursor(endpoint, extra_params):
    all_data = []
    cursor = None
    url = f"{BASE_URL_V1}{endpoint}"
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

def fetch_data_offset(endpoint, extra_params):
    all_data = []
    start = 0
    limit = 100
    url = f"{BASE_URL_V1}{endpoint}"
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

def authenticate_google_sheets():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def update_sheet(sheet, dataframe):
    sheet.clear()
    sheet.update([dataframe.columns.values.tolist()] + dataframe.fillna("").astype(str).values.tolist())

def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    for name, (endpoint, pagination_type, extra_params, sheet_name) in ENDPOINTS_CONFIG.items():
        print(f"\n🔍 Procesando endpoint: {name}")
        if pagination_type == "cursor":
            data = fetch_data_cursor(endpoint, extra_params)
        else:
            data = fetch_data_offset(endpoint, extra_params)

        if not data:
            print(f"⚠️ No se obtuvieron datos de {name}")
            continue

        df = pd.DataFrame(data)
        print(f"✅ {name}: {len(df)} registros. Actualizando hoja '{sheet_name}'...")
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")

        update_sheet(worksheet, df)
        print(f"Hoja '{sheet_name}' actualizada correctamente.")

if __name__ == "__main__":
    main()

