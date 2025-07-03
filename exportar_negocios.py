import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# 🔐 Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# 📌 Pipedrive API Token
api_token = os.environ["PIPEDRIVE_API_KEY"]
base_url = "https://api.pipedrive.com/v1"

def obtener_datos_paginados(endpoint, params=None):
    page = 0
    resultados = []
    if params is None:
        params = {}
    while True:
        page += 1
        parametros = params.copy()
        parametros.update({
            "api_token": api_token,
            "start": (page - 1) * 500,
            "limit": 500
        })
        url = f"{base_url}/{endpoint}"
        response = requests.get(url, params=parametros)
        data = response.json()
        if not data.get("data"):
            break
        resultados.extend(data["data"])
        if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            break
    return pd.json_normalize(resultados)

def limpiar_dataframe(df):
    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
    df.fillna("", inplace=True)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

def exportar_a_sheets_limited(df, sheet_url, hoja_nombre, max_cols_letter):
    spreadsheet = client.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(hoja_nombre)
    # Recortar dataframe solo a las primeras N columnas
    col_limit = ord(max_cols_letter) - ord('A') + 1 if len(max_cols_letter) == 1 else \
                (ord(max_cols_letter[0]) - ord('A') + 1) * 26 + (ord(max_cols_letter[1]) - ord('A') + 1)
    df_limit = df.iloc[:, :col_limit]
    # Definir rango
    range_notation = f"A1:{max_cols_letter}"
    worksheet.update(range_notation, [df_limit.columns.tolist()] + df_limit.values.tolist())
    print(f"✅ Exportado: {hoja_nombre} hasta columna {max_cols_letter}")

def exportar_completo(df, sheet_url, hoja_nombre):
    spreadsheet = client.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"✅ Exportado: {hoja_nombre}")

# 🔗 URL de tu hoja de cálculo
sheet_url = "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit"

# 📥 Exportar negocios (hasta CR)
df_deals = obtener_datos_paginados("deals")
df_deals = limpiar_dataframe(df_deals)
exportar_a_sheets_limited(df_deals, sheet_url, "Pipedrive Deals", "CR")

# 📥 Exportar notas (hasta AA)
df_notes = obtener_datos_paginados("notes")
df_notes = limpiar_dataframe(df_notes)
exportar_a_sheets_limited(df_notes, sheet_url, "Pipedrive Notas", "AA")

# 📥 Exportar actividades (hasta DA)
df_activities = obtener_datos_paginados("activities", {"user_id": 0})
df_activities = limpiar_dataframe(df_activities)
exportar_a_sheets_limited(df_activities, sheet_url, "Pipedrive Activities", "DA")

# 📥 Exportar usuarios completo (sin límite)
df_users = obtener_datos_paginados("users")
df_users = limpiar_dataframe(df_users)
exportar_completo(df_users, sheet_url, "Pipedrive Users")

print("🎉 Exportación completa")
