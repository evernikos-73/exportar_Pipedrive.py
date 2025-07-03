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
        # Actualizo los parámetros con paginación y token
        parametros = params.copy()  # para no modificar el dict externo
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

def exportar_a_sheets(df, sheet_url, hoja_nombre):
    spreadsheet = client.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"✅ Exportado: {hoja_nombre}")

# 📥 Exportar negocios
df_deals = obtener_datos_paginados("deals")
df_deals = limpiar_dataframe(df_deals)
exportar_a_sheets(df_deals, "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit", "Pipedrive Deals")

# 📥 Exportar notas
df_notes = obtener_datos_paginados("notes")
df_notes = limpiar_dataframe(df_notes)
exportar_a_sheets(df_notes, "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit", "Pipedrive Notas")

# 📥 Exportar actividades (pendientes y completadas)
df_activities_pend = obtener_datos_paginados("activities", {"done": 0})
df_activities_done = obtener_datos_paginados("activities", {"done": 1})

df_activities = pd.concat([df_activities_pend, df_activities_done], ignore_index=True)
df_activities = limpiar_dataframe(df_activities)
exportar_a_sheets(df_activities, "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1c8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit", "Pipedrive Activities")

print("🎉 Exportación completa")

