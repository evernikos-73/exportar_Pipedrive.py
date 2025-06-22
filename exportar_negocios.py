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

def obtener_datos_paginados(endpoint):
    page = 0
    resultados = []
    while True:
        page += 1
        url = f"{base_url}/{endpoint}?api_token={api_token}&start={(page - 1) * 500}&limit=500"
        response = requests.get(url)
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

# 📥 Exportar actividades
df_activities = obtener_datos_paginados("activities")
df_activities = limpiar_dataframe(df_activities)
exportar_a_sheets(df_activities, "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit", "Pipeline Actividades")

print("🎉 Exportación completa")
