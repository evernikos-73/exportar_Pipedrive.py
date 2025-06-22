import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# 📄 Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# 📌 Pipedrive API
api_token = os.environ["PIPEDRIVE_API_KEY"]
base_url = "https://api.pipedrive.com/v1/deals?api_token=" + "dc6509b45cd8b6d62c2ccdaac1a26e7c24725551"

# Obtener negocios
response = requests.get(base_url)
data = response.json()

# Convertir a DataFrame
df = pd.json_normalize(data["data"])

# Exportar a Sheets
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")
worksheet = spreadsheet.worksheet("Pipedrive Deals")
worksheet.clear()

# Limpiar datos antes de subir
df.replace([float('inf'), float('-inf')], pd.NA, inplace=True)
df.fillna("", inplace=True)

# Convertir listas/diccionarios a string
for col in df.columns:
    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# Subir a Google Sheets
worksheet.update([df.columns.values.tolist()] + df.values.tolist())

print("✅ Exportación completa")
