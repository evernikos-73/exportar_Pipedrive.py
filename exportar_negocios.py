import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# 🔐 Google Sheets Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
    client = gspread.authorize(creds)
except KeyError:
    print("⚠️ Variable de entorno 'GOOGLE_CREDENTIALS_JSON' no encontrada. Configúrala en tu entorno.")
    exit(1)
except json.JSONDecodeError:
    print("⚠️ Error decodificando el JSON de 'GOOGLE_CREDENTIALS_JSON'. Verifica su formato.")
    exit(1)
except Exception as e:
    print(f"⚠️ Error en la autenticación de Google Sheets: {e}")
    exit(1)

# 📌 Pipedrive API Token
try:
    api_token = os.environ["PIPEDRIVE_API_KEY"]
except KeyError:
    print("⚠️ Variable de entorno 'PIPEDRIVE_API_KEY' no encontrada. Configúrala en tu entorno.")
    exit(1)
base_url = "https://api.pipedrive.com/v2"  # Usando v2

def obtener_datos_paginados(endpoint, params=None):
    page = 0
    resultados = []
    if params is None:
        params = {}
    params.update({"api_token": api_token})  # Añadir token a los parámetros
    while True:
        page += 1
        params.update({
            "start": (page - 1) * 500,
            "limit": 500
        })
        url = f"{base_url}/{endpoint}"
        response = requests.get(url, params=params)
        print(f"🔍 Solicitud a {url} con params {params} - Código de estado: {response.status_code}")
        if response.status_code != 200:
            print(f"⚠️ Error en la solicitud: {response.text}")
            break
        try:
            data = response.json()
            if not data.get("items"):  # Verificar si hay items en v2
                print("⚠️ No se encontraron items en la respuesta.")
                break
            resultados.extend(data["items"])
            next_page_token = data.get("additional_data", {}).get("pagination", {}).get("next_page_token")
            if not next_page_token:
                break
            params["next_page_token"] = next_page_token
        except json.JSONDecodeError as e:
            print(f"⚠️ Error decodificando JSON: {e}. Respuesta: {response.text}")
            break
    return pd.json_normalize(resultados) if resultados else pd.DataFrame()

def limpiar_dataframe(df):
    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)
    df.fillna("", inplace=True)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

def exportar_a_sheets_limited(df, sheet_url, hoja_nombre, max_cols_letter):
    try:
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(hoja_nombre)
        col_limit = ord(max_cols_letter) - ord('A') + 1 if len(max_cols_letter) == 1 else \
                    (ord(max_cols_letter[0]) - ord('A') + 1) * 26 + (ord(max_cols_letter[1]) - ord('A') + 1)
        df_limit = df.iloc[:, :col_limit]
        range_notation = f"A1:{max_cols_letter}"
        worksheet.update(range_notation, [df_limit.columns.tolist()] + df_limit.values.tolist())
        print(f"✅ Exportado: {hoja_nombre} hasta columna {max_cols_letter}")
    except Exception as e:
        print(f"⚠️ Error al exportar a {hoja_nombre}: {e}")

def exportar_completo(df, sheet_url, hoja_nombre):
    try:
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(hoja_nombre)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ Exportado: {hoja_nombre}")
    except Exception as e:
        print(f"⚠️ Error al exportar a {hoja_nombre}: {e}")

# 🔗 URL de tu hoja de cálculo
sheet_url = "https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit"

# 📥 Exportar negocios (hasta CR)
print("🔍 Exportando deals...")
df_deals = obtener_datos_paginados("deals")
if not df_deals.empty:
    df_deals = limpiar_dataframe(df_deals)
    exportar_a_sheets_limited(df_deals, sheet_url, "Pipedrive Deals", "CR")
else:
    print("⚠️ No se obtuvieron datos para deals.")

# 📥 Exportar notas (hasta AA)
print("🔍 Exportando notas...")
df_notes = obtener_datos_paginados("notes")
if not df_notes.empty:
    df_notes = limpiar_dataframe(df_notes)
    exportar_a_sheets_limited(df_notes, sheet_url, "Pipedrive Notas", "AA")
else:
    print("⚠️ No se obtuvieron datos para notas.")

# 📥 Exportar actividades (hasta DA)
print("🔍 Exportando actividades...")
df_activities = obtener_datos_paginados("activities", {"user_id": "0"})
if not df_activities.empty:
    df_activities = limpiar_dataframe(df_activities)
    exportar_a_sheets_limited(df_activities, sheet_url, "Pipedrive Activities", "DA")
else:
    print("⚠️ No se obtuvieron datos para actividades.")

# 📥 Exportar usuarios completo (sin límite)
print("🔍 Exportando usuarios...")
df_users = obtener_datos_paginados("users")
if not df_users.empty:
    df_users = limpiar_dataframe(df_users)
    exportar_completo(df_users, sheet_url, "Pipedrive Users")
else:
    print("⚠️ No se obtuvieron datos para usuarios.")

print("🎉 Exportación completa. Hoy es 14 de julio de 2025, 06:22 PM -03.")
