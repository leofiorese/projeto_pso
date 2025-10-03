# app/services/sync_apontamentos.py
from app.apis.apontamentos_api import list_apontamentos, normalize_apontamento
from app.actions.actions_apontamento import upsert_apontamentos, upsert_full_history, upsert_apontamento
from datetime import date, datetime

def format_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
   
    except ValueError:
        raise ValueError("Formato de data inválido. Use o formato 'YYYY-MM-DD'.")

def sync_all_apontamentos(data_inicio: str = "2024-09-01", data_fim: str = date.today().strftime("%Y-%m-%d")):
    data_inicio = format_date(data_inicio)
    data_fim = format_date(data_fim)

    print(f"Sincronizando todos os apontamentos de {data_inicio} até {data_fim}...")

    result = upsert_apontamentos(data_inicio, data_fim)
    
    return result


def sync_single_apontamento(apon_id: str | int, data_inicio: str = "2024-09-01", data_fim: str = date.today().strftime("%Y-%m-%d")):

    data_inicio = format_date(data_inicio)
    data_fim = format_date(data_fim)

    print(f"Sincronizando apontamento {apon_id} de {data_inicio} até {data_fim}...")

    
    payload = next((p for p in list_apontamentos(data_inicio=data_inicio, data_fim=data_fim) if p.get("APON_ID") == apon_id), None)
    
    if not payload:
        return {"ok": False, "error": "Apontamento não encontrado"}

    row = normalize_apontamento(payload)
    print(f"Normalizando apontamento único: {row}")

 
    return upsert_apontamento(row)
