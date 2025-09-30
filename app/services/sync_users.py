# app/services/sync_users.py
from app.apis.users_api import list_users, normalize_user
from app.actions.actions_user import upsert_many, upsert_user

def sync_all_users() -> dict:
    payloads = list_users()  # Pega todos os usuários
    rows = [normalize_user(p) for p in payloads]

    # Verifica o formato dos dados
    for row in rows:
        print(f"Normalizando: {row}")

    return upsert_many(rows)  # Envia para o banco

def sync_single_user(user_id: str | int) -> dict:
    # Pega o usuário específico
    payload = next((u for u in list_users() if u.get("usuId") == user_id), None)
    if not payload:
        return {"ok": False, "error": "Usuário não encontrado"}

    row = normalize_user(payload)

    print(f"Normalizando usuário único: {row}")

    return upsert_user(row)  # Envia para o banco

