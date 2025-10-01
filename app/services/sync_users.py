# app/services/sync_users.py
from app.apis.users_api import list_users, normalize_user
from app.actions.actions_user import upsert_many, upsert_user

def sync_all_users():
    all_users = [] 
    indices = [0, 100, 200, 300]  
    
    for index in indices:
        print(f"Iniciando requisição para índice {index}")
        payloads = list_users(index=index) 
        normalized_rows = [normalize_user(p) for p in payloads]

        for row in normalized_rows:
            print(f"Normalizando: {row}")
        
        all_users.extend(normalized_rows)

        if index == 300:
            print(f"Enviando {len(all_users)} usuários para o banco de dados...")
            return upsert_many(all_users)  
        

def sync_single_user(user_id: str | int):
    payload = next((u for u in list_users() if u.get("usuId") == user_id), None)
    
    if not payload:
        return {"ok": False, "error": "Usuário não encontrado"}

    row = normalize_user(payload)
    print(f"Normalizando usuário único: {row}")

    return upsert_user(row) 

