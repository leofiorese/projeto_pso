# app/apis/apontamentos_api.py
from typing import Dict, Any, List
from .http import get

def list_apontamentos(data_inicio: str, data_fim: str):
    data = get(f"/api/v1/apontamentos/apontamentos?situacao=2&data_fim={data_fim}&data_inicio={data_inicio}")
    print(f"Dados recebidos da API: {data}")
    
    return data


def normalize_apontamento(p: Dict[str, Any]):
    print(f"Normalizando os dados do apontamento: {p}") 
    
    if not p.get("DT_SUBMISSAO") or not p.get("PROJREC_ID") or not p.get("ATRIB_ID"):
        print(f"Dados inválidos encontrados: {p}")
        return {}

    normalized = {
        "DT_SUBMISSAO": p.get("DT_SUBMISSAO"),
        "PROJREC_ID": p.get("PROJREC_ID"),
        "ATRIB_ID": p.get("ATRIB_ID"),
        "MINUTOS": p.get("MINUTOS"),
        "APON_ID": p.get("APON_ID"),
        "PROJ_ID": p.get("PROJ_ID"),
        "ATIV_ID": p.get("ATIV_ID"),
        "NOME_ATIVIDADE": p.get("NOME_ATIVIDADE"),
        "USUARIO": p.get("USUARIO"),
        "NOME_PROJETO": p.get("NOME_PROJETO"),
        "STATUS": p.get("STATUS"),
        "USU_ID": p.get("USU_ID"),
    }
    
    print(f"Dados normalizados: {normalized}") 

    return normalized
