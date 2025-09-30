# app/apis/users_api.py
from typing import Dict, Any, List
from .http import get

def list_users() -> List[Dict[str, Any]]:

    data = get("/api/rest/usuario")  
    return data if isinstance(data, list) else data.get("items", [])


def normalize_user(p: Dict[str, Any]) -> Dict[str, Any]:
    sup = p.get("superior") or {}
    emp = p.get("empresa") or {}
    return {
        "usu_id": p.get("usuId"),
        "nome": p.get("nome"),
        "sigla": p.get("sigla"),
        "login": p.get("login"),
        "email": p.get("email"),
        "ativo": 1 if p.get("ativo") else 0,
        "superior_usu_id": sup.get("usuId"),
        "superior_nome": sup.get("nome"),
        "empresa_pj_id": str(emp.get("pjId")) if emp.get("pjId") is not None else None,
        "empresa_codigo": emp.get("codigo"),
        "empresa_nome": emp.get("nome"),
        "telefone": _clean_phone(p.get("telefone")),
    }

def _clean_phone(phone: str | None) -> str | None:
    if not phone: 
        return None
    return "".join(filter(str.isdigit, phone))
