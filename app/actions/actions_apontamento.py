# app/actions/apontamentos.py
from typing import Dict, Any
from mysql.connector import Error
from app.db.db import get_conn
from datetime import date, datetime
from app.apis.apontamentos_api import list_apontamentos, normalize_apontamento

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `apontamentos` (
  `DT_SUBMISSAO` DATETIME NOT NULL,
  `PROJREC_ID` INT UNSIGNED NOT NULL,
  `ATRIB_ID` INT UNSIGNED NOT NULL,
  `MINUTOS` INT NOT NULL,
  `APON_ID` INT UNSIGNED NOT NULL,
  `PROJ_ID` INT UNSIGNED NOT NULL,
  `ATIV_ID` INT UNSIGNED NOT NULL,
  `NOME_ATIVIDADE` VARCHAR(150) NOT NULL,
  `USUARIO` VARCHAR(255) NOT NULL,
  `NOME_PROJETO` VARCHAR(255) NOT NULL,
  `STATUS` INT NOT NULL,
  `USU_ID` INT UNSIGNED NOT NULL,

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`APON_ID`),

  UNIQUE KEY `uq_apontamentos_apon_id` (`APON_ID`),
  KEY `idx_apontamentos_projeto` (`PROJ_ID`),
  KEY `idx_apontamentos_usuario` (`USU_ID`),
  KEY `idx_apontamentos_status` (`STATUS`)
)

ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

def create_apontamentos_table():
    conn = None
    cur = None
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("SELECT DATABASE()")
        (current_db,) = cur.fetchone()

        if not current_db:
            raise RuntimeError("Nenhum database selecionado na conexão.")

        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        return {"ok": True, "message": f"Tabela `apontamentos` criada/verificada em `{current_db}`."}
    
    except (Error, RuntimeError) as e:
        return {"ok": False, "error": str(e)}
    
    finally:
        try:
            if cur: cur.close()
        except Exception:
            pass

        try:
            if conn and conn.is_connected(): conn.close()
        except Exception:
            pass

UPSERT_SQL = """
INSERT INTO apontamentos (
  DT_SUBMISSAO, PROJREC_ID, ATRIB_ID, MINUTOS, APON_ID, PROJ_ID, ATIV_ID,
  NOME_ATIVIDADE, USUARIO, NOME_PROJETO, STATUS, USU_ID
) VALUES (
  %(DT_SUBMISSAO)s, %(PROJREC_ID)s, %(ATRIB_ID)s, %(MINUTOS)s, %(APON_ID)s, %(PROJ_ID)s, %(ATIV_ID)s,
  %(NOME_ATIVIDADE)s, %(USUARIO)s, %(NOME_PROJETO)s, %(STATUS)s, %(USU_ID)s
)
ON DUPLICATE KEY UPDATE
  DT_SUBMISSAO = VALUES(DT_SUBMISSAO),
  PROJREC_ID = VALUES(PROJREC_ID),
  ATRIB_ID = VALUES(ATRIB_ID),
  MINUTOS = VALUES(MINUTOS),
  PROJ_ID = VALUES(PROJ_ID),
  ATIV_ID = VALUES(ATIV_ID),
  NOME_ATIVIDADE = VALUES(NOME_ATIVIDADE),
  USUARIO = VALUES(USUARIO),
  NOME_PROJETO = VALUES(NOME_PROJETO),
  STATUS = VALUES(STATUS),
  USU_ID = VALUES(USU_ID),
  updated_at = CURRENT_TIMESTAMP
"""

def upsert_apontamento(row: Dict[str, Any]):
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        print(f"Dados inseridos/atualizados para {row['APON_ID']} - {row['NOME_ATIVIDADE']}")
        return {"ok": True}
    
    except Error as e:
        print(f"Erro ao inserir/atualizar apontamento {row['APON_ID']}: {e}")
        return {"ok": False, "error": str(e)}
    
    finally:
        try:
            if cur: cur.close()
        except Exception:
            pass

        try:
            if conn and conn.is_connected(): conn.close()
        except Exception:
            pass


def upsert_apontamentos(data_inicio: str = "2024-09-01", data_fim: str = date.today().strftime("%Y-%m-%d")):
    data_inicio = format_date(data_inicio)
    data_fim = format_date(data_fim)

    print(f"Realizando upsert de dados de {data_inicio} até {data_fim}.")
    
    apontamentos = buscar_apontamentos(data_inicio, data_fim) 
    
    for row in apontamentos:
        
        upsert_apontamento(row)
    
    return {"ok": True, "message": f"Upsert realizado entre {data_inicio} e {data_fim}"}


def upsert_full_history():
    data_inicio = "2024-09-01"
    data_fim = date.today().strftime("%Y-%m-%d")
    
   
    data_inicio = format_date(data_inicio)
    data_fim = format_date(data_fim)

    print(f"Realizando upsert de dados de {data_inicio} até {data_fim}.")
    
    apontamentos = buscar_apontamentos(data_inicio, data_fim)  
    for row in apontamentos:
       
        upsert_apontamento(row)
    
    return {"ok": True, "message": f"Upsert realizado entre {data_inicio} e {data_fim}"}



def buscar_apontamentos(data_inicio: str, data_fim: str):
    print(f"Buscando apontamentos entre {data_inicio} e {data_fim}...")
    
    payloads = list_apontamentos(data_inicio, data_fim)

    normalized_rows = [normalize_apontamento(p) for p in payloads]
    
    return normalized_rows


def format_date(date_str: str) -> str:
    
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
   
    except ValueError:
        raise ValueError("Formato de data inválido. Use o formato 'YYYY-MM-DD'.")
