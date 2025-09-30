# actions_users.py
from typing import Dict, Any
from mysql.connector import Error
from app.db.db import get_conn  #Banco de Dados: psoffice
from typing import Dict, Any, Iterable
from mysql.connector import Error
from app.db.db import get_conn


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `users` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,

  -- do payload

  `usu_id` INT UNSIGNED NOT NULL,               
  `nome` VARCHAR(150) NOT NULL,                  
  `sigla` VARCHAR(32) NULL,                       
  `login` VARCHAR(255) NOT NULL,                 
  `email` VARCHAR(255) NOT NULL,                  
  `ativo` TINYINT(1) NOT NULL DEFAULT 1,          

  -- superior { usuId, nome }

  `superior_usu_id` INT UNSIGNED NULL,
  `superior_nome` VARCHAR(150) NULL,

  -- empresa { pjId, codigo, nome }

  `empresa_pj_id` VARCHAR(32) NULL,
  `empresa_codigo` VARCHAR(100) NULL,
  `empresa_nome` VARCHAR(150) NULL,

  -- outros

  `telefone` VARCHAR(40) NULL,                 

  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`id`),

  -- Restrições/índices úteis

  UNIQUE KEY `uq_users_usu_id` (`usu_id`),
  UNIQUE KEY `uq_users_login` (`login`),
  UNIQUE KEY `uq_users_email` (`email`),

  KEY `idx_users_superior_usu_id` (`superior_usu_id`),
  KEY `idx_users_empresa_pj_id` (`empresa_pj_id`)
)

ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
"""

def create_users_table() -> Dict[str, Any]:

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

        return {"ok": True, "message": f"Tabela `users` criada/verificada em `{current_db}`."}
    
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
INSERT INTO users (
  usu_id, nome, sigla, login, email, ativo,
  superior_usu_id, superior_nome,
  empresa_pj_id, empresa_codigo, empresa_nome,
  telefone
) VALUES (
  %(usu_id)s, %(nome)s, %(sigla)s, %(login)s, %(email)s, %(ativo)s,
  %(superior_usu_id)s, %(superior_nome)s,
  %(empresa_pj_id)s, %(empresa_codigo)s, %(empresa_nome)s,
  %(telefone)s
)
ON DUPLICATE KEY UPDATE
  nome = VALUES(nome),
  sigla = VALUES(sigla),
  login = VALUES(login),
  email = VALUES(email),
  ativo = VALUES(ativo),
  superior_usu_id = VALUES(superior_usu_id),
  superior_nome = VALUES(superior_nome),
  empresa_pj_id = VALUES(empresa_pj_id),
  empresa_codigo = VALUES(empresa_codigo),
  empresa_nome = VALUES(empresa_nome),
  telefone = VALUES(telefone),
  updated_at = CURRENT_TIMESTAMP
"""

def upsert_user(row: Dict[str, Any]) -> Dict[str, Any]:
    conn = cur = None
    try:
        conn = get_conn()  # Conecta ao banco
        cur = conn.cursor()
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        print(f"Dados inseridos/atualizados para {row['usu_id']} - {row['nome']}")
        return {"ok": True}
    except Error as e:
        print(f"Erro ao inserir/atualizar usuário {row['usu_id']}: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if cur: cur.close()
        if conn and conn.is_connected(): conn.close() 


def upsert_many(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Exibe os dados que estão sendo inseridos
        for row in rows:
            print(f"Inserindo/Atualizando: {row}")

        cur.executemany(UPSERT_SQL, list(rows))
        conn.commit()

        # Verifica quantos registros foram afetados
        print(f"Inserção/atualização de {cur.rowcount} usuários.")
        return {"ok": True, "count": cur.rowcount}
    except Error as e:
        print(f"Erro ao inserir/atualizar em lote: {e}")
        return {"ok": False, "error": str(e)}
    finally:
        if cur: cur.close()
        if conn and conn.is_connected(): conn.close()

if __name__ == "__main__":
    res = create_users_table()
    if res.get("ok"):
        print("Criado:", res["message"])
    else:
        print("Erro:", res.get("error"))
