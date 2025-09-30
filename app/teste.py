# Teste simples do UPSERT com Python (conectando ao banco)
from app.db.db import get_conn
from mysql.connector import Error

UPSERT_SQL = """
INSERT INTO users (usu_id, nome, sigla, login, email, ativo)
VALUES (%(usu_id)s, %(nome)s, %(sigla)s, %(login)s, %(email)s, %(ativo)s)
ON DUPLICATE KEY UPDATE
  nome = VALUES(nome),
  sigla = VALUES(sigla),
  login = VALUES(login),
  email = VALUES(email),
  ativo = VALUES(ativo);
"""

def test_upsert():
    test_data = {
        "usu_id": 1,
        "nome": "Test User",
        "sigla": "TU",
        "login": "test@domain.com",
        "email": "test@domain.com",
        "ativo": 1
    }

    try:
        # Conectar ao banco de dados
        conn = get_conn()
        cur = conn.cursor()
        
        # Executa o UPSERT
        cur.execute(UPSERT_SQL, test_data)
        conn.commit()

        print("UPSERT realizado com sucesso.")
        
        # Verificar se o dado foi inserido
        cur.execute("SELECT * FROM users WHERE usu_id = %s", (test_data['usu_id'],))
        result = cur.fetchone()
        print("Dados inseridos/atualizados:", result)

    except Error as e:
        print(f"Erro ao executar UPSERT: {e}")

    finally:
        if cur: cur.close()
        if conn and conn.is_connected(): conn.close()

# Testar o UPSERT
test_upsert()