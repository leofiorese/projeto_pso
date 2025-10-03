import argparse
from app.actions.actions_user import create_users_table
from app.services.sync_users import sync_all_users, sync_single_user
from app.actions.actions_apontamento import create_apontamentos_table
from app.services.sync_apontamentos import sync_all_apontamentos, sync_single_apontamento
from datetime import date, datetime

def format_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
    except ValueError:
        raise ValueError("Formato de data inválido. Use o formato 'YYYY-MM-DD'.")

def main():
    parser = argparse.ArgumentParser(description="Sincronização de usuários e apontamentos da API para MySQL.")

    # Argumentos para a criação/verificação da tabela de usuários
    parser.add_argument("--init-table", action="store_true", help="Cria/verifica a tabela users.")
    
    # Argumentos para sincronização de usuários
    parser.add_argument("--sync-single", type=int, help="Sincroniza um único usuário pelo ID.")
    parser.add_argument("--sync-all", action="store_true", help="Sincroniza todos os usuários.")
    
    # Argumentos para a criação/verificação da tabela de apontamentos
    parser.add_argument("--init-apontamentos-table", action="store_true", help="Cria/verifica a tabela apontamentos.")

    # Argumentos para sincronização de apontamentos
    parser.add_argument("--sync-apontamentos-all", action="store_true", help="Sincroniza todos os apontamentos.")
    parser.add_argument("--sync-apontamento-single", type=int, help="Sincroniza um único apontamento pelo APON_ID.")
    parser.add_argument("--data-inicio", type=str, help="Data de início no formato 'YYYY-MM-DD'.")
    parser.add_argument("--data-fim", type=str, help="Data de fim no formato 'YYYY-MM-DD'.")
    
    args = parser.parse_args()

    # Criação/verificação da tabela de usuários
    if args.init_table:
        res = create_users_table()
        print(res)
        return

    # Sincroniza todos os usuários
    if args.sync_all:
        res = sync_all_users()
        print(res)
        return
    
    # Sincroniza um único usuário
    if args.sync_single:
        res = sync_single_user(args.sync_single)
        print(res)
        return
    



    # Criação/verificação da tabela de apontamentos
    if args.init_apontamentos_table:
        res = create_apontamentos_table()
        print(res)
        return

    # Sincroniza todos os apontamentos
    if args.sync_apontamentos_all:
        # Caso as datas não sejam passadas, usa as datas padrão
        data_inicio = args.data_inicio if args.data_inicio else "2024-09-01"
        data_fim = args.data_fim if args.data_fim else date.today().strftime("%Y-%m-%d")

        # Formata as datas para o formato DD-MM-YYYY
        data_inicio = format_date(data_inicio)
        data_fim = format_date(data_fim)
        
        res = sync_all_apontamentos(data_inicio, data_fim)
        print(res)
        return

    # Sincroniza um único apontamento
    if args.sync_apontamento_single:
        # Caso as datas não sejam passadas, usa as datas padrão
        data_inicio = args.data_inicio if args.data_inicio else "2024-09-01"
        data_fim = args.data_fim if args.data_fim else date.today().strftime("%Y-%m-%d")

        # Formata as datas para o formato DD-MM-YYYY
        data_inicio = format_date(data_inicio)
        data_fim = format_date(data_fim)
        
        res = sync_single_apontamento(args.sync_apontamento_single, data_inicio, data_fim)
        print(res)
        return

    # Caso nenhum comando seja fornecido
    parser.print_help()

if __name__ == "__main__":
    main()