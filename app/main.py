import argparse
from app.actions.actions_user import create_users_table
from app.services.sync_users import sync_all_users, sync_single_user

def main():
    parser = argparse.ArgumentParser(description="Sincronização de usuários da API para MySQL.")
    parser.add_argument("--init-table", action="store_true", help="Cria/verifica a tabela users.")
    parser.add_argument("--sync-single", type=int, help="Sincroniza um único usuário pelo ID.")
    parser.add_argument("--sync-all", action="store_true", help="Sincroniza todos os usuários.")
    args = parser.parse_args()

    if args.init_table:
        res = create_users_table()
        print(res)
        return

    if args.sync_all:
        res = sync_all_users()
        print(res)
        return
    
    if args.sync_single:
        res = sync_single_user(args.sync_single)
        print(res)
        return

    parser.print_help()

if __name__ == "__main__":
    main()