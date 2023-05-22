import psycopg2
from psycopg2 import sql

with psycopg2.connect(database="BD", user="postgres", password="") as conn:
    with conn.cursor() as cur:
        # cur.execute("""
        # DROP TABLE data_clients;
        # DROP TABLE clients
        # CASCADE;
        # """)

        def create_db():

            cur.execute("""
            CREATE TABLE IF NOT EXISTS clients(
                clients_id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR (50) NOT NULL,
                email VARCHAR (100) NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS data_clients(
                clients_id INTEGER REFERENCES clients(clients_id) 
                ON DELETE CASCADE ,
                phone VARCHAR (20) UNIQUE
            );
            """)
            return 'База данных создана'


        conn.commit()


        def get_id(current_value, find_by):

            if find_by == 'phone':
                cur.execute("""SELECT * FROM data_clients WHERE phone = %s""", (current_value,))
                id_clients = cur.fetchone()[0]
            else:
                cur.execute("""SELECT * FROM clients
                    WHERE first_name = %s OR last_name = %s OR email = %s""",
                            (current_value, current_value, current_value))
                id_clients = cur.fetchone()[0]

            return id_clients


        def add_client(first_name, last_name, email):

            cur.execute("""
            INSERT INTO clients (first_name, last_name, email)
            VALUES (%s, %s, %s);""", (first_name, last_name, email))
            return 'Новый клиент добавлен'


        conn.commit()


        def add_phone(phone, current_value, find_by='email'):

            cur.execute("""
                INSERT INTO data_clients(clients_id, phone)
                VALUES (%s, %s)""", (get_id(current_value, find_by), phone))
            return 'Номер телефона клиента добавлен'


        conn.commit()


        def change_client(table, column, new_value, current_value, find_by='email'):

            update_query = sql.SQL("""UPDATE {table} SET {column} = '{new_value}' WHERE clients_id = %s;""").format(
                table=sql.Identifier(table), column=sql.Identifier(column), new_value=sql.Identifier(new_value))
            cur.execute(update_query, (get_id(current_value, find_by),))
            return 'Данные клиента изменены'


        conn.commit()


        def delete_phone(find_by, current_value):

            cur.execute("""
            DELETE FROM data_clients WHERE clients_id = %s;""", (get_id(current_value, find_by),))
            return 'Номер телефона удалён'


        conn.commit()


        def delete_client(find_by, current_value):

            cur.execute("""
            DELETE FROM clients WHERE clients_id = %s;""", (get_id(current_value, find_by),))
            return 'Данные клиента удалены'


        conn.commit()


        def find_client(find_by, current_value):

            cur.execute("""SELECT * FROM clients c 
                   JOIN data_clients dc ON c.clients_id = dc.clients_id 
                   WHERE c.clients_id = %s""", (get_id(current_value, find_by),))
            return cur.fetchone()


        def main():
            while True:
                command = input('Введите команду ')
                if command == 't':
                    print(create_db())
                elif command == 'cl':
                    print(add_client(first_name=input('Введите имя '), last_name=input('Введите фамилию '),
                                     email=input('Введите email ')))
                elif command == 'p':
                    print(add_phone(input('Введите номер телефона '), input('Введите email клиента ')))
                elif command == 'd':
                    print(change_client(input('Введите название таблицы '), input('Введите название столбца '),
                                        input('Введите новое значение '), input('Введите параметр поиска ')))
                elif command == 'dp':
                    print(delete_phone('email', input('Введите параметр поиска ')))
                elif command == 'dc':
                    print(delete_client('email', input('Введите параметр поиска ')))
                elif command == 'f':
                    print(find_client(input('Введите название столбца '), input('Введите параметр поиска ')))
                elif command == 'e':
                    print('Выход')
                    break


        if __name__ == '__main__':
            main()