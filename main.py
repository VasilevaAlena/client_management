import psycopg


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone;
        DROP TABLE client;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR (30) NOT NULL,
                last_name VARCHAR (30) NOT NULL,
                email VARCHAR (40)
            );
            CREATE TABLE IF NOT EXISTS phone(
                client_id INTEGER REFERENCES client(id) NOT NULL,
                phone_number VARCHAR (20) 
            );
            """)
        conn.commit()


def add_client(conn, client_id, first_name, last_name, email, phone_number=None):
    with conn.cursor() as cur:
        cur.execute(""" 
            INSERT INTO client(id, first_name, last_name, email)
            VALUES(%s, %s, %s, %s)
            RETURNING id;
            """, 
            (client_id, first_name, last_name, email))
        print(cur.fetchone())
        cur.execute("""
            INSERT INTO phone(client_id, phone_number)
            VALUES(%s, %s)
            ;
            """, 
            (client_id, phone_number))
        cur.execute("""
            SELECT * FROM phone;
            """)
        print(cur.fetchall())


def add_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phone(client_id, phone_number)
            VALUES(%s,%s);
            """, 
            (client_id, phone_number))
        cur.execute("""
            SELECT * FROM phone;
            """)
        print(cur.fetchall())


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone_number=None, last_phone_number=None):
    with conn.cursor() as cur:
        if first_name is not None:
            cur.execute("""
                UPDATE client
                SET first_name = %s
                WHERE id = %s;
                """, 
                (first_name, client_id))
        if last_name is not None:
            cur.execute("""
                UPDATE client
                SET last_name = %s
                WHERE id = %s;
                """, 
                (last_name, client_id))
        if email is not None:
            cur.execute("""
                UPDATE client
                SET email = %s
                WHERE id = %s;
                """, 
                (email, client_id))
        if phone_number is not None and last_phone_number is not None:
            cur.execute("""
                UPDATE phone
                SET phone_number = %s
                WHERE client_id = %s AND phone_number = %s;
                """, 
                (phone_number, client_id, last_phone_number)) 
        elif phone_number is not None and last_phone_number is None:
            cur.execute("""
                UPDATE phone
                SET phone_number = %s
                WHERE client_id = %s AND phone_number IS NULL;
                """, 
                (phone_number, client_id))
        cur.execute("""
            SELECT * FROM client;
            """)
        print(cur.fetchall())
        cur.execute("""
            SELECT * FROM phone;
            """)
        print(cur.fetchall())


def delete_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone
            WHERE phone_number = %s AND client_id = %s;
            """, 
            (phone_number, client_id))
        cur.execute("""
            SELECT * FROM phone;
            """)
        print(cur.fetchall())


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone
            WHERE client_id = %s;
            """, (client_id,))
        cur.execute("""
            DELETE FROM client
            WHERE id = %s;
            """, (client_id,))
        cur.execute("""
            SELECT * FROM client;
            """)
        print(cur.fetchall())


def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if phone_number is not None:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email
                FROM phone AS p
                LEFT JOIN client AS c
                ON p.client_id = c.id
                WHERE p.phone_number = %s
                """, 
                (phone_number,))
            print(cur.fetchall())
        elif email is not None:
            cur.execute("""
                SELECT * 
                FROM client
                WHERE email = %s;
                """, 
                (email,))
            print(cur.fetchall())
        elif first_name and last_name is not None:
            cur.execute("""
                SELECT * 
                FROM client
                WHERE first_name = %s AND last_name = %s;
                """, 
                (first_name, last_name))
            print(cur.fetchall())
        elif first_name is not None:
            cur.execute("""
                SELECT * 
                FROM client
                WHERE first_name = %s;
                """, 
                (first_name,))
            print(cur.fetchall())
        elif last_name is not None:
            cur.execute("""
                SELECT * 
                FROM client
                WHERE last_name = %s;
                """, 
                (last_name,))
            print(cur.fetchall())
        else:
            print('Клиента не существует')


with psycopg.connect("dbname=client_management user=postgres password=9159") as conn:
    create_db(conn)
    add_client(conn, 1, 'Anna', 'Petrova', 'PetrovaAnna@mail.ru', '79151234567')
    add_client(conn, 2, 'Petr', 'Serov', 'SerovPetr@mail.ru')
    add_phone(conn, 1, '79701234567')
    add_phone(conn, 2, '79159876543')
    change_client(conn, 2, phone_number='79601234567')
    change_client(conn, 1, first_name='Olga', last_name='Salova', phone_number='79101234567', last_phone_number='79701234567')
    delete_phone(conn, 1, '79151234567')
    delete_client(conn, 2)
    find_client(conn, phone_number='79101234567')
    find_client(conn, email='PetrovaAnna@mail.ru')
    find_client(conn, first_name='Olga', last_name='Salova')
    find_client(conn, first_name='Olga')
