import psycopg2
import logging
import json


with open('db_info.json','r') as f:
    db_config:dict = json.load(f)

database: str = db_config['database']
user: str = db_config['user']
password: str = db_config['password']

def create_pgsql_table_new_cars()-> None:
    conn = psycopg2.connect(database,user,password)
    cursor = conn.cursor()
    SQL =f"""
    CREATE TABLE IF NOT EXISTS new_cars(
        id VARCHAR(50) UNIQUE,
        name VARCHAR(200),
        price INT,
        minimum_deposit INT,
        minimum_installment INT,
        CC VARCHAR(50),
        link VARCHAR(100),
        make VARCHAR(50),
        model VARCHAR(50)
    );
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def create_pgsql_table_used_cars() -> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    CREATE TABLE IF NOT EXISTS used_cars(
        id VARCHAR(50) UNIQUE,
        price INT,
        phone_number VARCHAR(20),
        installment INT,
        deposit INT
    );

    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()



def pgsql_add_column(table: str,column: str)-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    ALTER TABLE {table}
    ADD COLUMN IF NOT EXISTS {column} VARCHAR(50)  
    ;

    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def read_pgsql_table(table: str)-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    SELECT * FROM {table};
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    print(result)


def len_pgsql_table(table: str) -> int:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    select count(*) from {table};
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.close()
    return result[0][0]


def insert_pgsql_table(data:dict)-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor=conn.cursor()
    values = tuple([data[column] for column in data])
    SQL =f"""
    INSERT INTO new_cars
    VALUES {values} 
    ;
    """
    print('Values Inserted')
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def insert_pgsql_table_used(table,data:dict):
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor=conn.cursor()
    columns = tuple(data.keys())
    values = tuple(data.values())

    SQL =f"""
    INSERT INTO {table} 
    ({','.join([column.strip().replace(' ','_') for column in columns])})
    VALUES {values} 
    
    ;
    """
    print('Values Inserted')
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def drop_pgsql_table(table):
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor=conn.cursor()
    
    SQL =f"""

    DROP TABLE {table}
    ;
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def create_pgsql_table_new_cars_links()-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    SQL =f"""
    CREATE TABLE IF NOT EXISTS new_cars_links(
        id serial NOT NULL,
        link VARCHAR(250) UNIQUE
    );
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def create_pgsql_table_used_cars_links()-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    SQL =f"""
    CREATE TABLE IF NOT EXISTS used_cars_links(
        id serial NOT NULL,
        link VARCHAR(250) UNIQUE
    );
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def insert_link(table_name: str,link: str)-> None:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor=conn.cursor()
    logging.info(f'added {link}')
    SQL =f"""
    INSERT INTO {table_name} (link)
    VALUES ('{link}')
    ;
    """
    try:
        cursor.execute(SQL)
        conn.commit()
    except psycopg2.IntegrityError as e:
        logging.error(e.pgcode)
        pass
    conn.close()


def read_link(table: str,days: int)-> list[str]:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    SELECT * FROM {table}
    WHERE date > CURRENT_TIMESTAMP - INTERVAL '{days}day'
    ;
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    return [entry[1] for entry in result]


def read_links(table: str)-> list[str]:
    conn = psycopg2.connect(database=database,user=user,password=password)
    cursor = conn.cursor()
    
    SQL =f"""

    SELECT * FROM {table}
    ;
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    return [entry[1] for entry in result]

if __name__ == '__main__':
    print(read_links('used_cars_links'))