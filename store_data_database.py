

from typing import List, Tuple

import mysql.connector # Must include .connector


merck_url_table_name = "merck_url_detail"


DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "merck_product_db_2"
}

def get_connection():
    try:
        ## here ** is unpacking DB_CONFIG dictionary.
        connection = mysql.connector.connect(**DB_CONFIG)
        ## it is protect to autocommit
        connection.autocommit = False
        return connection
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS merck_product_db_2;")
    connection.commit()
    connection.close()
# create_db()


def create_table_merck_url():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  f"""
                CREATE TABLE IF NOT EXISTS {merck_url_table_name}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                main_category VARCHAR(100),
                sub_category VARCHAR(100),
                sub_sub_category VARCHAR(100),
                url VARCHAR(500) UNIQUE,
                status VARCHAR(100)
        ); """
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


batch_size_length = 100
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count


def merck_url_insert(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    if not list_data:
        return
    dict_data = list_data[0]
    columns = ", ".join(list(dict_data.keys()))
    values = "".join([len(dict_data.keys()) * '%s,']).strip(',')
    parent_sql = f"""INSERT IGNORE INTO {merck_url_table_name} ({columns}) VALUES ({values})"""
    try:

        cursor.execute(f"SELECT COUNT(*) FROM {merck_url_table_name}")
        total_contry_rows = cursor.fetchone()[0]
        if total_contry_rows == 0:

            product_values = []
            for dict_data in list_data:
                product_values.append( (
                    dict_data.get("main_category") ,
                    dict_data.get("sub_category"), 
                    dict_data.get("sub_sub_category"),
                    dict_data.get("url"),
                    dict_data.get("status")

                ))

            try:
                batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
                # print(f"Parent batches executed count={batch_count}")
            except Exception as e:
                print(f"batch can not. Error : {e} ")
        else:
            print(f"not {merck_url_table_name} table in data inserted ..")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back", e)
    except:
        print("except error raise ")
    finally:
        connection.close()




def fetch_merck_url_table_data():
    connection = get_connection()
    cursor = connection.cursor()
    # query = f"SELECT * FROM {merck_url_table_name} WHERE id = 414;"

    query = f"SELECT * FROM {merck_url_table_name} WHERE status = 'pending';"
    
    cursor.execute(query)
    rows = cursor.fetchall()

    result = []
    # print(rows)
    for row in rows:
        data = {
            "id": row[0],
            "main_category": row[1],
            "sub_category": row[2],
            "sub_sub_category": row[3],
            "url": row[4],
            "status": row[5]
        }
        result.append(data)

    cursor.close()
    connection.close()
    return result


### update country url status ..

def update_merck_url_status(merck_url_detail_id, status):
    connection = get_connection()
    cursor = connection.cursor()
    sql_query = f"UPDATE {merck_url_table_name} SET status = %s  WHERE id = %s ;"
    values = (status, merck_url_detail_id)
    cursor.execute(sql_query, values)
    connection.commit()
    cursor.close()
    connection.close()



def delete_crupt_child_recode():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        query = f"""
            DELETE c
            FROM {child_product_url_table_name} c
            JOIN {merck_url_table_name} p 
                ON c.parent_id = p.id
            WHERE p.status = 'pending';
        """

        cursor.execute(query)
        connection.commit()

        print(f"Deleted rows: {cursor.rowcount}")

    except Exception as e:
        print("Error:", e)
        connection.rollback()
    
    finally:
        cursor.close()
        connection.close()





### product_url table 
child_product_url_table_name = "product_url_detail"


def create_table_product_url():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  f"""
                CREATE TABLE IF NOT EXISTS {child_product_url_table_name}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                parent_id INT,
                sub_category_url VARCHAR(500), 
                product_name TEXT,
                brand_name TEXT,
                productNumber VARCHAR(400),
                product_url TEXT,
                CONSTRAINT fk_parent
                    FOREIGN KEY (parent_id)
                    REFERENCES {merck_url_table_name}(id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
        ); """
        cursor.execute(query)
        connection.commit()

    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()





def product_url_insert(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    if not list_data:
        return
    dict_data = list_data[0]
    columns = ", ".join(list(dict_data.keys()))
    values = "".join([len(dict_data.keys()) * '%s,']).strip(',')
    parent_sql = f"""INSERT INTO {child_product_url_table_name} ({columns}) VALUES ({values})"""
    try:

        # cursor.execute(f"SELECT COUNT(*) FROM {merck_url_table_name}")
        # total_contry_rows = cursor.fetchone()[0]
        # if total_contry_rows == 0:

        product_values = []
        for dict_data in list_data:
            product_values.append( (
                dict_data.get("parent_id") ,
                dict_data.get("sub_category_url") ,
                dict_data.get("product_name") ,
                dict_data.get("brand_name"), 
                dict_data.get("productNumber"),
                dict_data.get("product_url")

            ))

        try:
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
            # print(f"Parent batches executed count={batch_count}")
        except Exception as e:
            print(f"batch can not. Error : {e} ")
        # else:
        #     print(f"not {merck_url_table_name} table in data inserted ..")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back", e)
    except:
        print("except error raise ")
    finally:
        connection.close()
