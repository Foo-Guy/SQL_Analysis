import mysql.connector

def fetch_unique_values(cursor, table_name, column_name, row_limit=None):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    if row_limit:
        query += f" LIMIT {row_limit}"
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}

def fetch_column_details(cursor, table_name):
    query = f"SHOW COLUMNS FROM {table_name}"
    cursor.execute(query)
    return [(row[0], row[1]) for row in cursor.fetchall()]

def identify_soft_foreign_keys(db1_config, table1, db2_config, table2, threshold=5, row_limit=None):
    try:
        conn1 = mysql.connector.connect(**db1_config)
        cursor1 = conn1.cursor()
        table1_columns = fetch_column_details(cursor1, table1)
    except mysql.connector.Error as err:
        print(f"Something went wrong with the first database connection: {err}")
        return []
        
    try:
        conn2 = mysql.connector.connect(**db2_config)
        cursor2 = conn2.cursor()
        table2_columns = fetch_column_details(cursor2, table2)
    except mysql.connector.Error as err:
        print(f"Something went wrong with the second database connection: {err}")
        return []

    potential_foreign_keys = []
    for (col1, type1) in table1_columns:
        for (col2, type2) in table2_columns:
            if type1 == type2:
                unique_col1_values = fetch_unique_values(cursor1, table1, col1, row_limit)
                unique_col2_values = fetch_unique_values(cursor2, table2, col2, row_limit)
                common_values = unique_col1_values.intersection(unique_col2_values)
                
                if len(common_values) >= threshold:
                    potential_foreign_keys.append((col1, col2))

    conn1.close()
    conn2.close()
    return potential_foreign_keys

# Database configurations
db1_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password1',
    'database': 'database1'
}

db2_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password2',
    'database': 'database2'
}

# Identify potential foreign keys with a custom threshold and row limit
threshold_value = 10
row_limit_value = 100  # Set your custom row limit value here
potential_keys = identify_soft_foreign_keys(db1_config, 'table1', db2_config, 'table2', threshold=threshold_value, row_limit=row_limit_value)
print("Potential soft foreign keys:", potential_keys)
