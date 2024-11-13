import pyodbc

def connect_to_sql_server(dsn, database, username, password):
    
    conn_str = ( r'DRIVER = {SQL Server};'
                r'DSN=' + dsn + ';'
               # r'SERVER =' + server + ';'
                r'DATABASE =' + database + ';'
                r'UID =' + username + ';'
                r'PWD =' + password + ';'

    )
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    
    except pyodbc.Error as ex:
        print(f"Error connecting to SQL Server: {ex}")
        return None

def execut_custom_query(conn, query):
    """
    Executes a custom SQL query.
    Args:
        conn: The pyodbc connection object.
        query: The SQL query to execute.
    Returns:
        True if the query executed successfully, False otherwise.
    """
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print(f"Query executed successfully!")
            return True
        except pyodbc.Error as ex:
            print(f"Error executing query: {ex}")
            return False
        finally: 
            conn.close()
    else:
        print("Connection failed. Query not executed.")

def create_table(conn, table_name, columns):
    """
    Creates a table in the SQL Server database.
    Args:
        conn: The pyodbc connection object.
        table_name: The name of the table to create.
        columns: A list of tuples, where each tuple represents a column: (column_name, data_type, is_nullable = True)
    Returns:
        True if the table was created successfully, False otherwise.
    """
    """
    try:
        cursor = conn.cursor()
        column_definitions = ",".join(f"{name} {data_type} {'NULL' if nullable else 'NOT NULL'}" for name, data_type, nullable in columns)
        create_table_sql = f"CREATE TABLE {table_name} ({column_definitions})"
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
        return True
    except pyodbc.Error as ex:
        print(f"Error creating table '{table_name}' : {ex}")
        return False
    finally:
        cursor.close
    """

    """
    Creates a table in the SQL Server database.
    Args:
        conn: The connection to the SQL Server database.
        table_name: The name of the table to create.
        columns: A list of tuples, where each tuple represents a column: (column_name, data_type, length, is_nullable)
    """
    """
    cursor = conn.cursor()
    # Build the CREATE TABLE statement
    sql = f"CREATE TABLE {table_name}("
    for i, column in enumerate(columns):
        column_name, data_type, length, is_nullable = column
        if i>0:
            sql +=","
        sql += f"{column_name}{data_type}"
        if length:
            sql +=f"({length})"
        sql += f"{'NULL' if is_nullable else 'NOT NULL'}"
    sql += ")"
    try:
        cursor.execute(sql)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating table: {e}") 
"""
    cursor = conn.cursor()
    # Build the CREATE TABLE statement
    sql = f"CREATE TABLE {table_name}("
    for i, column_def in enumerate(columns.split(';')):
        # Split the string into column definitions
        column_name,data_type,length,is_nullable = column_def.split(',') # Split each definition into parts
        if i>0:
            sql +=","
        sql += f"{column_name.strip()} {data_type.strip()}"
        if length:
            sql += f"({length.strip()})"
        sql += f" {'NULL' if is_nullable.strip().lower() == 'true' else 'NOT NULL'}"
    sql += ")"
    try:
        cursor.execute(sql)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating table: {e}")
def insert_data(conn, table_name, data):
    """
    Inserts data into a table in the SQL Server database.
    Args:
        conn: The connection to the SQL Server database.
        table_name: The name of the table to insert data into.
        data: A string representing a single row of data, separated by commas.
    """
    cursor = conn.cursor()
    # Convert the input string to a list of values
    values = [value.strip() for value in data.split(',')]   # Split the input string into values
    # Get the column names from the table
    column_names = get_table_schema(conn, table_name)   # You need to define get_table_schema

    # Build the INSERT statement
    sql = f"INSERT INTO {table_name}({','.join(column_names)}) VALUES ({', '.join(['?' for _ in column_names])})"
    try:
        cursor.execute(sql, values)   # Use execute for a single row
        conn.commit()
        print(f"Data inserted into table '{table_name}' successfully.")
    except pyodbc.Error as e:
        print(f"Error inserting data: {e}")
    
    """
    Inserts data into a table.
    Args:
        conn: The pyodbc connection object.
        table_name: The name of the table to insert data.
        data: A list of tuples, where each tuple represents a row of data to inserted. The order of values in the tuples must match the order of columns in the table.
    
    Returns:
        True if the data was inserted successfully, False otherwise.
    """
    """
    try:
        cursor = conn.cursor()
        placeholders = ",".join(["?"] * len(data[0]))
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"Data inserted into table '{table_name}' successfully.")
        return True
    except pyodbc.Error as ex:
        print(f"Error inserting data: {ex}")
        return False
    finally:
        cursor.close()
    """

def list_tables(conn):
    """Lists all existing tables in the database along with their column names and data types.
    """
    try:

        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        if tables:
            for table in tables:
                print(f"\nTable: {table}")
                cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION")
                columns = cursor.fetchall()
                for column_name, data_type in columns:
                    print(f" - {column_name}: {data_type}")
        else:
            print("No tables found in the database.")
    except pyodbc.Error as e:
        print(f"Error listing tables: {e}")

def select_data(conn, table_name, columns = None, where_clause = None):
    """
    Selects data from a table in the SQL Server database.
    Args:
        conn: The pyodbc connection object ( The connection to the SQL Server database).
        table_name: The name of the table to select data from.
        columns: An optional list of column names to select (defaults to all columns)
        where_clause: An optional WHERE clause for filtering data.
    Returns:
        A list of tuples containing the selected data, or an empty list if no data is found.
    """
    try:
        cursor = conn.cursor()
        select_sql = f"SELECT {columns if columns else '*'} FROM {table_name}"
        if where_clause:
            select_sql += f"WHERE {where_clause}"
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        return rows
    except pyodbc.Error as ex:
        print(f"Error selecting data from '{table_name}' : {ex}")
        return None
    finally:
        cursor.close()

def update_data(conn, table_name,set_values, where_clause=None):
    """
    Updates data in a table in the SQL Server database.
    Args:
        conn: The pyodbc connection object (The connection to the SQL Server database).
        table_name: The name of the table to update data in.
        set_values: A string of column-value pairs to set, separated by commas, in the format: column1=value1, column2=value2, ...
        where_clause: An optional WHERE clause for filtering data.
    Returns:
        True if the data was updated successfully. False otherwise.
    """
    """
    try:
        cursor = conn.cursor()
        set_clauses = ",".join(f"{col}=?" for col in set_values)
        update_sql = f"UPDATE {table_name} SET {set_clauses}"
        if where_clause:
            update_sql += f"WHERE {where_clause}"
        cursor.execute(update_sql, tuple(set_values.values()))
        conn.commit()
        print(f"Data updated in table '{table_name}' successfully.")
        return True
    except pyodbc.Error as ex:
        print(f"Error updating data in table '{table_name}': {ex}")
        return False
    finally:
        cursor.close()
"""
    cursor = conn.cursor()
    try:
        # Parse the set_values string into a dictionary
        update_dict = {}
        for pair in set_values.split(','):
            key, value = pair.split('=')
            update_dict[key.strip()] = value.strip()
        # Build the UPDATE statement
        update_sql = f"UPDATE {table_name} SET "
        update_clauses = [f"{key}= ?" for key in update_dict]
        update_sql += ",".join(update_clauses)
        if where_clause:
            update_sql += " WHERE ?"    #Use a placeholder for the where clause
        
        # Execute the UPDATE statement with values and the where clause as parameters
        params  = tuple(update_dict.values())
        if where_clause:
            params += (where_clause,)   #Add the where clause to the parameters
            
        cursor.execute(update_sql, params)
        conn.commit()
        print(f"Data updated in table '{table_name}' successfully.")
    except pyodbc.Error as e:
        print(f"Error updating data: {e}")
def delete_data(conn, table_name, where_clause=None):
    """
    Deletes data from a table.
    Args:
        conn: The pyodbc connection object.
        table_name: The name of the table to delete from.
        where_clause: An optional WHERE clause for filtering data.
    Returns:
        True if the data was deleted successfully, False otherwise.
    """
    try:
        cursor = conn.cursor()
        delete_sql = f"DELETE FROM {table_name}"
        if where_clause:
            delete_sql += f" WHERE {where_clause}"
        cursor.execute(delete_sql)
        conn.commit()
        print(f"Data deleted from table '{table_name}' successfully.")
        return True
    except pyodbc.Error as ex:
        print(f"Error deleting data from table '{table_name}' : {ex}")
        return False
    finally:
        cursor.close()

def get_table_schema(conn, table_name):
    """
    Retrieves the schema of a table in the SQL Server database.
    Args:
        conn: The pyodbc connection object ( The connection to the SQL Server database)
        table_name: The name of the table to get the schema for.
    Returns:
        A list of column names as strings.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        column_names = [row[0] for row in cursor.fetchall()]
        return column_names # Return a lisst of column names.
    except pyodbc.Error as ex:
        print(f"Error retrieving schema for table '{table_name}' : {ex}")
        return []
    finally:
        cursor.close()

def get_table_schema_columnname_datatype_nullability(conn, table_name):
    """
    Retrieves the schema of a table in the SQL Server database, including column names, data types and nullability.
    Args:
        A list of tuples, where each tuple represents a column: (column_name, data_type, length, is_nullable) 
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        schema = [(row[0], row[1], row[2], row[3] == 'YES') for row in cursor.fetchall()]
        return schema
    except pyodbc.Error as e:
        print(f"Error retrieving table schema: {e}")
        return None # Return None if an error occurs


def get_current_timestamp(conn):
    """
    Gets the current timestamp from the SQL Server database.
    Args:
        conn: The pyodbc connection object.
    Returns:
        A datetime object representing the current timestamp.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE()")
        timestamp = cursor.fetchone()[0]
        return timestamp
    except pyodbc.Error as ex:
        print(f"Error getting current timestamp: {ex}")

# Example usage
# Main function to interact with the database
def main():
    # Database connection settings
    dsn_name = 'MySQLServerDSN'
    db_name = 'db_TEST2'
    user = 'sardar'
    pwd = '1234'

    conn = connect_to_sql_server(dsn_name, db_name, user, pwd)
    while True:
        print("\nChoose an action:")
        print("1. Execute Custom Query")
        print("2. Create Table")
        print("3. Insert Data")
        print("4. Select Data")
        print("5. Update Data")
        print("6. Delete Data")
        print("7. Get Table Schema")
        print("8. Get Current Timestamp")
        print("9. List Tables")
        print("10. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            query = input("Enter your SQL query: ")
            execut_custom_query(conn, query)
        elif choice == '2':
            table_name = input("Enter table name: ")
            columns = input("Enter column names (comma-separated, without any SPACE) (For example: C1,INT,,FALSE;C2,VARCHAR,50,FALSE): \n")
            create_table(conn, table_name, columns)
        elif choice == '3':
            table_name = input("Enter table name: ")
            data = input("Enter data (comma-seperated values, rows separated by semicolon): ")
            insert_data(conn, table_name, data)
        elif choice == '4':
            table_name = input("Enter table name: ")
            columns = input("Enter columns to select (optional, comma-separated): ")
            where_clause = input("Enter WHERE clause (optional): ")
            print(select_data(conn, table_name, columns, where_clause))
        elif choice == '5':
            table_name = input("Enter table name: ")
            set_values = input("Enter values to set (column= value, comma-separated): ")
            where_clause = input("Enter WHERE clause (optional): ")
            update_data(conn, table_name, set_values, where_clause)
        elif choice == '6':
            table_name = input("Enter table name: ")
            where_clause = input("Enter WHERE clause (optional): ")
            delete_data(conn, table_name, where_clause)
        elif choice == '7':
            table_name = input("Enter table name: ")
            print(get_table_schema_columnname_datatype_nullability(conn, table_name))
        elif choice == '8':
            print(get_current_timestamp(conn))
        
        elif choice == '9':
            list_tables(conn)
        elif choice == '10':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")
    
    if conn:
        conn.close()
if __name__ == "__main__":
    main()
"""if conn:
    cursor = conn.cursor()
    print("Connection successful!")
    q = "SELECT * FROM db_TEST2.dbo.t"
    cursor.execute(q)

    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    cursor.execute(q)
    
else:
    print("Connection failed.")

if conn:
    table_name = input("Enter table name: ")
    columns = []
    while True:
        column_name = input("Enter column name (or 'done' to finish): ")
        if column_name.lower() == 'done':
            break
        data_type = input("Enter data type: ")
        is_nullable = input("Is nullable (True/False)?").lower() == 'true'

    columns.append((column_name, data_type, is_nullable))
    create_table(conn, table_name, columns)
##################################
    print(select_data(conn, 't3'))
    """