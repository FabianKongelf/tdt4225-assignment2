from DbConnector import DbConnector
from tabulate import tabulate

class Task1():
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, query):
        self.cursor.execute(query)
        self.db_connection.commit()

def main():
    program = None
    try:
        program = Task1()

        try:
            program.create_table("""CREATE TABLE IF NOT EXISTS user (id VARCHAR(3) NOT NULL PRIMARY KEY, has_label BOOLEAN DEFAULT FALSE)""")
        
        except:
        
        
        
        # program.insert_data(table_name="Person")
        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="Person")
        # Check that the table is dropped
        # program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()