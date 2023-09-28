from DbConnector import DbConnector
from tabulate import tabulate
import os

class Task1():
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, query):
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

def main():
    program = None

    # init program
    try:
        program = Task1()

        # create tables
        try:
            # user table
            program.create_table("""CREATE TABLE IF NOT EXISTS user (
                                 id VARCHAR(3) NOT NULL PRIMARY KEY, 
                                 has_label BOOLEAN NOT NULL DEFAULT FALSE)""")
            
            # activity table
            program.create_table("""CREATE TABLE IF NOT EXISTS activity (
                                 id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
                                 user_id VARCHAR(3) NOT NULL, 
                                 transportation_mode VARCHAR(100),
                                 start_date_time DATETIME NOT NULL,
                                 end_date_time DATETIME NOT NULL)""")
            
            # trackpoint
            program.create_table("""CREATE TABLE IF NOT EXISTS trackpoint (
                                 id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
                                 activity_id INT NOT NULL,
                                 lat DOUBLE PRECISION NOT NULL,
                                 lon DOUBLE PRECISION NOT NULL,
                                 altitude INT,
                                 date_days DOUBLE PRECISION NOT NULL,
                                 date_time DATETIME NOT NULL)""")
        
        except Exception as e:
            print("ERROR: Failed to create Table: ", e)
        
        finally:
            # insert data
            try:
                current_location = os.path.dirname(__file__)
                dataset_location = os.path.join(current_location, "dataset", "dataset")
                user_data = os.path.join(dataset_location, "Data")
                user_list = os.listdir(user_data)

                try:
                    with open(os.path.join(user_data, "labeled_ids.txt"), "r") as file:
                        labels = file.read()
                except FileNotFoundError:
                    print("ERROR cant find file")
                except Exception as e:
                    print("ERROR cant read labels file: ", e)
                
                print(user_list)
                print(labels)

                # program.insert_data()

            except Exception as e:
                print("ERROR: Failed inserting data: ", e)


        
        
        
        # program.insert_data(table_name="Person")
        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="Person")
        # Check that the table is dropped
        # program.show_tables()
    
    # failed init program
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    # after try-catch block is finished
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()