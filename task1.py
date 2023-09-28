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

    def insert_data(self, query, data):
        self.cursor.executemany(query, data)
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
            current_location = os.path.dirname(__file__)
            dataset_location = os.path.join(current_location, "dataset", "dataset")
            user_data = os.path.join(dataset_location, "Data")
            
            # insert users
            try:
                user_list = os.listdir(user_data)
                label_file_path = os.path.join(dataset_location, "labeled_ids.txt")

                labels = None
                try:
                    with open(label_file_path, "r") as file:
                        labels = file.read()
                except FileNotFoundError:
                    print("ERROR cant find file")
                except Exception as e:
                    print("ERROR cant read labels file: ", e)
                
                user_list_converted = []
                for user_id in user_list:
                    has_labels = 1 if user_id in labels else 0
                    user_list_converted.append((user_id, has_labels))

                # program.insert_data("INSERT INTO user (id, has_label) VALUES (%s, %s)", user_list_converted)

            except Exception as e:
                print("ERROR: Failed inserting user: ", e)
            finally:
                print("-- inserted users")

            # insert activitys
            try:
                
            except Exception as e:
                print("ERROR: Failed inserting activity: ", e)
            finally:
                print("-- inserted activity")

        
        
        
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