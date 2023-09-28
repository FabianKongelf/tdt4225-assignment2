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
                                 end_date_time DATETIME NOT NULL,
                                 FOREIGN KEY (user_id) REFERENCES user(id))""")
            
            # trackpoint
            program.create_table("""CREATE TABLE IF NOT EXISTS trackpoint (
                                 id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
                                 activity_id INT NOT NULL,
                                 lat DOUBLE PRECISION NOT NULL,
                                 lon DOUBLE PRECISION NOT NULL,
                                 altitude INT,
                                 date_days DOUBLE PRECISION NOT NULL,
                                 date_time DATETIME NOT NULL,
                                 FOREIGN KEY (activity_id) REFERENCES activity(id))""")
        
        except Exception as e:
            print("ERROR: Failed to create Table: ", e)
        
        finally:
            # insert data
            current_location = os.path.dirname(__file__)
            dataset_location = os.path.join(current_location, "dataset", "dataset")
            user_data = os.path.join(dataset_location, "Data")
            user_list_converted = []

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
                
                for user_id in user_list:
                    has_labels = 1 if user_id in labels else 0
                    user_list_converted.append((user_id, has_labels))

                # program.insert_data("""INSERT INTO user (id, has_label) VALUES (%s, %s)""", user_list_converted)

            except Exception as e:
                print("ERROR: Failed inserting user: ", e)
            finally:
                print("-- inserted users")

            # insert activitys
            try:
                for user in user_list_converted:
                    id = user[0]
                    user_data_path = os.path.join(user_data, id)
                    print(user)
                    
                    if user[1] == 1:
                        # inseter labels
                        activities = []
                        labels_path = os.path.join(user_data_path, "labels.txt")
                        with open(labels_path, "r") as file:
                            lines = file.readlines()[1:]
                            for line in lines:
                                array_line = line.strip().split("\t")
                                print(array_line)
                                # converted_line = [user[0], array_line[0] + " " + array_line[1], array_line[2] + " " + array_line[3], array_line[4]]
                                array_line.append(user[0])
                                activities.append(array_line)

                        program.insert_data("""INSERT INTO activity (start_date_time, end_date_time, transportation_mode, user_id) VALUES (%s, %s, %s, %s)""", activities)
                    else:
                        print("hei")

                    files_path = os.path.join(user_data_path, "Trajectory")
                    files = os.listdir(files_path)
                
                    for file_name in files:
                        file_path = os.path.join(files_path, file_name)

                        file_data = []
                        with open(file_path, "r") as file:
                            lines = file.readlines()[6:]
                            for line in lines:
                                converted_line = line.strip().split(",")
                                datetime = converted_line[-2] + " " + converted_line[-1]
                                converted_line[-2:] = [datetime]
                                file_data.append(converted_line)
                        
                        if user[1] == 0:
                            user_activity = [id, file_data[0][5], file_data[-1][5]]


                        # print(content)
                    # print(data)

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