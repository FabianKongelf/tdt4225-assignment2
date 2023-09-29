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
        last_insert_id = self.cursor.lastrowid
        self.db_connection.commit()
        return last_insert_id

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
        
        # insert data
        finally:
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

                # _ = program.insert_data("""INSERT INTO user (id, has_label) VALUES (%s, %s)""", user_list_converted)

            except Exception as e:
                print("ERROR: Failed inserting users: ", e)
            finally:
                print("-- inserted users")

            # insert activitys
            try:
                for user in user_list_converted:
                    user_id = user[0]
                    user_data_path = os.path.join(user_data, user_id)
                    # print(user)
                    
                    # if user has labels read them
                    lables = []
                    if user[1] == 1:
                        # inseter labels    
                        labels_path = os.path.join(user_data_path, "labels.txt")
                        with open(labels_path, "r") as file:
                            lines = file.readlines()[1:]
                            for line in lines:
                                array_line = line.strip().split("\t")
                                array_line.append(user[0])
                                lables.append(array_line)

                    # find all files assosiaded with user
                    files_path = os.path.join(user_data_path, "Trajectory")
                    files = os.listdir(files_path)

                    # loop on all files for user
                    for file_name in files:
                        file_path = os.path.join(files_path, file_name)

                        # read file data
                        file_data = []
                        with open(file_path, "r") as file:
                            lines = file.readlines()[6:]
                            for line in lines:
                                converted_line = line.strip().split(",")

                                # correct datatime 
                                datetime = converted_line[-2] + " " + converted_line[-1]
                                datetime = datetime.replace("-", "/")
                                converted_line[-2:] = [datetime]

                                # ajust altitute
                                if converted_line[3] == -777:
                                    if len(file_data) > 0:
                                        converted_line[3] = file_data[-1][2]
                                    else:
                                        converted_line[3] = 0

                                # remove field 3
                                converted_line.pop(2)

                                file_data.append(converted_line)
                        
                        # ignorre data if longer then 2500
                        if len(file_data) > 2500:
                            continue
                
                        # create user activity
                        user_activity = [user_id, file_data[0][4], file_data[-1][4]]
                        query = """INSERT INTO activity (user_id, start_date_time, end_date_time) VALUES (%s, %s, %s)"""
              
                        # if user has labels
                        if user[1] == 1:
                            for row in lables:
                                if row[0] == file_data[0][4] and row[1] == file_data[-1][4]:
                                    # print(row[2])
                                    user_activity = [user_id, row[2], file_data[0][4], file_data[-1][4]]
                                    query = """INSERT INTO activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"""
                        
                        # insert activity
                        activity_id = program.insert_data(query, [user_activity])
                        
                        # insert trackpoint
                        new_file_data = [inner_array + [activity_id] for inner_array in file_data]
                        _ = program.insert_data("""INSERT INTO trackpoint (lat, lon, altitude, date_days, date_time, activity_id) VALUES (%s, %s, %s, %s, %s, %s)""", new_file_data)

                        # print(content)
                    print("-- inserted data for: " + user[0])

            except Exception as e:
                print("ERROR: Failed inserting activity: ", e)
            finally:
                print("-- insertes done")
    
    # failed init program
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    # after try-catch block is finished
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()