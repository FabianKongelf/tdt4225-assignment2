from DbConnector import DbConnector
from tabulate import tabulate
import os

class Task2():
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def get_request(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

def main():
    program = None

    # init program
    try:
        program = Task2()

        # do task2
        try:
            # task2.1
            count_users = program.get_request("""select count(*) from user;""")
            print("ant users: " + str(count_users[0][0]))

            count_activity = program.get_request("""select count(*) from activity;""")
            print("ant activities: " + str(count_activity[0][0]))

            count_trackpoint = program.get_request("""select count(*) from trackpoint;""")
            print("ant trackpoint: " + str(count_trackpoint[0][0]))

            print("\n----------------------------------\n")

            # task2.2
            avgminmax_trackpoints = program.get_request("""select AVG(total_trackpoints) as avg_trackpoints, MIN(total_trackpoints) as min_trackpoints, MAX(total_trackpoints) as max_trackpoints from (SELECT activity.user_id, COUNT(trackpoint.id) AS total_trackpoints FROM trackpoint JOIN activity ON trackpoint.activity_id = activity.id GROUP BY activity.user_id) as subquery;""")
            print("avg trackpoints: ", avgminmax_trackpoints[0][0])
            print("min trackpoints: ", avgminmax_trackpoints[0][1])
            print("max trackpoints: ", avgminmax_trackpoints[0][2])

            print("\n----------------------------------\n")


        except Exception as e:
            print("ERROR: failed to do task2: ", e)

    # failed init program
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    # after try-catch block is finished
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()