from DbConnector import DbConnector
from tabulate import tabulate
import os
import pandas as pd
import numpy as np
import geopy.distance
# from datetime import datetime

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
            print("total amount of rows in database")

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

            # task2.3
            top15_highest_activites = program.get_request("""SELECT activity.user_id, COUNT(activity.id) AS total_activities FROM activity GROUP BY activity.user_id ORDER BY total_activities DESC LIMIT 15;""")
            print("Top 15 users highest activites \n")
            print(tabulate(top15_highest_activites, headers=["User", "Activities"]))

            print("\n----------------------------------\n")

            #task2.4
            user_bus = program.get_request("""SELECT user_id FROM activity WHERE transportation_mode = "bus" GROUP BY user_id;""")
            print("Users who have taken the bus\n")
            print(tabulate(user_bus, headers=["User"]))

            print("\n----------------------------------\n")

            #Task2.5
            diff_transport = program.get_request("""SELECT user_id, COUNT(DISTINCT transportation_mode) as distinct_mode FROM activity GROUP BY user_id ORDER BY distinct_mode DESC LIMIT 10;""")
            print("Top 10 users by the amount of different transportation modes \n")
            print(tabulate(diff_transport, headers=["User", "Different transportation modes"]))

            print("\n----------------------------------\n")

            #Task2.6
            duplicates = program.get_request("""SELECT user_id, start_date_time, end_date_time, transportation_mode FROM activity GROUP BY user_id, start_date_time, end_date_time, transportation_mode HAVING (Count(user_id) > 1) and (Count(start_date_time) > 1) and (Count(end_date_time) > 1) and (Count(transportation_mode) > 1);""")
            print(duplicates)

            print("\n----------------------------------\n")

            #Task2.7a
            overnight_users = program.get_request("""SELECT COUNT(DISTINCT user_id) as Overnight_users FROM activity WHERE DATEDIFF(start_date_time, end_date_time) <> 0 AND DATEDIFF(start_date_time, end_date_time) <= 1;""")
            print("Total users who have an activity which last until the next day: " + str(overnight_users[0][0]))

            #Task2.7b
            list_overnight_activites = program.get_request("""SELECT IFNULL(transportation_mode, "-"), user_id, TIMEDIFF(end_date_time, start_date_time) as duration FROM activity WHERE DATEDIFF(start_date_time, end_date_time) <> 0 AND DATEDIFF(start_date_time, end_date_time) <= 1 LIMIT 20 OFFSET 620;""")
            print("\nAll activites which last until the next day (limit 20, offset 630): ")
            print(tabulate(list_overnight_activites, headers=["Transportation mode", "User", "Duration"]))

            print("\n----------------------------------\n")

            #Task2.8
            # close_calles = program.get_request("""""")
            # print("Users how have been close to each other: ")
            # print(tabulate(close_calles, headers=[]))

            # select a1.user_id, a1.id, t1.lat, t1.lon, t1.date_days, sq.user_id, sq.id, sq.lat, sq.lon, sq.date_days from trackpoint t1 join activity a1 on t1.activity_id = a1.id join (select a2.user_id, a2.id, t2.lat, t2.lon, t2.date_days from trackpoint t2 join activity a2 on t2.activity_id = a2.id) as sq on sq.user_id <> a1.user_id and abs(t1.date_days - sq.date_days) < 0.00003472 limit 20;


            print("\n----------------------------------\n")

            #Task2.9
            altitude_gain = program.get_request("""SELECT user_id, SUM(sq.hightgain) * 0.3048 as user_highgain FROM (SELECT ssq.user_id, ssq.activity_id, SUM(CASE WHEN ssq.diff > 0 THEN ssq.diff ELSE 0 END) as hightgain FROM (SELECT user_id, activity_id, altitude, altitude - LAG(altitude) OVER (ORDER BY activity_id) as diff FROM trackpoint JOIN activity on trackpoint.activity_id = activity.id) as ssq GROUP BY ssq.activity_id) as sq GROUP BY sq.user_id ORDER BY user_highgain DESC LIMIT 15;""")
            print("Users how have gain the most altitude (meter): ")
            print(tabulate(altitude_gain, headers=["User", "Altitude gain (m)"]))

            print("\n----------------------------------\n")

            # #Task2.10
            print("Users who have traveled the futhest using a transportation mode: ")

            # as geografical data requiers more complex mathematical formulas we opt to do the data processing outside of sql
            modes = program.get_request("""select distinct(transportation_mode) as modes from activity;""")
            for mode in modes:
                if mode[0]: # filter out NULL value as it is unintresting
                    user_distance = program.get_request("""select activity.user_id, activity.id, activity.transportation_mode, trackpoint.lat, trackpoint.lon, trackpoint.date_time from trackpoint join activity on activity.id = trackpoint.activity_id where activity.transportation_mode = "%s";""" % mode[0])
                    df = pd.DataFrame(user_distance, columns=["user", "activity", "mode", "lat", "lon", "datetime"])
                    user_list = df["user"].unique()
                    res = pd.DataFrame(user_list, columns=["user"])
                    res["distance"] = float()

                    for user in user_list:
                        udf = df[df["user"] == user]
                        udf = udf.reset_index(drop=True)
                        ubest = [float(), None]

                        all_activities = udf["activity"].unique()
                        for activity in all_activities:
                            adf = udf[udf["activity"] == activity]
                            adf = adf.reset_index(drop=True)

                            start_date = adf.at[0, "datetime"]
                            end_date = adf.at[(len(adf)-1), "datetime"]

                            if start_date.date() == end_date.date():
                                tot_dis = 0
                                for i in range(1, len(adf)):
                                    coord1 = (adf.at[(i-1), "lat"], adf.at[(i-1), "lon"])   
                                    coord2 = (adf.at[i, "lat"], adf.at[i, "lon"])
                                    tot_dis += geopy.distance.geodesic(coord1, coord2).km

                                if ubest[1] and ubest[1].date() == start_date.date():
                                    ubest[0] += tot_dis
                                elif ubest[0] < tot_dis:
                                    ubest[0] = tot_dis
                                    ubest[1] = start_date
                        
                        index = res.index[res["user"] == adf.iloc[0]["user"]]
                        if not index.empty:
                            res.loc[index[0], "distance"] = round(ubest[0], 6)

                    max_index = res["distance"].idxmax()
                    row = res.iloc[max_index]
                    print(mode[0], "\t - ", "user: ", row["user"], ", distance: ", row["distance"],"km", sep="")
                                                
            print("\n----------------------------------\n")

            #Task2.11
            errors = program.get_request("""SELECT activity.user_id, COUNT(DISTINCT(activity.id)) as 'invalid activities' FROM (SELECT t1.activity_id as activity_id, ABS(t2.date_days - t1.date_days) AS time_diff FROM trackpoint t1 JOIN trackpoint t2 on t1.activity_id = t2.activity_id and t1.id+1 = t2.id HAVING time_diff >= 0.00347222) as subquery JOIN activity on activity.id = subquery.activity_id GROUP BY activity.user_id LIMIT 20;""")
            print("All users how have invalid activities (limit 20): ")
            print(tabulate(errors, headers=["User", "ant invalid"]))

            print("\n----------------------------------\n")

            #Task2.12
            most_common_data = program.get_request("""SELECT user_id, transportation_mode FROM activity WHERE transportation_mode IS NOT NULL;""")
            
            print("The users with labeles most common transportation mode: ")
            
            df = pd.DataFrame(most_common_data, columns=["user", "mode"])
            for user in df["user"].unique():
                common = df[df["user"] == user]["mode"].value_counts()[:1].index.tolist() # return first encounterd equaly common
                print("User: ", user, ", most common transport mode: ", str(common[0]), sep="")

            # "https://gitlab.stud.idi.ntnu.no/trymg/exercise-2-tdt4225/-/blob/master/src/main.py?ref_type=heads"


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
