from DbConnector import DbConnector
from tabulate import tabulate
import os
import pandas as pd
import numpy as np
import geopy.distance
from haversine import haversine, Unit
from math import radians
import pickle
from datetime import datetime 
import time
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
            print ("TASK 2.1 - How many users, activities and trackpoints are there in the dataset (after it is inserted into the database\n")

            count_users = program.get_request("""select count(*) from user;""")
            print("amount of users: " + str(count_users[0][0]))

            count_activity = program.get_request("""select count(*) from activity;""")
            print("amount of activities: " + str(count_activity[0][0]))

            count_trackpoint = program.get_request("""select count(*) from trackpoint;""")
            print("amount of trackpoints: " + str(count_trackpoint[0][0]))

            print("\n----------------------------------\n")

            # task2.2
            print ("TASK 2.2 - Find the average, maximum and minimum number of trackpoints per user\n")
            avgminmax_trackpoints = program.get_request("""select AVG(total_trackpoints) as avg_trackpoints, MIN(total_trackpoints) as min_trackpoints, MAX(total_trackpoints) as max_trackpoints from (SELECT activity.user_id, COUNT(trackpoint.id) AS total_trackpoints FROM trackpoint JOIN activity ON trackpoint.activity_id = activity.id GROUP BY activity.user_id) as subquery;""")
            print("avg trackpoints: ", avgminmax_trackpoints[0][0])
            print("max trackpoints: ", avgminmax_trackpoints[0][2])
            print("min trackpoints: ", avgminmax_trackpoints[0][1])

            print("\n----------------------------------\n")

            # task2.3
            print ("TASK 2.3 - Find the top 15 users with the highest number of activities\n")
            top15_highest_activites = program.get_request("""SELECT activity.user_id, COUNT(activity.id) AS total_activities FROM activity GROUP BY activity.user_id ORDER BY total_activities DESC LIMIT 15;""")
            print("Top 15 users highest activites \n")
            print(tabulate(top15_highest_activites, headers=["User", "Activities"]))

            print("\n----------------------------------\n")

            # #task2.4
            print ("TASK 2.4 - Find all users who have taken a bus\n")
            user_bus = program.get_request("""SELECT user_id FROM activity WHERE transportation_mode = "bus" GROUP BY user_id;""")
            print("Users who have taken the bus\n")
            print(tabulate(user_bus, headers=["User"]))

            print("\n----------------------------------\n")

            #Task2.5
            print ("TASK 2.5 - List the top 10 users by their amount of different transportation modes\n")
            diff_transport = program.get_request("""SELECT user_id, COUNT(DISTINCT transportation_mode) as distinct_mode FROM activity GROUP BY user_id ORDER BY distinct_mode DESC LIMIT 10;""")
            print("Top 10 users by the amount of different transportation modes \n")
            print(tabulate(diff_transport, headers=["User", "Different transportation modes"]))

            print("\n----------------------------------\n")

            # Task2.6
            print ("TASK 2.6 - Find activities that are registered multiple times. You should find the query even if it gives zero result\n")
            duplicates = program.get_request("""SELECT user_id, start_date_time, end_date_time, transportation_mode, COUNT(*) FROM activity GROUP BY user_id, start_date_time, end_date_time, transportation_mode HAVING COUNT(*)>1;""")
            print(duplicates)
            if (duplicates == []):
                print("Empty set returned, meaning there are no duplicates")

            print("\n----------------------------------\n")

            # Task2.7a
            print ("TASK 2.7 A) - Find the number of users that have started an activity in one day and ended the activity the next day\n")
            overnight_users = program.get_request("""SELECT COUNT(DISTINCT user_id) as Overnight_users FROM activity WHERE DATEDIFF(start_date_time, end_date_time) <> 0 AND DATEDIFF(start_date_time, end_date_time) <= 1;""")
            print("Total users who have an activity which last until the next day: " + str(overnight_users[0][0]))

            # Task2.7b
            print ("TASK 2.7 B) - List the transportation mode, user id and duration for these activities\n")
            list_overnight_activites = program.get_request("""SELECT IFNULL(transportation_mode, "-"), user_id, TIMEDIFF(end_date_time, start_date_time) as duration FROM activity WHERE DATEDIFF(start_date_time, end_date_time) <> 0 AND DATEDIFF(start_date_time, end_date_time) <= 1 LIMIT 20 OFFSET 620;""")
            print("\nAll activites which last until the next day (limit 20, offset 630): ")
            print(tabulate(list_overnight_activites, headers=["Transportation mode", "User", "Duration"]))

            print("\n----------------------------------\n")

            # Task2.8
            # warning this Task takes a long time to run.
            
            print("TASK 2.8) - Find the number of users which have been close to each other in time and space\n")

            cache_file = 'user_trackpoints.pkl'

            # Load data from cache or query database if not cached
            if os.path.isfile(cache_file):
                with open(cache_file, 'rb') as file:
                    user_trackpoints = pickle.load(file)
            else:
                users = program.get_request("SELECT id FROM user;")
                user_ids = [row[0] for row in users]
                user_trackpoints = {}

                for user_id in user_ids:
                    trackpoints = program.get_request(
                        f"SELECT lat, lon, date_time FROM trackpoint WHERE activity_id IN (SELECT id FROM activity WHERE user_id = {user_id});")
                    trackpoints = [(lat, lon, time.timestamp())
                                   for lat, lon, time in trackpoints]

                    user_trackpoints[user_id] = trackpoints
                    print(user_id)

                with open(cache_file, 'wb') as file:
                    pickle.dump(user_trackpoints, file)

            # Initialize the start time for the entire process
            total_start_time = time.time()

            pairs = set()
            result_table = []

            # Iterate through pairs and perform comparisons
            user_ids = list(user_trackpoints.keys())
            max_running_time = 0.01  # Max running time for elapsed time

            # Iterate through pairs and perform comparisons
            for i in range(len(user_ids)):
                user_id1 = user_ids[i]
                for j in range(i + 1, len(user_ids)):
                    user_id2 = user_ids[j]
                    pair = (user_id1, user_id2)
                    #print("Testing pair:", pair)

                    start_time = time.time()
                    row_count = 0
                    valid_pair_found = False

                    for trackpoint1 in user_trackpoints[user_id1]:
                        lat1, lon1, time1 = trackpoint1
                        for trackpoint2 in user_trackpoints[user_id2]:
                            lat2, lon2, time2 = trackpoint2
                            timediff = abs(time1 - time2)
                            distance = round(
                                haversine((lat1, lon1), (lat2, lon2), unit=Unit.METERS), 1)
                            row_count += 1
                            if timediff <= 30 and distance <= 50:
                                pairs.add(pair)
                                result_table.append(
                                    [pair, timediff, distance, row_count])
                                print("Valid pair:", pair, ", Timediff:",
                                      timediff, ", Distance:", distance, ", Rows compared:", row_count)
                                valid_pair_found = True
                                break

                            elapsed_time = time.time() - start_time
                            if elapsed_time > max_running_time:
                                valid_pair_found = True
                                break

                        if valid_pair_found:
                            break  # Break the outer loop

            # Calculate the total elapsed time for pair comparisons only
            total_elapsed_time = time.time() - total_start_time

            # Convert total elapsed time into minutes and seconds
            total_minutes = int(total_elapsed_time // 60)
            total_seconds = total_elapsed_time % 60

            pair_count = len(result_table)
            print()
            print(
                f"Total elapsed time for pair comparisons: {total_minutes} minutes {total_seconds:.2f} seconds")
            print("Time limit for comparing each pair:", max_running_time)
            print("Amount of pairs:", pair_count)
            headers = ["Pair", "Timediff", "Distance",
                       "Rows compared until match"]
            print("The pairs:")
            print(tabulate(result_table, headers, tablefmt="grid"))		
           
            print("\n----------------------------------\n")

            #Task2.9
            # warning this Task takes a long time to run.
            print ("TASK 2.9) - Find the top 15 users who have gained the most altitude meters\n")
            altitude_gain = program.get_request("""SELECT user_id, SUM(sq.hightgain) * 0.3048 as user_highgain FROM (SELECT ssq.user_id, ssq.activity_id, SUM(CASE WHEN ssq.diff > 0 THEN ssq.diff ELSE 0 END) as hightgain FROM (SELECT user_id, activity_id, altitude, altitude - LAG(altitude) OVER (ORDER BY activity_id) as diff FROM trackpoint JOIN activity on trackpoint.activity_id = activity.id) as ssq GROUP BY ssq.activity_id) as sq GROUP BY sq.user_id ORDER BY user_highgain DESC LIMIT 15;""")
            print("Users who have gained the most altitude (meters): \n")
            print(tabulate(altitude_gain, headers=["User", "Altitude gain (m)"]))

            print("\n----------------------------------\n")

            # Task2.10
            # warning this Task takes a long time to run.

            print ("TASK 2.10) - Find the users that have traveled the longest total distance in one day for each transportation mode\n")
            print("Users who have traveled the furthest using a transportation mode: ")
            
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
            # warning this Task takes a long time to run.
            print ("TASK 2.11) - Find all users who have invalid activities, and the number of invalid activities per use.\n")
            errors = program.get_request("""SELECT activity.user_id, COUNT(DISTINCT(activity.id)) as 'invalid activities' FROM (SELECT t1.activity_id as activity_id, ABS(t2.date_days - t1.date_days) AS time_diff FROM trackpoint t1 JOIN trackpoint t2 on t1.activity_id = t2.activity_id and t1.id+1 = t2.id HAVING time_diff >= 0.00347222) as subquery JOIN activity on activity.id = subquery.activity_id GROUP BY activity.user_id LIMIT 20;""")
            print("All users how have invalid activities (limit 20): \n")
            print(tabulate(errors, headers=["User", "ant invalid"]))

            print("\n----------------------------------\n")

            #Task2.12
            print ("TASK 2.12) - Find all users who have registered transportation_mode and their most used transportation_mode\n")
            most_common_data = program.get_request("""SELECT user_id, transportation_mode FROM activity WHERE transportation_mode IS NOT NULL;""")
            
            print("The users with labeles most common transportation mode: ")
            
            df = pd.DataFrame(most_common_data, columns=["user", "mode"])
            for user in df["user"].unique():
                common = df[df["user"] == user]["mode"].value_counts()[:1].index.tolist() # return first encounterd equaly common
                print("User: ", user, ", most common transport mode: ", str(common[0]), sep="")

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
