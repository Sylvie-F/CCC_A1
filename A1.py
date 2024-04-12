import re
import os
import datetime
import math
from mpi4py import MPI
from collections import defaultdict
from collections import Counter

# start the timer
processing_start_time = datetime.datetime.now()

# mpi related variables
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# structure to store info
happy_hour_dict = defaultdict(float)
happy_day_dict = defaultdict(float)
active_hour_dict = Counter()
active_day_dict = Counter()

# merge for the final result
def merge(dict1, dict2):
    for key, value in dict2.items():
        dict1[key] += value
    return dict1

# extract info needed from lines read in
created_at_pattern = re.compile(r'"created_at":"(\d{4}-\d{2}-\d{2})T(\d{2})')
sentiment_pattern = re.compile(r'"sentiment":([\d\.-]+)')

def parse_info(line):
    hour, day, sentiment = "Unknown", "Unknown", 0.0
    created_at_match = created_at_pattern.search(line)
    sentiment_match = sentiment_pattern.search(line)
    
    if created_at_match:
        day, hour = created_at_match.groups()
    
    if sentiment_match:
        sentiment = float(sentiment_match.group(1))
    
    return f"{day}T{hour}", day, sentiment


total_bytes = os.path.getsize('./twitter-100gb.json')
each_bytes = math.ceil(total_bytes / size)
begin_position = rank * each_bytes
end_position = (rank + 1) * each_bytes

with open('./twitter-100gb.json', 'r', encoding='utf-8') as twit_file:

    twit_file.seek(begin_position)

    # skip first line
    twit_file.readline()

    # stop when reach the end of the file
    while True:

        twit = twit_file.readline()
        if not twit:
            break
        hour, day, sentiment = parse_info(twit)

        active_hour_dict[hour] += 1
        active_day_dict[day] += 1
        happy_hour_dict[hour] += sentiment
        happy_day_dict[day] += sentiment

        # each process handle the line to the end
        if twit_file.tell() >= end_position:
            break

processing_end_time = datetime.datetime.now()
# gather the result of each process
processing_time = (processing_end_time - processing_start_time).total_seconds()
result_list = comm.gather([happy_hour_dict, happy_day_dict, active_hour_dict, active_day_dict,processing_time], root=0)

if rank == 0:
    output_start_time = datetime.datetime.now()

    # collect result
    result_happy_hour_dict = defaultdict(float)
    result_happy_day_dict = defaultdict(float)
    result_active_hour_dict = Counter()
    result_active_day_dict = Counter()

    # merge result list
    for dict_list in result_list:
        result_happy_hour_dict = merge(result_happy_hour_dict,dict_list[0])
        result_happy_day_dict = merge(result_happy_day_dict,dict_list[1])
        result_active_hour_dict.update(dict_list[2])
        result_active_day_dict.update(dict_list[3])

    happiest_hour = max(result_happy_hour_dict, key=result_happy_hour_dict.get)
    happiest_day = max(result_happy_day_dict, key=result_happy_day_dict.get)
    most_active_hour = max(result_active_hour_dict, key=result_active_hour_dict.get)
    most_active_day = max(result_active_day_dict, key=result_active_day_dict.get)

    happiest_hour_split = happiest_hour.split('T')
    most_active_hour_split = most_active_hour.split('T')    

    happiest_hour_hour = int(happiest_hour_split[1])
    most_active_hour_hour = int(most_active_hour_split[1])

    print(f"Happiest Hour: {happiest_hour_hour}:00-{happiest_hour_hour + 1}:00 on {happiest_hour_split[0]} was the happiest hour with an overall sentiment score of {result_happy_hour_dict[happiest_hour]}")
    print(f"Happiest Day: {happiest_day} was the happiest day with an overall sentiment score of {result_happy_day_dict[happiest_day]}")
    print(f"Most Active Hour: {most_active_hour_hour}:00-{most_active_hour_hour + 1}:00 on {most_active_hour_split[0]} had the most tweets(#{result_active_hour_dict[most_active_hour]})")
    print(f"Most Active Day: {most_active_day} had the most tweets(#{result_active_day_dict[most_active_day]})")

    output_end_time = datetime.datetime.now()
    
    communication_time = (output_start_time - processing_end_time).total_seconds()
    output_time = (output_end_time - output_start_time).total_seconds()
    processing_times = [item[4] for item in result_list]  
    avg_processing_time = sum(processing_times) / len(processing_times)
    total_time = (output_end_time - processing_start_time).total_seconds()

    print(f"Average Parallel Processing Time: {avg_processing_time} seconds")
    print(f"Max Parallel Processing Time: {max(processing_times)} seconds")
    print(f"Parallel Processing Times per Process: {processing_times}")
    print(f"Output Calculation Time: {output_time} seconds")
    print(f"Total time: {total_time} seconds")