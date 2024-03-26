import re
import os
import datetime
from mpi4py import MPI
from collections import defaultdict
from collections import Counter

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
each_bytes = total_bytes // size
begin_position = rank * each_bytes
end_position = (rank + 1) * each_bytes

processing_start_time = datetime.datetime.now()

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

        begin_position += len(twit.encode('utf-8'))
        # each process handle the line to the end
        if begin_position >= end_position:
            break

processing_end_time = datetime.datetime.now()
# gather the result of each process
processing_time = (processing_end_time - processing_start_time).total_seconds()
result_list = comm.gather([happy_hour_dict, happy_day_dict, active_hour_dict, active_day_dict,processing_time], root=0)

if rank == 0:
    # merge result list
    output_start_time = datetime.datetime.now()
    for dict_list in result_list:
        happy_hour_dict = merge(happy_hour_dict,dict_list[0])
        happy_day_dict = merge(happy_day_dict,dict_list[1])
        active_hour_dict.update(dict_list[2])
        active_day_dict.update(dict_list[3])

    happiest_hour = max(happy_hour_dict, key=happy_hour_dict.get)
    happiest_day = max(happy_day_dict, key=happy_day_dict.get)
    most_active_hour = max(active_hour_dict, key=active_hour_dict.get)
    most_active_day = max(active_day_dict, key=active_day_dict.get)

    print(f"Happiest Hour: {happiest_hour}")
    print(f"Happiest Day: {happiest_day}")
    print(f"Most Active Hour: {most_active_hour}")
    print(f"Most Active Day: {most_active_day}")

    output_end_time = datetime.datetime.now()
    
    communication_time = (output_start_time - processing_end_time).total_seconds()
    output_time = (output_end_time - output_start_time).total_seconds()
    processing_times = [item[4] for item in result_list]  
    avg_processing_time = sum(processing_times) / len(processing_times)

    print(f"Average Processing Time: {avg_processing_time} seconds")
    print(f"Processing Times per Process: {processing_times}")
    print(f"Communication Time: {communication_time} seconds")
    print(f"Output Time: {output_time} seconds")
