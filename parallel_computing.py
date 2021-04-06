###Parallel computing

##Using multiprocessing.Pool API
import pandas as pd
athlete_events = pd.read_csv(r'/home/felipe/miniconda3/envs/data_e/workspace/datasources/athlete_events.csv')

from multiprocessing import Pool 

def take_mean_age(year_and_group):
    year, group = year_and_group
    return pd.DataFrame({"Age": group["Age"].mean()}, index = [year])

with Pool(3) as p:
    results = p.map(take_mean_age,athlete_events.groupby("Year"))

result_df = pd.concat(results)

# Function to apply a function over multiple cores
@print_timing
def parallel_apply(apply_func, groups, nb_cores):
    with Pool(nb_cores) as p:
        results = p.map(apply_func, groups)
    return pd.concat(results)

# Parallel apply using 1 core
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 1)

# Parallel apply using 2 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 2)

# Parallel apply using 4 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 3)

##Using Dask
import dask.dataframe as dd
#Partition datafrtame in 3
athlete_events_dask = dd.from_pandas(athlete_events, npartitions = 3)

#Run parallel computation in each partition
result_df = athlete_events_dask.groupby("Year").Age.mean().compute()

#Pyspark groupby
# Print the type of athlete_events_spark
print(type(athlete_events_spark))

# Print the schema of athlete_events_spark
print(athlete_events_spark.printSchema())

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age'))

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age').show())

