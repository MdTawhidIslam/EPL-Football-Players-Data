import os
import pandas as pd
import time
from overview import player_overview
from stats import player_statistics

# For timing purposes
start_time = time.time()

# Creating folders for storing players list and their statistics by season
current_dir = os.getcwd()
overview_path = os.path.join(current_dir, 'player_overview')
statistics_path = os.path.join(current_dir, 'player_statistics')
final_path = os.path.join(current_dir, 'player_final')

os.makedirs(overview_path, exist_ok=True)
os.makedirs(statistics_path, exist_ok=True)
os.makedirs(final_path, exist_ok=True)

print("The new directory is created!")


# Season ids dictionary (collected from webpage)

year = {
    "2021_22": "418",
    "2020_21": "363",
    "2019_20": "274",
    "2018_19": "210",
    "2017_18": "79",
    "2016_17": "54",
    "2015_16": "42",
    "2014_15": "27",
    "2013_14": "22",
    "2012_13": "21",
    "2011_12": "20",
    "2010_11": "19",
    "2009_10": "18",
    "2008_09": "17",
    "2007_08": "16",
    "2006_07": "15",
    "2005_06": "14",
    "2004_05": "13",
    "2003_04": "12",
    "2002_03": "11",
    "2001_02": "10",
    "2000_01": "9",
    "1999_00": "8",
    "1998_99": "7",
    "1997_98": "6",
    "1996_97": "5",
    "1995_96": "4",
    "1994_95": "3",
    "1993_94": "2",
    "1992_93": "1"
}
year_overview = year.copy()
year_statistics = year.copy()

# Sorting out which seasons to request
overview_path_files = list(map(lambda x: year_overview.pop(x[:-4], None), os.listdir(overview_path)))
statistics_path_files = list(map(lambda x: year_statistics.pop(x[:-4], None), os.listdir(statistics_path)))

print(year_overview, '\n', year_statistics)


# Running the functions
list(map(player_overview, year_overview.keys(), year_overview.values()))
list(map(player_statistics, year_statistics.keys(), year_statistics.values()))


# Merging the two files into single file
def final_merge():
    # All data into a single DataFrame
    df_main = pd.DataFrame()

    for key in year.keys():
        df_overview = pd.read_csv(os.path.join(overview_path, key+'.csv'))
        df_statistics = pd.read_csv(os.path.join(statistics_path, key+'.csv'))

        df = df_overview.merge(df_statistics, how='inner', on='id')
        df.to_csv(os.path.join(final_path, key+'.csv'), index=False)

        df_main = pd.concat([df_main, df], axis=0, ignore_index=True)

    print(df_main.shape)
    df_main.to_csv('Final_df.csv', index=False)


final_merge()
print("\nCompletion Time: --- %s seconds ---\n" % (time.time() - start_time))