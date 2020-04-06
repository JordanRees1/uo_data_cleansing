import numpy as np
import pandas as pd
import csv
import os
from time import process_time

### USER VARIABLES

# Used to specify what sensor variables to select
sensor_vars = ['PM10', 'PM1', 'CO', 'NO2']
# Timescale '...T' (mins) '...H' (hours)
delta = '12H'
# Used for hours to specify the start time e.g 12H and 7 = 12hr intervals: 7-7
start_time_hr = 7
# Location of folder containing week sub folders (absolute path needed) must contain backslashes '\'
path =r"C:\Users\witcombe\OneDrive - Newcastle University\3rd Year\Dissertation\Code\sensor_data\city_centre\december2019"

### END OF VARIABLES

# Adds a column for a variable value
def add_column(var):
    data_df[var] = data_df.loc[data_df['Variable']==var, ['Value']]

# This gets the mean of a variable based on delta (timescale 'xxT' (mins) 'H' (hours))
def get_mean_resample(var, delta):
        if delta == '12H':
                return tmp_df.resample(delta, on='Timestamp', base=start_time_hr) \
                [var].mean().reset_index(name='mean_'+var).bfill()
        else:
                return tmp_df.resample(delta, on='Timestamp') \
                [var].mean().reset_index(name='mean_'+var).bfill()

# This resamples the date time to the t minutes
def resample_timestamp(df, t):
        df['Timestamp 2'] = df['Timestamp'].dt.round(t)
        df['Timestamp'] = df['Timestamp 2']
        del df['Timestamp 2']

# Used to delete unneeded columns before outputting
def del_columns():
    del tmp_df['Flagged as Suspect Reading']
    del tmp_df['Units']
    del tmp_df['Variable']
    del tmp_df['Value']
    for var in sensor_vars:
        del tmp_df[var]

# Used for adding day/night for 12 hours delta
def half_day():
        # Gets timestamp and converts to string using temp column
        data_df['Timestamp2'] = data_df['Timestamp'].dt.strftime('%d-%m-%Y %H:%M:%S')
        if start_time_hr > 10:
                str_start = '0'+str(start_time_hr)
        else:
                str_start = str(start_time_hr)

        variable_filter = data_df['Timestamp2'].str.contains(str_start+':00:00')
        data_df.loc[variable_filter, 'Time'] = 'Day'

        str_end = str(start_time_hr+12)
        variable_filter = data_df['Timestamp2'].str.contains(str_end+':00:00')
        data_df.loc[variable_filter, 'Time'] = 'Night'
        # Remove temp column
        del data_df['Timestamp2']

# Start execution timer
start_time = process_time()

# This is getting the files from the sub directory, currently works for a month. Numbering each week 1 > 5 
filepaths = []
# Loop through directory to get files
for subdir, dirs, files in os.walk(path):
    for file in files:
        if (file == 'data.csv'):
                # Get the week number (used for key)
                week = subdir[len(subdir)-1]
                filepaths.append(subdir)

week_results = []
ticker = 0

# for each file in file path run the below code.
for data_file in filepaths:
        # updating the path variable
        sub_path = filepaths[ticker]
        # Weekno can be used as a key for which week of the month.
        weekno = filepaths[ticker][len(filepaths[ticker])-1]

        # Getting the sensor data
        data_df = pd.read_csv(sub_path+r'\data.csv', parse_dates=[3])

        # rounding time stamp to nearest minute
        resample_timestamp(data_df, '1min')

        # Store the week number
        data_df['Week'] = weekno

        # Get the day of week for axis when visualising
        data_df['Week Day'] = data_df['Timestamp'].dt.day_name()

        # Gets the date number of month e.g 31/12/2019 = '31'
        data_df['Date'] = data_df['Timestamp'].dt.day

        # Add a column with the unix time
        data_df['Unix'] = (data_df['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # Using integer division to get the 15 minute time slice.
        data_df['Slice'] = (data_df['Unix'] // (15 * 60))

        # Creating relative time to map these values onto the same graphs.
        data_df['Relative Time'] = data_df['Unix'] - data_df['Unix'].min()

        # Getting the sensor information
        sensor_df = pd.read_csv(sub_path+r"\sensors.csv")

        # Getting the sensor names into an array for iterating.
        file = open(sub_path+r"\sensors.csv", newline='')
        reader = csv.reader(file)
        header = next(reader)
        sensor_names = []
        for row in reader:
                sensor_names.append(str(row[0]))

        # Filtering the dataframe for Longitude & Latitude
        sensor_df = sensor_df.loc[:, ['Sensor Name', 'Sensor Centroid Longitude', 'Sensor Centroid Latitude']]

        # Renaming column names for convenience
        sensor_df = sensor_df.rename(columns={'Sensor Centroid Longitude': 'Longitude',
                                'Sensor Centroid Latitude': 'Latitude'})

        # Merging the two dataframes on the sensor name
        data_df = pd.merge(data_df, sensor_df, on=['Sensor Name'])

        # Hiding suspected false readings flagged 
        data_df = data_df.loc[~data_df['Flagged as Suspect Reading'], :]

        # Filter needed values using boolean operations
        data_df = data_df[(data_df['Variable'] == sensor_vars[0]) | (data_df['Variable'] == sensor_vars[1]) \
                | (data_df['Variable'] == sensor_vars[2]) | (data_df['Variable'] == sensor_vars[3]) ]

        # Used to store results for each sensor
        sensor_results = []

        # Looping through each sensor
        for sensor_name in sensor_names:
                var_results = []
                # Creating a copy each iteration of the loop to get the correct data 
                tmp_df = data_df[(data_df['Sensor Name'] == sensor_name)].copy()

                for var in sensor_vars:
                        # For each sensor set the column
                        tmp_df[var] = tmp_df.loc[data_df['Variable']==var, ['Value']]
                        # Get the mean value and resample time series for delta
                        var_results.append(get_mean_resample(var, delta)) 

                # Resample to closest 15 minute, used for merging
                resample_timestamp(tmp_df, '15min')

                # Merge these
                for r in range(1,len(sensor_vars)):
                        # var_results are appending on the end infintely
                        var_results[0] = var_results[0].merge(var_results[r], on='Timestamp')

                # Remove unwanted columns before outputting
                del_columns()

                # Merge sensor data with mean aggregate data
                new_df = tmp_df.merge(var_results[0], on='Timestamp').drop_duplicates()
                
                sensor_results.append(new_df)

        # Updating the dataframe
        week_results.append(pd.concat(sensor_results))

        ticker += 1

# Merging these into a data frame 
data_df = pd.concat(week_results)

# Dropping duplicates from merges
data_df = data_df.drop_duplicates(subset=['Sensor Name', 'Timestamp'], keep='first')

# Label day/night if working with 12 hour averages
if delta == '12H':
        half_day()

# Output to csv
data_df.to_csv(path+r"\output\output"+delta+".csv", index = False)

stop_time = process_time()

# Print execution time to compare against alteryx
print("Executed in: ", stop_time - start_time, "seconds")
print("Executed in: ", (stop_time - start_time)/60, "minutes")
