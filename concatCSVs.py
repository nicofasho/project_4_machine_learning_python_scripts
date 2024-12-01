import os
import pandas as pd

# Directory where your .csv files are stored
directory = './'

# List to store data frames
df_list = []

for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        # Read each .csv file
        df = pd.read_csv(os.path.join(directory, filename))
        df_list.append(df)

# Concatenate all data frames
concatenated_df = pd.concat(df_list, ignore_index=True)

# Drop duplicate header rows if they exist
concatenated_df = concatenated_df.loc[~(concatenated_df.iloc[:, 0] == concatenated_df.columns[0])]

# Save concatenated DataFrame to a new .csv file
concatenated_df.to_csv('dota2_all_matches.csv', index=False)
