import pandas as pd
import requests
import ast

print("Starting data processing...")

# Load CSV file
print("Loading CSV file...")
df = pd.read_csv('./dota2_all_matches.csv', low_memory=False)
print(f"Loaded {len(df)} matches")

# Drop unnecessary columns
print("Dropping unnecessary columns...")
df = df.drop(
    columns=[
        "players",
        "pre_game_duration",
        "start_time",
        "match_id",
        "match_seq_num",
        "cluster",
        "lobby_type",
        "human_players",
        "leagueid",
        "game_mode",
        "flags",
        "engine",
        "radiant_captain",
        "dire_captain",
        "radiant_team_id",
        "radiant_name",
        "radiant_logo",
        "radiant_team_complete",
        "dire_team_id",
        "dire_name",
        "dire_logo",
        "dire_team_complete",
    ]
)

# Fetch heroes data from the API
print("Fetching heroes data from API...")
response = requests.get('https://api.opendota.com/api/heroes')
heroes_data = response.json()
hero_id_to_name = {hero['id']: hero['localized_name'] for hero in heroes_data}

def process_picks_bans(picks_bans):
    # Pre-define all possible columns to ensure consistent order
    all_heroes = sorted(hero_id_to_name.values())
    result = {}
    
    # Initialize all columns as 0
    for team in ['radiant', 'dire']:
        for hero_name in all_heroes:
            column_name = f"{team}_{hero_name.replace(' ', '_')}"
            result[column_name] = 0
    
    if pd.isna(picks_bans):
        return result
    
    try:
        if isinstance(picks_bans, str):
            picks_bans = ast.literal_eval(picks_bans)
            
        for item in picks_bans:
            if item['is_pick'] == 1:  # Only process picks
                hero_id = item['hero_id']
                if hero_id in hero_id_to_name:
                    hero_name = hero_id_to_name[hero_id]
                    team_prefix = 'radiant_' if item['team'] == 0 else 'dire_'
                    column_name = f"{team_prefix}{hero_name.replace(' ', '_')}"
                    result[column_name] = 1
    except Exception as e:
        print(f"Error processing picks/bans: {str(e)}")
    return result

# Remove columns with titles that are just numbers and contain no data
for column in df.columns:
    if column.isdigit() and df[column].isnull().all():
        df.drop(columns=[column], inplace=True)

# Process the picks_bans column
print("Processing picks and bans...")
new_columns = df['picks_bans'].apply(process_picks_bans)
new_df = pd.DataFrame(new_columns.tolist())

# Ensure consistent column order
all_heroes = sorted(hero_id_to_name.values())
expected_columns = [f"{team}_{hero_name.replace(' ', '_')}" 
                   for team in ['radiant', 'dire']
                   for hero_name in all_heroes]

# Initialize missing columns with 0
for col in expected_columns:
    if col not in new_df.columns:
        new_df[col] = 0
    new_df[col] = new_df[col].astype(int)

# Reorder columns to ensure consistency
new_df = new_df[expected_columns]

# Join with original dataframe
df = pd.concat([df.drop(columns=['picks_bans']), new_df], axis=1)

# Verify column count
print(f"Total columns in final dataset: {len(df.columns)}")

# Save to CSV
print("Saving processed data...")
df.to_csv('dota2_matches_processed.csv', index=False, na_rep='?')
print("Data saved successfully")
