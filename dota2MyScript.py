import signal
import sys
from time import sleep
import pandas as pd
import requests
from requests.exceptions import RequestException
from datetime import datetime, timedelta
import random
import json

print("Starting Dota 2 match data collection script...")

# Constants
API_KEY = "your_steam_api_key"
BASE_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1"
HERO_IDS = list(range(1, 130))
SEQ_NUM_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1"

# Configuration
TOTAL_MATCHES = 100000  # Total matches to collect
BATCH_SIZE = 1000      # Matches per batch
PROGRESS_FILE = "dota2_progress.json"
MIN_WAIT_TIME = 1
MAX_WAIT_TIME = 120
MAX_RETRIES = 3

# Tracking variables
current_batch = 1
matches = []

# Add new URL for getting recent matches
HISTORY_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1"

# Add at top with other globals
total_matches_collected = 0

def signal_handler(signum, frame):
    print("\nGracefully shutting down...")
    save_current_state()
    sys.exit(0)

def save_current_state():
    """Save the current state to allow resuming later"""
    if 'matches' in globals() and matches:
        save_batch(matches, current_batch)
        print(f"\nSaved progress to batch {current_batch}")

def get_most_recent_match():
    """Get the most recent match ID to start collection from"""
    params = {
        'key': API_KEY,
        'matches_requested': 1,
        'min_players': 10
    }
    
    try:
        response = requests.get(HISTORY_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and data['result'].get('matches'):
            return data['result']['matches'][0]['match_id']
    except Exception as e:
        print(f"Error getting recent match: {e}")
    
    return None

def get_start_seq_num():
    """Get match sequence number from approximately a week ago"""
    # Calculate timestamp for 1 week ago
    week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
    
    params = {
        'key': API_KEY,
        'matches_requested': 1,
        'date_min': week_ago,
        'min_players': 10
    }
    
    try:
        response = requests.get(HISTORY_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and 'matches' in data['result'] and data['result']['matches']:
            match = data['result']['matches'][0]
            print(f"Found match from ~1 week ago:")
            print(f"Match ID: {match['match_id']}")
            print(f"Date: {datetime.fromtimestamp(match['start_time']).strftime('%Y-%m-%d %H:%M:%S')}")
            return match['match_seq_num']
    except Exception as e:
        print(f"Error getting start sequence number: {e}")
    return None

def fetch_matches(start_seq_num=None, wait_time=MIN_WAIT_TIME):
    """Fetch matches using sequence numbers for proper pagination"""
    params = {
        'key': API_KEY,
        'matches_requested': 100,
        'start_at_match_seq_num': start_seq_num
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(SEQ_NUM_URL, params=params)
            print(f"API Response Status: {response.status_code}")
            
            if response.status_code == 429:
                wait_time = min(wait_time * 2, MAX_WAIT_TIME)
                print(f"Rate limited. Waiting {wait_time} seconds...")
                sleep(wait_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if 'result' in data:
                matches = data['result'].get('matches', [])
                print(f"Matches found: {len(matches)}")
                
                if matches:
                    first_seq = matches[0]['match_seq_num']
                    last_seq = matches[-1]['match_seq_num']
                    print(f"First match seq: {first_seq}")
                    print(f"Last match seq: {last_seq}")
                    
                    # Next sequence number should be the last one plus one
                    next_seq = last_seq + 1
                    return matches, next_seq, MIN_WAIT_TIME
            
            return None, start_seq_num, wait_time
            
        except RequestException as e:
            print(f"Error fetching matches: {e}")
            wait_time = min(wait_time * 2, MAX_WAIT_TIME)
            sleep(wait_time)
    
    return None, start_seq_num, wait_time

def process_match(match):
    """Process and filter a single match"""
    try:
        # Only include matches with 10 players
        if len(match.get('players', [])) != 10:
            return None
            
        # Extract relevant match data
        processed = {
            'match_id': match['match_id'],
            'match_seq_num': match['match_seq_num'],
            'start_time': datetime.fromtimestamp(match['start_time']).isoformat(),
            'duration': match['duration'],
            'game_mode': match['game_mode'],
            'radiant_win': match['radiant_win'],
            'players': []
        }
        
        # Process player data
        for player in match['players']:
            processed['players'].append({
                'account_id': player.get('account_id', 'anonymous'),
                'hero_id': player['hero_id'],
                'player_slot': player['player_slot'],
                'kills': player['kills'],
                'deaths': player['deaths'],
                'assists': player['assists'],
                'gold_per_min': player['gold_per_min'],
                'xp_per_min': player['xp_per_min']
            })
            
        return processed
    except KeyError as e:
        print(f"Error processing match {match.get('match_id')}: {e}")
        return None

def save_batch(matches_data, batch_num):
    """Save a batch of matches to CSV"""
    try:
        df = pd.DataFrame(matches_data)
        filename = f"dota2_matches_batch_{batch_num}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved batch {batch_num} with {len(matches_data)} matches to {filename}")
    except Exception as e:
        print(f"Error saving batch {batch_num}: {e}")

def save_progress(seq_num, matches_collected):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            'last_sequence': seq_num,
            'matches_collected': matches_collected
        }, f)

def load_progress():
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def get_latest_match_seq():
    """Get the most recent match sequence number"""
    try:
        response = requests.get(HISTORY_URL, params={
            'key': API_KEY,
            'matches_requested': 1,
            'game_mode': 1,
            'skill': 3
        })
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and 'matches' in data['result'] and data['result']['matches']:
            match = data['result']['matches'][0]
            print(f"Found recent match ID: {match['match_id']}")
            return match['match_id']
    except Exception as e:
        print(f"Error getting latest match: {e}")
    return None

def print_progress(total_matches, current_seq, batch_num):
    """Print detailed progress information"""
    print("\n" + "="*50)
    print(f"Progress Report:")
    print(f"Total matches collected: {total_matches}/{TOTAL_MATCHES}")
    print(f"Current batch: {batch_num}")
    print(f"Current sequence number: {current_seq}")
    print(f"Completion: {(total_matches/TOTAL_MATCHES)*100:.2f}%")
    print("="*50 + "\n")

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    global matches, current_batch, total_matches_collected
    start_seq_num = get_start_seq_num()
    wait_time = MIN_WAIT_TIME
    
    try:
        while total_matches_collected < TOTAL_MATCHES:
            print(f"\nIteration {current_batch}")
            print(f"Total matches collected so far: {total_matches_collected}/{TOTAL_MATCHES}")
            print(f"Current sequence number: {start_seq_num}")
            
            result, next_seq_num, new_wait_time = fetch_matches(start_seq_num, wait_time)
            
            if result:
                valid_matches = [m for m in result if process_match(m)]
                matches.extend(valid_matches)
                total_matches_collected += len(valid_matches)
                
                if len(matches) >= BATCH_SIZE:
                    save_batch(matches[:BATCH_SIZE], current_batch)
                    matches = matches[BATCH_SIZE:]
                    current_batch += 1
                
                start_seq_num = next_seq_num
                wait_time = new_wait_time
                
                print_progress(total_matches_collected, start_seq_num, current_batch)
            
            sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
        save_current_state()
    except Exception as e:
        print(f"Error in main loop: {e}")
        save_current_state()
        raise

if __name__ == "__main__":
    print("Starting Dota 2 match data collection script...")
    main()
