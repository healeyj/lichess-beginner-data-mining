import io
import zstandard
import re
import random
from collections import defaultdict
import os
import time
from datetime import datetime # 1. NEW: Import datetime for timestamp comparison

# CRITICAL (Implemented)
# get monthly average glicko score in addition to min, max, and number of games played
# get earliest and latest (beginning of the month, end of the month) glicko scores for each of the players
# maybe we can see improvement in just 1 month correlated to number of games played?
# IMPORTANT
# TODO: throw out players that had any game above the 1200 skill cap in january
# NICE TO HAVE
# TODO: run and save full 100k players, then from that output file sample 5k players (save time moving forward)
# TODO: save list of 5k players as a hashmap, so that it is efficient to locate them and rerun the analysis for february and beyond

# --- 1. CONFIGURATION ---
ZST_FILE_PATH = '/Users/healeyj/Desktop/lichess-extracts/lichess_db_standard_rated_2024-01.pgn.zst' # Jan24 datafile
OUTPUT_FILE_PATH = '/lichess-beginner-data-mining/target_players_stats_rapid_quick_pairing_2024_january_1.csv'
TARGET_TIMECONTROL = "600+0" #10m
ALTERNATE_TIMECONTROL_1 = "600+5" #10m+5s
ALTERNATE_TIMECONTROL_2 = "900+10" #15m+10s
MAX_RATING = 1000 # Glicko-2 rating, where 1500 is players' initially assigned rating
MIN_GAMES_JANUARY = 15 # january grinders to find initial sample
                        # but for rest of the year, must play at least 1 game every 2 months
                        # and at least 50 or 100 games total
TARGET_SAMPLE_SIZE = 5000

# --- 2. OPTIMIZED UTILITIES ---
TAG_RE = re.compile(r'^\[(\w+)\s+"(.+)"\]$')
EVENT_RE = re.compile(r'^\[Event\s+".+"\]$')

# Helper function to format elapsed time
def format_time(seconds):
    """Formats seconds into Hh Mmin S.Ssec format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes:02d}m")
    parts.append(f"{seconds:05.2f}s")
    
    return " ".join(parts).strip()

def parse_pgn_timestamp(date_str, time_str):
    """Parses PGN date and time strings into a single datetime object."""
    # Date format: YYYY.MM.DD (e.g., 2024.01.01)
    # Time format: HH:MM:SS (e.g., 10:30:15)
    try:
        dt_str = f"{date_str} {time_str}"
        return datetime.strptime(dt_str, "%Y.%m.%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


# --- 3. MAIN PROCESSING FUNCTION ---

def run_player_filter_and_sampling():
    """
    Scans the Lichess ZST file, filters games, aggregates player stats, 
    and generates a random sample of active low-RATING players.
    """
    if not os.path.exists(ZST_FILE_PATH):
        print(f"ERROR: File not found at '{ZST_FILE_PATH}'")
        print("Please check the file path and ensure the name is correct.")
        return

    print(f"Starting rapid scan of {ZST_FILE_PATH}...")
    
    # 2. NEW: Updated structure to track sum/count for average, and earliest/latest ratings/times
    player_stats = defaultdict(lambda: {
        'games': 0, 
        'min_rating': float('inf'), 
        'max_rating': float('-inf'),
        'rating_sum': 0,
        'rating_count': 0,
        'earliest_time': datetime(2099, 1, 1), # Initialize to a future date
        'latest_time': datetime(1900, 1, 1),   # Initialize to a past date
        'earliest_rating': 0,
        'latest_rating': 0,
    })
    
    current_game = {}
    game_count = 0
    line_number = 0
    last_processed_line = "" 
    
    start_time = time.time()

    try:
        with open(ZST_FILE_PATH, 'rb') as ifh:
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(ifh)
            text_stream = io.TextIOWrapper(stream, encoding='utf-8', errors='ignore')
            
            # --- MAIN PROCESSING LOOP ---
            for line_number, line in enumerate(text_stream, 1):
                line = line.strip()
                last_processed_line = line 

                if EVENT_RE.match(line):
                    
                    if current_game:
                        game_count += 1
                        
                        tc = current_game.get('TimeControl')
                        if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL_1, ALTERNATE_TIMECONTROL_2):
                            
                            white_rating = None
                            black_rating = None
                            
                            # 3. USE CORRECT LICHESS PGN TAGS: WhiteElo and BlackElo
                            white_rating_str = current_game.get('WhiteElo') 
                            black_rating_str = current_game.get('BlackElo')
                            
                            # 3. NEW: Extract Date/Time for earliest/latest tracking
                            date_str = current_game.get('UTCDate')
                            time_str = current_game.get('UTCTime')
                            game_dt = parse_pgn_timestamp(date_str, time_str)
                            
                            try:
                                if white_rating_str:
                                    white_rating = int(white_rating_str)
                                    if white_rating <= 0:
                                        white_rating = None 

                                if black_rating_str:
                                    black_rating = int(black_rating_str)
                                    if black_rating <= 0:
                                        black_rating = None
                                        
                            except ValueError:
                                pass
                            
                            
                            is_low_rating_game = (white_rating is not None and white_rating <= MAX_RATING) or \
                                                 (black_rating is not None and black_rating <= MAX_RATING)
                            
                            if is_low_rating_game and game_dt is not None: # Check if timestamp is valid
                                white_player = current_game.get('White')
                                black_player = current_game.get('Black')
                                
                                # Update stats for White Player
                                if white_player and white_rating is not None:
                                    stats = player_stats[white_player]
                                    stats['games'] += 1
                                    stats['min_rating'] = min(stats['min_rating'], white_rating)
                                    stats['max_rating'] = max(stats['max_rating'], white_rating)
                                    
                                    # 4. NEW: Update for Average
                                    stats['rating_sum'] += white_rating
                                    stats['rating_count'] += 1
                                    
                                    # 4. NEW: Update for Earliest/Latest
                                    if game_dt < stats['earliest_time']:
                                        stats['earliest_time'] = game_dt
                                        stats['earliest_rating'] = white_rating
                                    if game_dt > stats['latest_time']:
                                        stats['latest_time'] = game_dt
                                        stats['latest_rating'] = white_rating

                                # Update stats for Black Player
                                if black_player and black_rating is not None:
                                    stats = player_stats[black_player]
                                    stats['games'] += 1
                                    stats['min_rating'] = min(stats['min_rating'], black_rating)
                                    stats['max_rating'] = max(stats['max_rating'], black_rating)
                                    
                                    # 4. NEW: Update for Average
                                    stats['rating_sum'] += black_rating
                                    stats['rating_count'] += 1
                                    
                                    # 4. NEW: Update for Earliest/Latest
                                    if game_dt < stats['earliest_time']:
                                        stats['earliest_time'] = game_dt
                                        stats['earliest_rating'] = black_rating
                                    if game_dt > stats['latest_time']:
                                        stats['latest_time'] = game_dt
                                        stats['latest_rating'] = black_rating
                    
                    # Reset for the new game header
                    current_game = {}
                    
                    # Now extract the first tag ([Event])
                    match = TAG_RE.match(line)
                    if match:
                        tag_name, tag_value = match.groups()
                        current_game[tag_name] = tag_value

                # 2. Other header tags (only process if a game has started)
                elif line.startswith('['):
                    match = TAG_RE.match(line)
                    if match:
                        tag_name, tag_value = match.groups()
                        current_game[tag_name] = tag_value
                
                # Progress indicator
                if line_number % 5000000 == 0:
                    elapsed = time.time() - start_time
                    time_str = format_time(elapsed)
                    
                    print(f"Processed ~{game_count:,} games in {time_str} (Line: {line_number:,})...")

            # --- CRITICAL: PROCESS THE LAST GAME IN THE FILE ---
            if current_game:
                game_count += 1
                tc = current_game.get('TimeControl')
                if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL_1, ALTERNATE_TIMECONTROL_2):
                    
                    white_rating = None
                    black_rating = None
                    white_rating_str = current_game.get('WhiteElo') 
                    black_rating_str = current_game.get('BlackElo')
                    
                    date_str = current_game.get('UTCDate')
                    time_str = current_game.get('UTCTime')
                    game_dt = parse_pgn_timestamp(date_str, time_str)
                    
                    try:
                        if white_rating_str:
                            white_rating = int(white_rating_str)
                            if white_rating <= 0:
                                white_rating = None 

                        if black_rating_str:
                            black_rating = int(black_rating_str)
                            if black_rating <= 0:
                                black_rating = None
                                
                    except ValueError:
                        pass
                    
                    is_low_rating_game = (white_rating is not None and white_rating <= MAX_RATING) or \
                                         (black_rating is not None and black_rating <= MAX_RATING)
                    
                    if is_low_rating_game and game_dt is not None:
                        white_player = current_game.get('White')
                        black_player = current_game.get('Black')
                        
                        if white_player and white_rating is not None:
                            stats = player_stats[white_player]
                            stats['games'] += 1
                            stats['min_rating'] = min(stats['min_rating'], white_rating)
                            stats['max_rating'] = max(stats['max_rating'], white_rating)
                            stats['rating_sum'] += white_rating
                            stats['rating_count'] += 1
                            if game_dt < stats['earliest_time']:
                                stats['earliest_time'] = game_dt
                                stats['earliest_rating'] = white_rating
                            if game_dt > stats['latest_time']:
                                stats['latest_time'] = game_dt
                                stats['latest_rating'] = white_rating
                        
                        if black_player and black_rating is not None:
                            stats = player_stats[black_player]
                            stats['games'] += 1
                            stats['min_rating'] = min(stats['min_rating'], black_rating)
                            stats['max_rating'] = max(stats['max_rating'], black_rating)
                            stats['rating_sum'] += black_rating
                            stats['rating_count'] += 1
                            if game_dt < stats['earliest_time']:
                                stats['earliest_time'] = game_dt
                                stats['earliest_rating'] = black_rating
                            if game_dt > stats['latest_time']:
                                stats['latest_time'] = game_dt
                                stats['latest_rating'] = black_rating

    except Exception as e:
        # Error handling block remains the same
        print("\n" + "="*50)
        print("🚨 **FATAL ERROR DETECTED** 🚨")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {e}")
        print(f"Last successfully processed line (raw): '{last_processed_line}'")
        print(f"Line Number of Crash: {line_number:,}")
        print(f"Game Metadata at Crash: {current_game}")
        print("="*50)
        return

    # --- 4. AGGREGATION AND SAMPLING ---
    end_time = time.time()
    total_elapsed = end_time - start_time
    total_time_str = format_time(total_elapsed)

    print(f"\nScan complete. Processed a total of {game_count:,} games in {total_time_str}.")

    print("--- Aggregation and Sampling ---")
    
    # Filter for players who meet the minimum game requirement AND have valid RATING data
    grinders = {}
    for player, stats in player_stats.items():
        # Player is valid if they played enough games AND stats were successfully recorded
        if stats['games'] >= MIN_GAMES_JANUARY and stats['min_rating'] != float('inf'):
             grinders[player] = stats

    active_player_list = list(grinders.keys())
    
    print(f"Total unique players found: {len(player_stats):,}")
    print(f"Players who played >= {MIN_GAMES_JANUARY} games: {len(active_player_list):,}")

    if len(active_player_list) > TARGET_SAMPLE_SIZE:
        final_player_sample_keys = random.sample(active_player_list, TARGET_SAMPLE_SIZE)
    else:
        final_player_sample_keys = active_player_list
        print("Note: Fewer active players found than the target sample size. Using all of them.")

    # Prepare data for output file
    # 5. NEW: Updated Header with Average, Earliest, and Latest Ratings
    output_lines = [
        "Username,Total_Games_January,Min_RATING_January,Max_RATING_January,Average_RATING,Earliest_RATING,Latest_RATING"
    ]
    
    for username in final_player_sample_keys:
        stats = grinders[username]
        
        # Calculate Average Rating
        average_rating = round(stats['rating_sum'] / stats['rating_count']) if stats['rating_count'] > 0 else 0
        
        # Clean up ratings for output (guaranteed to be valid integers at this point)
        min_rating = int(stats['min_rating'])
        max_rating = int(stats['max_rating'])
        earliest_rating = stats['earliest_rating']
        latest_rating = stats['latest_rating']

        # 5. NEW: Format line with all new data points
        line = (f"{username},{stats['games']},{min_rating},{max_rating},{average_rating},"
                f"{earliest_rating},{latest_rating}")
        output_lines.append(line)


    try:
        with open(OUTPUT_FILE_PATH, 'w') as f:
            f.write('\n'.join(output_lines))
        
        print(f"\n✅ Successfully sampled {len(final_player_sample_keys)} players with full stats.")
        print(f"Data saved to: {OUTPUT_FILE_PATH}")
    except IOError as e:
        print(f"ERROR: Could not write output file: {e}")

if __name__ == "__main__":
    run_player_filter_and_sampling()