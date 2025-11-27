import io
import zstandard
import re
import random
from collections import defaultdict
import os
import time

# TODO: throw out players that had any game above the 1200 skill cap in january
# TODO: run and save full 100k players, then from that output file sample 5k players (save time moving forward)
# TODO: commit to github

'''
progress report presentation outline:
- is there a correlation between number of chess games played and skill rating improvement?
    - do you need to study to git good, or is grinding games enough, at least up to a certain skill level?
    - need a lot of data to prove this: hundreds of players, hundreds of games over time
        - inspired by a research paper that showed game length did not correlate with improvement
    - i'm a newbie at chess so these concepts/communities are new to me
    - i had more ambitious, technical questions but narrowed focus due to limitations
- explored various datasets:
    - lumbrasgigabase (big scrapes, but proprietary format designed for game analysis)
    - kaggle lichess.org extract dataset (too small)
    - chess.com public api (request limits)
    - lichess.org public monthly extracts (each 30gb file contains ~100million games)
- vibecode with gemini
    - read game headers/metadata only: player names, player ratings, number of games played
    - limit scope to January 2024, 15min rapid games, <1200 glicko rating (beginner), new year's grinders (>10 games that month)
    - i found 1400 players that meet criteria
    - it took 20 minutes to extract this from the January dataset
    - next datasets should go a lot faster now that i have a playerlist
- up next
    - track these players through 2024 monthly datasets to observe ratings over time
    - attrition will reduce sample size, may need to adjust filters to compensate
    - will the glicko-2 rating system interfere with my analysis?
    - adjust research question/strategy as necessary
'''

# --- 1. CONFIGURATION ---
ZST_FILE_PATH = 'lichess_db_standard_rated_2024-01.pgn.zst'
OUTPUT_FILE_PATH = 'target_players_stats_rapid_quick_pairing_2024_january.csv'
TARGET_TIMECONTROL = "600+0"
ALTERNATE_TIMECONTROL_1 = "600+5"
ALTERNATE_TIMECONTROL_2 = "900+10"
MAX_RATING = 1000 # Glicko-2 rating
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
    if minutes > 0 or hours > 0: # Show minutes if > 0 or if hours is present
        parts.append(f"{minutes:02d}m")
    parts.append(f"{seconds:05.2f}s")
    
    return " ".join(parts).strip()


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
    
    # Updated structure to track games, min, and max RATING
    player_stats = defaultdict(lambda: {
        'games': 0, 
        'min_rating': float('inf'), 
        'max_rating': float('-inf')
    })
    
    current_game = {}
    game_count = 0
    line_number = 0
    last_processed_line = "" 
    
    start_time = time.time() # <-- Start time recorded

    try:
        with open(ZST_FILE_PATH, 'rb') as ifh:
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(ifh)
            text_stream = io.TextIOWrapper(stream, encoding='utf-8', errors='ignore')
            
            # --- MAIN PROCESSING LOOP ---
            for line_number, line in enumerate(text_stream, 1):
                line = line.strip()
                last_processed_line = line 

                # 1. Start of a new game? (i.e., the [Event] tag)
                if EVENT_RE.match(line):
                    
                    # --- CRITICAL: PROCESS PREVIOUS GAME BEFORE RESETTING ---
                    if current_game:
                        game_count += 1
                        
                        # Apply Filters
                        tc = current_game.get('TimeControl')
                        if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL_1, ALTERNATE_TIMECONTROL_2):
                            try:
                                white_rating = int(current_game.get('WhiteRating', '0'))
                                black_rating = int(current_game.get('BlackRating', '0'))
                            except ValueError:
                                white_rating, black_rating = 0, 0 

                            is_low_rating_game = (white_rating <= MAX_RATING or black_rating <= MAX_RATING)
                            
                            if is_low_rating_game:
                                white_player = current_game.get('White')
                                black_player = current_game.get('Black')
                                
                                # Update stats for White Player
                                if white_player:
                                    stats = player_stats[white_player]
                                    stats['games'] += 1
                                    # Track min/max RATING for this player across all their games
                                    stats['min_rating'] = min(stats['min_rating'], white_rating)
                                    stats['max_rating'] = max(stats['max_rating'], white_rating)

                                # Update stats for Black Player
                                if black_player:
                                    stats = player_stats[black_player]
                                    stats['games'] += 1
                                    # Track min/max RATING for this player across all their games
                                    stats['min_rating'] = min(stats['min_rating'], black_rating)
                                    stats['max_rating'] = max(stats['max_rating'], black_rating)
                    
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
                
                # Ignore everything else (blank lines and move text)

                # Progress indicator
                if line_number % 5000000 == 0:
                    elapsed = time.time() - start_time # <-- Calculate elapsed time
                    time_str = format_time(elapsed)    # <-- Format time
                    
                    print(f"Processed ~{game_count:,} games in {time_str} (Line: {line_number:,})...")

            # --- CRITICAL: PROCESS THE LAST GAME IN THE FILE ---
            if current_game:
                game_count += 1
                tc = current_game.get('TimeControl')
                if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL_1, ALTERNATE_TIMECONTROL_2):
                    try:
                        white_rating = int(current_game.get('WhiteRating', '0'))
                        black_rating = int(current_game.get('BlackRating', '0'))
                    except ValueError:
                        white_rating, black_rating = 0, 0 

                    is_low_rating_game = (white_rating <= MAX_RATING or black_rating <= MAX_RATING) # fixme: should check target player RATING, not RATING of both players
                                                                                     # this is causing some people >MAX_RATING to be included
                                                                                     # simply because they played a match against someone with <MAX_RATING
                    
                    if is_low_rating_game:
                        white_player = current_game.get('White')
                        black_player = current_game.get('Black')
                        
                        if white_player:
                            stats = player_stats[white_player]
                            stats['games'] += 1
                            stats['min_rating'] = min(stats['min_rating'], white_rating)
                            stats['max_rating'] = max(stats['max_rating'], white_rating)
                        
                        if black_player:
                            stats = player_stats[black_player]
                            stats['games'] += 1
                            stats['min_rating'] = min(stats['min_rating'], black_rating)
                            stats['max_rating'] = max(stats['max_rating'], black_rating)

    except Exception as e:
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
    output_lines = [
        "Username,Total_Games_January,Min_RATING_January,Max_RATING_January" # Header row
    ]
    
    for username in final_player_sample_keys:
        stats = grinders[username]
        # Clean up any leftover infinities just in case, though the filter should prevent it
        min_rating = int(stats['min_rating']) if stats['min_rating'] != float('inf') else 0
        max_rating = int(stats['max_rating']) if stats['max_rating'] != float('-inf') else 0

        line = f"{username},{stats['games']},{min_rating},{max_rating}"
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