import io
import zstandard
import re
import random
from collections import defaultdict
import os

# --- 1. CONFIGURATION (Same as before) ---
ZST_FILE_PATH = 'lichess_db_standard_rated_2024-01.pgn.zst'
OUTPUT_FILE_PATH = 'target_players.txt'
TARGET_TIMECONTROL = "900+0"
ALTERNATE_TIMECONTROL = "900" 
MAX_ELO = 1200
MIN_GAMES_JANUARY = 10
TARGET_SAMPLE_SIZE = 1000

# --- 2. OPTIMIZED UTILITIES (Same as before) ---
TAG_RE = re.compile(r'^\[(\w+)\s+"(.+)"\]$')
EVENT_RE = re.compile(r'^\[Event\s+".+"\]$')
#END_HEADER_RE = re.compile(r'^\s*$') 


# --- 3. MAIN PROCESSING FUNCTION (Revised with Debugging) ---

def run_player_filter_and_sampling():
    """
    Scans the Lichess ZST file, filters games, aggregates player stats, 
    and generates a random sample of active low-ELO players.
    Includes robust error logging to find the crash cause.
    """
    if not os.path.exists(ZST_FILE_PATH):
        print(f"ERROR: File not found at '{ZST_FILE_PATH}'")
        print("Please check the file path and ensure the name is exactly 'lichess_db_rapid_2024-01.pgn.zst'.")
        return

    print(f"Starting rapid scan of {ZST_FILE_PATH} with debugging enabled...")
    
    player_stats = defaultdict(lambda: {'games': 0, 'total_elo': 0})
    current_game = {}
    game_count = 0
    line_number = 0
    last_processed_line = "" # New variable to store the last line read

    try:
        with open(ZST_FILE_PATH, 'rb') as ifh:
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(ifh)
            text_stream = io.TextIOWrapper(stream, encoding='utf-8', errors='ignore')
            
            current_game = {}
            game_count = 0
            
            # --- MAIN PROCESSING LOOP ---
            for line_number, line in enumerate(text_stream, 1):
                line = line.strip()
                last_processed_line = line 

                # 1. Start of a new game? (i.e., the [Event] tag)
                if EVENT_RE.match(line):
                    # We have found a *new* game. Process the *previous* game if it exists.
                    
                    # --- CRITICAL: PROCESS PREVIOUS GAME BEFORE RESETTING ---
                    if current_game:
                        game_count += 1
                        
                        # Apply Filters (Same logic as before, now correctly executed)
                        tc = current_game.get('TimeControl')
                        if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL):
                            try:
                                white_elo = int(current_game.get('WhiteElo', '0'))
                                black_elo = int(current_game.get('BlackElo', '0'))
                            except ValueError:
                                # Should be caught by isdigit checks, but as a fallback
                                white_elo, black_elo = 0, 0 

                            is_low_elo_game = (white_elo <= MAX_ELO or black_elo <= MAX_ELO)
                            
                            if is_low_elo_game:
                                white_player = current_game.get('White')
                                black_player = current_game.get('Black')
                                
                                if white_player:
                                    player_stats[white_player]['games'] += 1
                                if black_player:
                                    player_stats[black_player]['games'] += 1
                    
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
                    print(f"Processed ~{game_count:,} games (Line: {line_number:,})...")

            # --- CRITICAL: PROCESS THE LAST GAME IN THE FILE ---
            # When the loop ends, the last game header is in current_game.
            # We need to process it one last time.
            if current_game:
                # Re-run the filter logic here exactly as above
                game_count += 1
                tc = current_game.get('TimeControl')
                if tc in (TARGET_TIMECONTROL, ALTERNATE_TIMECONTROL):
                    try:
                        white_elo = int(current_game.get('WhiteElo', '0'))
                        black_elo = int(current_game.get('BlackElo', '0'))
                    except ValueError:
                        white_elo, black_elo = 0, 0 

                    is_low_elo_game = (white_elo <= MAX_ELO or black_elo <= MAX_ELO)
                    
                    if is_low_elo_game:
                        white_player = current_game.get('White')
                        black_player = current_game.get('Black')
                        
                        if white_player:
                            player_stats[white_player]['games'] += 1
                        if black_player:
                            player_stats[black_player]['games'] += 1

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
    print(f"\nScan complete. Processed a total of {game_count:,} games.")

    print("--- Aggregation and Sampling ---")
    grinders = {
        player: stats for player, stats in player_stats.items()
        if stats['games'] >= MIN_GAMES_JANUARY
    }
    active_player_list = list(grinders.keys())
    
    print(f"Total unique players found: {len(player_stats):,}")
    print(f"Players who played >= {MIN_GAMES_JANUARY} games: {len(active_player_list):,}")

    if len(active_player_list) > TARGET_SAMPLE_SIZE:
        final_player_sample = random.sample(active_player_list, TARGET_SAMPLE_SIZE)
    else:
        final_player_sample = active_player_list
        print("Note: Fewer active players found than the target sample size. Using all of them.")

    try:
        with open(OUTPUT_FILE_PATH, 'w') as f:
            f.write('\n'.join(final_player_sample))
        
        print(f"\n✅ Successfully sampled {len(final_player_sample)} players.")
        print(f"List saved to: {OUTPUT_FILE_PATH}")
    except IOError as e:
        print(f"ERROR: Could not write output file: {e}")

if __name__ == "__main__":
    run_player_filter_and_sampling()