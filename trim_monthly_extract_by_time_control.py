import io
import zstandard
import re
import os
import time

# --- CONFIG ---
# Input File: The full Lichess monthly PGN dump.
ZST_FILE_PATH = '/Users/healeyj/Desktop/lichess-extracts/lichess_db_standard_rated_2024-01.pgn.zst'
# Output File: The filtered file containing ONLY the specified time controls.
OUTPUT_FILE_PATH = '/Users/healeyj/Desktop/lichess-extracts/lichess_db_standard_rated_2024-01_rapid_subset.pgn.zst'

# The specific TimeControl tag values to keep.
TARGET_TIMECONTROLS = {"900+10", "600+5", "600+0"} # 15m+10s, 10m+5s, and 10m

# --- REGEX PGN PARSING HELPERS ---
TAG_RE = re.compile(r'^\[(\w+)\s+"(.+)"\]$')
EVENT_RE = re.compile(r'^\[Event\s+".+"\]$')
# Regex to detect the start of a move section (e.g., "1. e4 e5 ...")
MOVES_START_RE = re.compile(r'^\d+\.')

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


def filter_games_by_time_control():
    """
    Reads a ZST PGN file, filters for a set of specific time controls, 
    and writes the matching games to a new ZST PGN file.
    """
    if not os.path.exists(ZST_FILE_PATH):
        print(f"ERROR: File not found at '{ZST_FILE_PATH}'")
        return

    print(f"Starting filter scan of {ZST_FILE_PATH}...")
    print(f"Target Time Controls: **{', '.join(TARGET_TIMECONTROLS)}**")
    
    game_count_total = 0
    game_count_filtered = 0
    
    # Store the lines of the current game being read
    current_game_lines = []
    # Store metadata tags for the current game
    current_game_metadata = {} 
    
    # Flag to indicate if the current game meets the time control filter
    keep_current_game = False 
    
    start_time = time.time()

    # Setup Zstandard decompressor for input and compressor for output
    dctx = zstandard.ZstdDecompressor()
    cctx = zstandard.ZstdCompressor(level=3) # Use level 3 for good compression speed/ratio balance

    try:
        # Open the input file for reading binary
        with open(ZST_FILE_PATH, 'rb') as ifh:
            # Open the output file for writing binary
            with open(OUTPUT_FILE_PATH, 'wb') as ofh:
                
                # Create a ZstdDecompressor stream reader for the input file
                input_stream = dctx.stream_reader(ifh)
                text_input_stream = io.TextIOWrapper(input_stream, encoding='utf-8', errors='ignore')
                
                # Create a ZstdCompressor stream writer for the output file
                output_stream = cctx.stream_writer(ofh)
                text_output_stream = io.TextIOWrapper(output_stream, encoding='utf-8', write_through=True)

                # --- MAIN PROCESSING LOOP ---
                for line_number, line in enumerate(text_input_stream, 1):
                    line_stripped = line.strip()
                    current_game_lines.append(line)
                    
                    # 1. Start of a new game ([Event...]) or end of previous game
                    if EVENT_RE.match(line_stripped):
                        
                        # Process the previous game block first (if one exists)
                        if game_count_total > 0:
                            # If the previous game matched the filter, write it out
                            if keep_current_game:
                                # Write all lines from the previous game except the [Event] line we just read
                                text_output_stream.write(''.join(current_game_lines[:-1]))
                                game_count_filtered += 1
                        
                        # Reset for the new game header
                        current_game_lines = [line] # Keep the current line (the [Event] tag)
                        current_game_metadata = {}
                        keep_current_game = False
                        game_count_total += 1

                        # Extract the first tag ([Event])
                        match = TAG_RE.match(line_stripped)
                        if match:
                            tag_name, tag_value = match.groups()
                            current_game_metadata[tag_name] = tag_value

                    # 2. Other header tags
                    elif line_stripped.startswith('['):
                        match = TAG_RE.match(line_stripped)
                        if match:
                            tag_name, tag_value = match.groups()
                            current_game_metadata[tag_name] = tag_value
                            
                            # CRITICAL CHECK: Check TimeControl tag as soon as it's found
                            # The only change is checking if tag_value is IN the set of target controls
                            if tag_name == 'TimeControl' and tag_value in TARGET_TIMECONTROLS:
                                keep_current_game = True

                    # 3. Game moves section (begins with "1. ")
                    elif MOVES_START_RE.match(line_stripped):
                        # This line is the moves/result line and is typically followed by a blank line.
                        
                        # If the game passed the filter, write the entire block
                        if keep_current_game:
                            # Write the game header lines that were collected, the move line, and the trailing blank line
                            text_output_stream.write(''.join(current_game_lines))
                            game_count_filtered += 1
                        
                        # Reset for the next game block
                        current_game_lines = []
                        current_game_metadata = {}
                        keep_current_game = False
                    
                    # 4. Progress indicator
                    if line_number % 5000000 == 0:
                        elapsed = time.time() - start_time
                        time_str = format_time(elapsed)
                        print(f"Processed ~{game_count_total:,} games in {time_str} (Line: {line_number:,})...")

                # Process the very last game if the file ended mid-block
                if keep_current_game and current_game_lines:
                    text_output_stream.write(''.join(current_game_lines))
                    game_count_filtered += 1
                
                # Close the output streams
                text_output_stream.close()
                output_stream.close()

    except Exception as e:
        print("\n" + "="*50)
        print("🚨 **FATAL ERROR DETECTED** 🚨")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {e}")
        print("="*50)
        return

    # --- RESULTS ---
    end_time = time.time()
    total_elapsed = end_time - start_time
    total_time_str = format_time(total_elapsed)

    print(f"\nScan complete in {total_time_str}.")
    print(f"Total games processed: {game_count_total:,}")
    print(f"Games saved (Time Controls: {', '.join(TARGET_TIMECONTROLS)}): {game_count_filtered:,}")
    print(f"\n✅ Filtered file saved to: **{OUTPUT_FILE_PATH}**")


if __name__ == "__main__":
    filter_games_by_time_control()