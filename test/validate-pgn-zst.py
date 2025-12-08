import zstandard
import io
import os

# --- CONFIG ---
# This path must point to your .pgn.zst file
ZST_FILE_PATH = '/Users/healeyj/Desktop/lichess-extracts/lichess_db_standard_rated_2024-01_rapid_subset.pgn.zst'
TARGET_GAMES_TO_PRINT = 3

# --- MAIN FUNCTION ---

def read_and_print_first_games():
    """
    Reads the ZST file, decompresses it, finds the PGN content, 
    and prints the first TARGET_GAMES_TO_PRINT games to the console.
    """
    if not os.path.exists(ZST_FILE_PATH):
        print(f"ERROR: File not found at '{ZST_FILE_PATH}'")
        return

    print(f"Reading and decompressing: {ZST_FILE_PATH}...")
    
    current_game_lines = []
    games_printed = 0
    
    try:
        with open(ZST_FILE_PATH, 'rb') as ifh:
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(ifh)
            # Wrap the decompressed stream for reading lines of text
            text_stream = io.TextIOWrapper(stream, encoding='utf-8', errors='ignore')
            
            # --- MAIN PROCESSING LOOP ---
            for line in text_stream:
                line = line.strip()
                
                # Check if we've reached the target number of games
                if games_printed >= TARGET_GAMES_TO_PRINT:
                    break
                
                # PGN games are separated by an empty line following the move text/result line.
                # However, the best way to detect a *new* game is by looking for the mandatory [Event "..."] tag.
                if line.startswith('[Event '):
                    # If current_game_lines is not empty, it means we've finished the previous game.
                    if current_game_lines:
                        # Print the completed game
                        print("\n" + "="*20 + f" GAME {games_printed + 1} " + "="*20)
                        print('\n'.join(current_game_lines))
                        games_printed += 1
                        
                        # Stop if we hit the limit
                        if games_printed >= TARGET_GAMES_TO_PRINT:
                            break
                            
                    # Start a new game
                    current_game_lines = [line]
                
                # Add all other non-empty lines to the current game buffer
                elif line:
                    current_game_lines.append(line)

            # If the loop finished by breaking due to the game limit
            if games_printed == TARGET_GAMES_TO_PRINT:
                print("\n" + "="*50)
                print(f"Successfully printed the first {TARGET_GAMES_TO_PRINT} games.")
                print("="*50)
            
            # If the file ended before reaching the target
            elif games_printed > 0:
                 # Print the final game if the loop broke or the file ended after the last full game was processed
                 print("\n" + "="*50)
                 print(f"File end reached. Printed {games_printed} total games.")
                 print("="*50)

    except Exception as e:
        print(f"\n🚨 **ERROR** while processing the file: {e}")

if __name__ == "__main__":
    read_and_print_first_games()