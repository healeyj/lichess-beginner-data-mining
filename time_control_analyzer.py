import gzip
import re
from collections import Counter
import os
import sys

# NOTE: The 'zstandard' library is required for .zst files. 
# It must be installed separately (pip install zstandard) in your local environment.
try:
    import zstandard as zstd
except ImportError:
    zstd = None # Handle case where library is not installed

def analyze_pgn_dataset(input_path: str, output_path: str):
    """
    Reads a Lichess PGN dataset (handles .pgn, .pgn.gz, or .pgn.zst), counts total games,
    and analyzes the frequency of each TimeControl tag.

    Args:
        input_path: Path to the input PGN file (e.g., 'lichess_db_standard_2023-01.pgn.zst').
        output_path: Path to the output text file where results will be written.
    """
    print(f"Starting analysis of: {input_path}")
    
    # Game counter
    total_games = 0
    # Time control counter
    time_controls = Counter()

    # Regex to find the TimeControl tag and capture its value
    # Example match: [TimeControl "900+15"]
    time_control_pattern = re.compile(r'^\[TimeControl\s+"([^"]+)"\]')

    # Determine how to open the file
    opener = None
    input_path_lower = input_path.lower()
    
    if input_path_lower.endswith('.pgn.gz') or input_path_lower.endswith('.gz'):
        print("Detected GZIP compression (.gz)")
        opener = gzip.open
    elif input_path_lower.endswith('.pgn.zst') or input_path_lower.endswith('.zst'):
        if zstd is None:
            print("\nError: Detected ZSTANDARD file (.zst), but the 'zstandard' library is not imported.")
            print("Please ensure it is installed: 'pip install zstandard'")
            return
        print("Detected ZSTANDARD compression (.zst)")
        # zstandard.open() provides a clean file-like object and context manager
        opener = zstd.open 
    else:
        print("Detected uncompressed PGN file (.pgn)")
        opener = open
        
    try:
        # Process the file line by line to handle large datasets efficiently
        # Use the determined opener for robust file handling
        with opener(input_path, 'rt', encoding='utf-8') as f:
            for line in f:
                match = time_control_pattern.match(line)
                
                if match:
                    # Found the TimeControl tag, increment total games and count the control
                    control_value = match.group(1)
                    time_controls[control_value] += 1
                    total_games += 1
                    
    except FileNotFoundError:
        print(f"\nError: Input file not found at '{input_path}'. Please check the path and filename.")
        return
    except Exception as e:
        # Catch ZstdError specifically if zstd is used and fails to decompress
        print(f"\nAn unexpected error occurred during file reading: {e}")
        return

    # --- Prepare Output Content ---
    output_content = []
    
    # 1. Total game count
    output_content.append(f"1) Total Games Played: {total_games}\n")
    
    # 2. List of distinct time controls and their proportions
    output_content.append("2) Distinct Time Controls and Proportions:\n")
    
    if total_games == 0:
        output_content.append("No TimeControl tags were found in the dataset.")
    else:
        # Sort by count (descending) for readability
        sorted_controls = sorted(time_controls.items(), key=lambda item: item[1], reverse=True)
        
        for control, count in sorted_controls:
            # Calculate proportion and format to two decimal places
            proportion = (count / total_games) * 100
            output_content.append(f"  - {control}: {count} games ({proportion:.2f}%)")

    # --- Write Results to Output File ---
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_content))
        print(f"\nAnalysis complete! Results written successfully to: {output_path}")
        print("-" * 40)
        print(f"Total Games Found: {total_games}")
        
    except Exception as e:
        print(f"\nAn error occurred during file writing: {e}")

# --- Main Execution Block ---
if __name__ == '__main__':
    # IMPORTANT: Change this to the actual path of your Lichess PGN file
    # Updated example path to reflect the .zst extension.
    INPUT_PGN_PATH = 'lichess_db_standard_rated_2024-01.pgn.zst' 
    OUTPUT_TXT_PATH = 'time_control_analysis.txt'

    # Check if the example input file exists (only for demonstration robustness)
    if not os.path.exists(INPUT_PGN_PATH):
        print("=" * 60)
        print(f"ALERT: The placeholder file '{INPUT_PGN_PATH}' was not found.")
        print("Please replace the INPUT_PGN_PATH variable with the actual path")
        print("to your downloaded Lichess PGN dataset file (.pgn, .pgn.gz, or .pgn.zst).")
        print("=" * 60)
        
        # Create a minimal uncompressed PGN mock file for a robust example that always runs.
        MOCK_FILE = "mock_lichess_data.pgn"
        if not os.path.exists(MOCK_FILE):
             print(f"Creating mock data file: {MOCK_FILE} for demonstration...")
             # Mock content with minimal headers for time control extraction
             mock_content = """
[Event "Rated Blitz game"]
[Site "https://lichess.org/ABCDE123"]
[TimeControl "300+0"]
[Result "1-0"]
*

[Event "Rated Classical game"]
[Site "https://lichess.org/FGHIJ456"]
[TimeControl "1800+30"]
[Result "1/2-1/2"]
*

[Event "Rated Blitz game"]
[Site "https://lichess.org/KLMNO789"]
[TimeControl "300+0"]
[Result "0-1"]
*

[Event "Rated Rapid game"]
[Site "https://lichess.org/LMN456"]
[TimeControl "600+0"]
[Result "1-0"]
*

[Event "Rated Rapid game"]
[Site "https://lichess.org/OPQ789"]
[TimeControl "600+0"]
[Result "0-1"]
*
"""
             with open(MOCK_FILE, 'w') as f:
                 f.write(mock_content)
             
             analyze_pgn_dataset(MOCK_FILE, OUTPUT_TXT_PATH)
        else:
            analyze_pgn_dataset(MOCK_FILE, OUTPUT_TXT_PATH)

    else:
        # Run the analysis with the user-specified path
        analyze_pgn_dataset(INPUT_PGN_PATH, OUTPUT_TXT_PATH)
