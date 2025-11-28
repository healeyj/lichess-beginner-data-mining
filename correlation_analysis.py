#/chessvenv/bin/python -m pip install scipy #ect.

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import os
import io

# --- Configuration ---
INPUT_CSV_PATH = 'lichess-beginner-data-mining/target_players_stats_rapid_quick_pairing_2024_january_1.csv'
OUTPUT_CSV_PATH = 'lichess-beginner-data-mining/correlation_results.csv'
OUTPUT_PLOT_PATH = 'lichess-beginner-data-mining/games_vs_rating_change.png'

# Define bins for the grouped analysis (Suggestion 3)
# These bins should start at MIN_GAMES_JANUARY (e.g., 15)
GAME_BINS = [15, 30, 60, 100, 200, np.inf]
BIN_LABELS = ['15-30 Games', '31-60 Games', '61-100 Games', '101-200 Games', '201+ Games']

# --- Helper Functions ---

def create_mock_data(filepath):
    """Creates a mock CSV file for demonstration if the actual file is missing."""
    print(f"ALERT: Input file '{filepath}' not found. Creating mock data for analysis.")
    
    # Generate 500 rows of synthetic data
    n_rows = 500
    
    # Games played (highly skewed towards lower numbers)
    games = np.random.lognormal(mean=3.5, sigma=0.8, size=n_rows)
    games = np.clip(games.astype(int), 15, 300) # Ensure min 15 games
    
    # Base Rating (centered around 800-900 for low-rated players)
    base_rating = np.random.normal(loc=850, scale=100, size=n_rows).astype(int)
    
    # Rating Change: Introduce a weak positive correlation (Rating_Change = 0.2 * Games + Noise)
    rating_gain_base = 0.2 * games
    noise = np.random.normal(loc=15, scale=20, size=n_rows)
    rating_change = (rating_gain_base + noise).astype(int)
    
    # Earliest/Latest RATING generation
    latest_rating = base_rating + rating_change
    earliest_rating = base_rating
    
    data = pd.DataFrame({
        'Username': [f'Player{i:04d}' for i in range(n_rows)],
        'Total_Games_January': games,
        'Min_RATING_January': np.clip(earliest_rating - 50, 500, 1000),
        'Max_RATING_January': np.clip(latest_rating + 50, 500, 1000),
        'Average_RATING': (earliest_rating + latest_rating) / 2,
        'Earliest_RATING': earliest_rating,
        'Latest_RATING': latest_rating
    })
    
    data.to_csv(filepath, index=False)
    return data

def run_correlation_analysis(df: pd.DataFrame):
    """
    Performs the correlation analysis and generates the plot.
    """
    print("\n--- Starting Correlation Analysis ---")
    
    # 1. Prepare Data: Calculate the key dependent variable
    df['Rating_Change'] = df['Latest_RATING'] - df['Earliest_RATING']
    
    # ----------------------------------------------------
    # ANALYSIS METHOD 1: PEARSON CORRELATION (Linear Test)
    # ----------------------------------------------------
    games = df['Total_Games_January']
    rating_change = df['Rating_Change']
    
    # Calculate Pearson's r and the p-value
    correlation, p_value = pearsonr(games, rating_change)

    # Convert results to a DataFrame for CSV output
    results_df = pd.DataFrame({
        'Metric': ['Pearson R (Linear Correlation)', 'P-Value'],
        'Value': [correlation, p_value],
        'Interpretation': [
            f'Strength of linear relationship between games played and rating gain.',
            f'P-Value < 0.05 indicates the correlation is statistically significant.'
        ]
    })
    
    print(f"Pearson Correlation (r): {correlation:.4f} (P-Value: {p_value:.4f})")


    # ----------------------------------------------------
    # ANALYSIS METHOD 3: BINNING AND GROUPED AVERAGES
    # ----------------------------------------------------
    
    # Create the game bins based on the defined thresholds
    df['Game_Bin'] = pd.cut(df['Total_Games_January'], bins=GAME_BINS, labels=BIN_LABELS, right=False)
    
    # Calculate the mean rating change and mean games played for each bin
    grouped_analysis = df.groupby('Game_Bin', observed=True).agg(
        Average_Rating_Gain=('Rating_Change', 'mean'),
        Average_Games_Played=('Total_Games_January', 'mean'),
        Player_Count=('Username', 'size')
    ).reset_index()
    
    # Rename columns for clarity in the CSV
    grouped_analysis.rename(columns={'Game_Bin': 'Games_Played_Group'}, inplace=True)
    
    # Append grouped results to the main results DataFrame
    grouped_summary = grouped_analysis[['Games_Played_Group', 'Average_Rating_Gain', 'Average_Games_Played', 'Player_Count']]
    grouped_summary.columns = ['Metric', 'Value', 'Games_Played', 'Player_Count']
    grouped_summary['Interpretation'] = 'Average rating change within defined game volume groups.'
    
    results_df = pd.concat([results_df, grouped_summary], ignore_index=True)

    # Save the results to CSV
    results_df.to_csv(OUTPUT_CSV_PATH, index=False, float_format='%.4f')
    print(f"\n✅ Analysis results saved to: {OUTPUT_CSV_PATH}")


    # ----------------------------------------------------
    # ANALYSIS METHOD 2: SCATTER PLOT WITH REGRESSION LINE
    # ----------------------------------------------------
    
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 6))
    
    # Use seaborn to create a scatter plot with a linear regression line
    sns.regplot(
        x='Total_Games_January', 
        y='Rating_Change', 
        data=df, 
        scatter_kws={'alpha': 0.3, 's': 15}, 
        line_kws={'color': '#ff4500', 'linewidth': 2}
    )
    
    plt.title(
        f'Games Played vs. Rating Change (Glicko-2) | N={len(df)} Players\nPearson r: {correlation:.4f}', 
        fontsize=14
    )
    plt.xlabel('Total Games Played in January (X)')
    plt.ylabel('Rating Change (Latest RATING - Earliest RATING) (Y)')
    plt.axhline(0, color='gray', linestyle='--', linewidth=1) # Zero-gain line
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    try:
        plt.savefig(OUTPUT_PLOT_PATH)
        print(f"✅ Visualization saved to: {OUTPUT_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving plot: {e}")
        
    plt.close()


if __name__ == '__main__':
    
    # Check if the input file exists, otherwise create mock data
    if not os.path.exists(INPUT_CSV_PATH):
        print("ERROR: Input CSV not found, proceeding with mock data.")
        df_data = create_mock_data(INPUT_CSV_PATH)
    else:
        try:
            df_data = pd.read_csv(INPUT_CSV_PATH)
        except Exception as e:
            print(f"FATAL ERROR: Could not read input CSV file: {e}")
            df_data = None
            
    if df_data is not None:
        if len(df_data) < 2:
            print("ERROR: Not enough data points (need at least 2) to run correlation analysis.")
        elif 'Earliest_RATING' not in df_data.columns or 'Latest_RATING' not in df_data.columns:
            print("ERROR: Input CSV is missing required columns (Earliest_RATING or Latest_RATING).")
        else:
            run_correlation_analysis(df_data)