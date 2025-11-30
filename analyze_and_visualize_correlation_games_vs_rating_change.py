import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import os
import io
import re

# TODO: add visualization of pearson correlation of bins


# --- Configuration ---
INPUT_CSV_PATH = 'lichess-beginner-data-mining/2024_01_900+10_ONLY_players_max_rating_1600_results.csv'
OUTPUT_CSV_PATH = 'lichess-beginner-data-mining/2024_01_900+10_ONLY__players_max_rating_1600_correlation_results.csv'
OUTPUT_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_900+10_ONLY__players_max_rating_1600_correlation_games_vs_rating_change.png'
OUTPUT_BIN_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_900+10_ONLY__players_max_rating_1600_correlation_rating_gain_by_bin.png'

MAX_RATING_FILTER = 1600


# Define bins for the grouped analysis (Suggestion 3)
# These bins should start at MIN_GAMES_JANUARY (e.g., 15)
GAME_BINS = [15, 50, 100, 150, 200, 250, 300, 350, 400, np.inf]
BIN_LABELS = ['15-50 Games', '51-100 Games', '101-150 Games', '151-200 Games', '201-250 Games', '251-300 Games', '301-350 Games', '350-400 Games', '401+ Games']

# --- Analysis Function ---

def run_correlation_analysis(df: pd.DataFrame):
    """
    Performs the correlation analysis and generates the plot.
    """
    print("\n--- Starting Correlation Analysis ---")
    
    # 1. Prepare Data: Calculate the key dependent variable
    df['Rating_Change'] = df['Latest_RATING'] - df['Earliest_RATING']
    
    # ----------------------------------------------------
    # ANALYSIS METHOD 1: PEARSON CORRELATION (Linear Test on Raw Data)
    # ----------------------------------------------------
    games = df['Total_Games_January']
    rating_change = df['Rating_Change']
    
    # Calculate Pearson's r and the p-value
    correlation, p_value = pearsonr(games, rating_change)

    # Convert results to a DataFrame for CSV output
    results_df = pd.DataFrame({
        'Metric': ['Pearson R (Linear Correlation on Raw Data)', 'P-Value'],
        'Value': [correlation, p_value],
        'Interpretation': [
            f'Strength of linear relationship between games played and rating gain on the full dataset.',
            f'P-Value < 0.05 indicates the correlation is statistically significant.'
        ]
    })
    
    print(f"Raw Pearson Correlation (r): {correlation:.4f} (P-Value: {p_value:.4f})")


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


    # ----------------------------------------------------
    # ANALYSIS METHOD 5: GROUPED PEARSON CORRELATION (Correlate Bin Averages)
    # ----------------------------------------------------
    
    # Correlate the average games played per bin against the average rating gain per bin
    # This measures the linear trend of the group means visible in the bar chart.
    avg_games = grouped_analysis['Average_Games_Played']
    avg_gain = grouped_analysis['Average_Rating_Gain']
    
    # Calculate Pearson's r and the p-value on the grouped data
    grouped_correlation, grouped_p_value = pearsonr(avg_games, avg_gain)

    print("\n--- Grouped Correlation Analysis (Bin Averages) ---")
    print(f"Grouped Pearson Correlation (r): {grouped_correlation:.4f} (P-Value: {grouped_p_value:.4f})")
    
    # Append results to the main results DataFrame
    grouped_corr_results = pd.DataFrame([
        {'Metric': 'Grouped Pearson R (Bin Averages)', 'Value': grouped_correlation, 'Interpretation': 'Strength of linear relationship between average games played and average rating gain across the binned groups.'},
        {'Metric': 'Grouped P-Value', 'Value': grouped_p_value, 'Interpretation': 'P-Value < 0.05 indicates the grouped correlation is statistically significant.'},
    ])
    
    results_df = pd.concat([results_df, grouped_corr_results], ignore_index=True)


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

    # Set X-axis limits: starts at 15 (your minimum filter) and ends at 400 (to cut outliers)
    plt.xlim(15, 400) 
    
    # --- DYNAMICALLY UPDATED SCATTER PLOT TITLE ---
    plt.title(
        f'Games Played vs. Rating Change (Beginner Group, Max Rating < {MAX_RATING_FILTER}) | N={len(df)} Players\nRaw Pearson r: {correlation:.4f}', 
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

    # ----------------------------------------------------
    # ANALYSIS METHOD 4: BAR PLOT OF GROUPED AVERAGES (DUAL-AXIS)
    # ----------------------------------------------------
    
    # Start a new figure and the primary axis (for Rating Gain bars)
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Bar plot for Average Rating Gain (Left Axis)
    sns.barplot(
        x='Games_Played_Group', 
        y='Average_Rating_Gain', 
        data=grouped_analysis,
        palette='viridis', 
        ax=ax1,
        # label='Avg Rating Gain' # Labels will be added using fig.legend
    )
    
    # --- DYNAMICALLY UPDATED BAR PLOT TITLE ---
    ax1.set_title(f'Average Rating Change and Sample Size by Games Played Volume (Max Rating < {MAX_RATING_FILTER})', fontsize=14)
    # ------------------------------
    
    ax1.set_xlabel('Games Played Group (Volume)')
    ax1.set_ylabel('Average Glicko-2 Rating Gain (Latest - Earliest)', color=sns.color_palette('viridis')[0])
    ax1.tick_params(axis='y', labelcolor=sns.color_palette('viridis')[0])
    ax1.axhline(0, color='red', linestyle='--', linewidth=1) # Zero-gain line
    ax1.grid(axis='y', linestyle=':', alpha=0.6)

    # Rotate x-axis labels for readability
    ax1.set_xticklabels(grouped_analysis['Games_Played_Group'], rotation=45, ha='right')

    # Create a secondary axis for Player Count (Line plot, Right Axis)
    ax2 = ax1.twinx() 
    
    # Line plot for Player Count (Right Axis)
    line_plot = ax2.plot(
        grouped_analysis['Games_Played_Group'], 
        grouped_analysis['Player_Count'], 
        color='blue', 
        marker='o',
        linewidth=2,
        label='Player Count (N)'
    )
    
    # Set labels for the secondary axis
    ax2.set_ylabel('Player Count (N)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    ax2.grid(False) # Turn off grid for the secondary axis to keep it clean

    # Add count labels for the line markers (N)
    for index, row in grouped_analysis.iterrows():
        ax2.text(
            index, 
            row['Player_Count'] * 1.05, # Offset slightly above the marker
            f"N={row['Player_Count']}", 
            color='blue', 
            ha="center"
        )
    
    # Add labels for the average gain on top of the bars
    for index, row in grouped_analysis.iterrows():
        # Dynamic offset based on the maximum y-limit for better positioning
        offset = ax1.get_ylim()[1] * 0.02
        y_pos = row['Average_Rating_Gain'] + offset
        
        ax1.text(
            index, 
            y_pos, 
            f"{row['Average_Rating_Gain']:.1f}", 
            color='black', 
            ha="center"
        )
        
    # Combine legends from both axes
    bar_legend = [plt.Rectangle((0,0),1,1, fc=sns.color_palette('viridis')[0])]
    ax1.legend(bar_legend + line_plot, ['Avg Rating Gain', 'Player Count (N)'], loc='upper center')
    
    fig.tight_layout() # Ensures everything fits
    
    try:
        fig.savefig(OUTPUT_BIN_PLOT_PATH)
        print(f"✅ Grouped analysis visualization saved to: {OUTPUT_BIN_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving grouped plot: {e}")
    plt.close(fig)


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