import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import os
import io
import re
import sys

# --- Configuration ---
INPUT_CSV_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_results.csv'
OUTPUT_CSV_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_correlation_results.csv'
OUTPUT_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_correlation_games_vs_rating_change.png'
OUTPUT_BIN_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_correlation_rating_gain_by_bin.png'
OUTPUT_DAYS_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_correlation_days_vs_rating_change.png'
OUTPUT_DAYS_BIN_PLOT_PATH = 'lichess-beginner-data-mining/2024_01_rapid_players_rated_801-1600_correlation_rating_gain_by_days_bin.png'

MIN_RATING_FILTER = 801
MAX_RATING_FILTER = 1600

# Define bins for the grouped analysis (Games Played)
GAME_BINS = [15, 50, 100, 150, 200, 250, 300, 350, 400, np.inf]
BIN_LABELS = ['15-50 Games', '51-100 Games', '101-150 Games', '151-200 Games', '201-250 Games', '251-300 Games', '301-350 Games', '350-400 Games', '401+ Games']

# Bins for days played
DAYS_BINS = [0, 5, 10, 15, 20, 25, np.inf]
DAYS_BIN_LABELS = ['1-5 Days', '6-10 Days', '11-15 Days', '16-20 Days', '21-25 Days', '26-30+ Days']

# --- Analysis Function ---

def run_correlation_analysis(df: pd.DataFrame):
    """
    Performs the correlation analysis and generates the plots.
    """
    print("\n--- Starting Correlation Analysis ---")
    
    # 1. Prepare Data: Calculate the key dependent variable
    df['Rating_Change'] = df['Latest_RATING'] - df['Earliest_RATING']
    
    # Check for the required new column
    if 'Days_Played' not in df.columns:
        print("FATAL ERROR: The required column 'Days_Played' is missing from the input CSV.")
        return
        
    if df.empty:
        print("ERROR: Filtering resulted in an empty DataFrame. Cannot run analysis.")
        return
        
    # Reusable Rating Range String for Titles
    RATING_RANGE_STR = f"Rating Range: {MIN_RATING_FILTER} - {MAX_RATING_FILTER}"
    
    # Total number of players for titles
    TOTAL_PLAYERS_N = len(df)


    # ----------------------------------------------------
    # ANALYSIS METHOD 1: PEARSON CORRELATION (Games Played vs. Rating Change)
    # ----------------------------------------------------
    games = df['Total_Games_January']
    rating_change = df['Rating_Change']
    
    correlation_games, p_value_games = pearsonr(games, rating_change)

    # Convert results to a DataFrame for CSV output
    results_df = pd.DataFrame({
        'Metric': ['Pearson R (Games)', 'P-Value (Games)'],
        'Value': [correlation_games, p_value_games],
        'Interpretation': [
            f'Strength of linear relationship between games played and rating change.',
            f'P-Value < 0.05 indicates the correlation is statistically significant.'
        ]
    })
    
    print(f"Raw Pearson Correlation (Games): {correlation_games:.4f} (P-Value: {p_value_games:.4f})")
    
    # ----------------------------------------------------
    # ANALYSIS METHOD 6: PEARSON CORRELATION (Days Played vs. Rating Change)
    # ----------------------------------------------------
    Days_Played = df['Days_Played']
    
    correlation_days, p_value_days = pearsonr(Days_Played, rating_change)
    
    # Append Days Played results to the main results DataFrame
    days_corr_results = pd.DataFrame([
        {'Metric': 'Pearson R (Days)', 'Value': correlation_days, 'Interpretation': 'Strength of linear relationship between days played and rating change.'},
        {'Metric': 'P-Value (Days)', 'Value': p_value_days, 'Interpretation': 'P-Value < 0.05 indicates the correlation is statistically significant.'},
    ])
    results_df = pd.concat([results_df, days_corr_results], ignore_index=True)

    print(f"Raw Pearson Correlation (Days): {correlation_days:.4f} (P-Value: {p_value_days:.4f})")

    # ----------------------------------------------------
    # ANALYSIS METHOD 3: BINNING AND GROUPED AVERAGES (Games Played)
    # ----------------------------------------------------
    
    # Create the game bins based on the defined thresholds
    df['Game_Bin'] = pd.cut(df['Total_Games_January'], bins=GAME_BINS, labels=BIN_LABELS, right=False)
    
    # Calculate the mean rating change and mean games played for each bin
    grouped_games_analysis = df.groupby('Game_Bin', observed=True).agg(
        Average_Rating_Gain=('Rating_Change', 'mean'),
        Average_Games_Played=('Total_Games_January', 'mean'),
        Player_Count=('Username', 'size')
    ).reset_index()
    
    # Rename columns for clarity in the CSV
    grouped_games_analysis.rename(columns={'Game_Bin': 'Games_Played_Group'}, inplace=True)
    
    # Append grouped results to the main results DataFrame
    grouped_summary_games = grouped_games_analysis[['Games_Played_Group', 'Average_Rating_Gain', 'Average_Games_Played', 'Player_Count']]
    grouped_summary_games.columns = ['Metric', 'Value', 'Games_Played', 'Player_Count']
    grouped_summary_games['Interpretation'] = 'Average rating change within defined game volume groups.'
    
    results_df = pd.concat([results_df, grouped_summary_games], ignore_index=True)


    # ----------------------------------------------------
    # ANALYSIS METHOD 7: BINNING AND GROUPED AVERAGES (Days Played)
    # ----------------------------------------------------
    
    # Create the days bins based on the defined thresholds
    df['Days_Bin'] = pd.cut(df['Days_Played'], bins=DAYS_BINS, labels=DAYS_BIN_LABELS, right=False)
    
    # Calculate the mean rating change and mean days played for each bin
    grouped_days_analysis = df.groupby('Days_Bin', observed=True).agg(
        Average_Rating_Gain=('Rating_Change', 'mean'),
        Average_Days_Played=('Days_Played', 'mean'),
        Player_Count=('Username', 'size')
    ).reset_index()
    
    # Rename columns for clarity in the CSV
    grouped_days_analysis.rename(columns={'Days_Bin': 'Days_Played_Group'}, inplace=True)
    
    # Append grouped results to the main results DataFrame
    grouped_summary_days = grouped_days_analysis[['Days_Played_Group', 'Average_Rating_Gain', 'Average_Days_Played', 'Player_Count']]
    grouped_summary_days.columns = ['Metric', 'Value', 'Days_Played', 'Player_Count']
    grouped_summary_days['Interpretation'] = 'Rating change within defined days played groups.'
    
    results_df = pd.concat([results_df, grouped_summary_days], ignore_index=True)


    # ----------------------------------------------------
    # ANALYSIS METHOD 5: GROUPED PEARSON CORRELATION (Games Played Bins)
    # ----------------------------------------------------
    
    # Correlate the average games played per bin against the average rating change per bin
    avg_games = grouped_games_analysis['Average_Games_Played']
    avg_gain_games = grouped_games_analysis['Average_Rating_Gain']
    
    grouped_correlation_games, grouped_p_value_games = pearsonr(avg_games, avg_gain_games)

    print("\n--- Grouped Correlation Analysis (Games Bins) ---")
    print(f"Grouped Pearson Correlation (Games): {grouped_correlation_games:.4f} (P-Value: {grouped_p_value_games:.4f})")
    
    # Append results to the main results DataFrame
    grouped_corr_results_games = pd.DataFrame([
        {'Metric': 'Grouped Pearson R (Games Bins)', 'Value': grouped_correlation_games, 'Interpretation': 'Strength of linear relationship between average games played and average rating gain across the binned groups.'},
        {'Metric': 'Grouped P-Value (Games Bins)', 'Value': grouped_p_value_games, 'Interpretation': 'P-Value < 0.05 indicates the grouped correlation is statistically significant.'},
    ])
    results_df = pd.concat([results_df, grouped_corr_results_games], ignore_index=True)


    # ----------------------------------------------------
    # ANALYSIS METHOD 8: GROUPED PEARSON CORRELATION (Days Played Bins)
    # ----------------------------------------------------
    
    # Correlate the average days played per bin against the average rating change per bin
    avg_days = grouped_days_analysis['Average_Days_Played']
    avg_gain_days = grouped_days_analysis['Average_Rating_Gain']
    
    grouped_correlation_days, grouped_p_value_days = pearsonr(avg_days, avg_gain_days)

    print("\n--- Grouped Correlation Analysis (Days Bins) ---")
    print(f"Grouped Pearson Correlation (Days): {grouped_correlation_days:.4f} (P-Value: {grouped_p_value_days:.4f})")
    
    # Append results to the main results DataFrame
    grouped_corr_results_days = pd.DataFrame([
        {'Metric': 'Grouped Pearson R (Days Bins)', 'Value': grouped_correlation_days, 'Interpretation': 'Strength of linear relationship between average days played and average rating change across the binned groups.'},
        {'Metric': 'Grouped P-Value (Days Bins)', 'Value': grouped_p_value_days, 'Interpretation': 'P-Value < 0.05 indicates the grouped correlation is statistically significant.'},
    ])
    results_df = pd.concat([results_df, grouped_corr_results_days], ignore_index=True)


    # Save the results to CSV
    results_df.to_csv(OUTPUT_CSV_PATH, index=False, float_format='%.4f')
    print(f"\n✅ Analysis results saved to: {OUTPUT_CSV_PATH}")

    # ====================================================
    #           VISUALIZATION SECTION
    # ====================================================
    
    # ----------------------------------------------------
    # ANALYSIS METHOD 2: SCATTER PLOT (Games Played vs. Rating Change)
    # ----------------------------------------------------
    
    plt.style.use('ggplot')
    plt.figure(figsize=(10, 6))
    
    sns.regplot(
        x='Total_Games_January', 
        y='Rating_Change', 
        data=df, 
        scatter_kws={'alpha': 0.3, 's': 15}, 
        line_kws={'color': '#ff4500', 'linewidth': 2}
    )

    plt.xlim(15, 400) 
    
    # UPDATED: Added P-value to the title
    plt.title(
        f'Games Played vs. Rating Change ({RATING_RANGE_STR}) | N={TOTAL_PLAYERS_N} Players\nRaw Pearson r: {correlation_games:.4f} (p-value: {p_value_games:.4f})', 
        fontsize=14
    )    
    plt.xlabel('Total Games Played in January (X)')
    plt.ylabel('Rating Change (Latest RATING - Earliest RATING) (Y)')
    plt.axhline(0, color='gray', linestyle='--', linewidth=1) 
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    try:
        plt.savefig(OUTPUT_PLOT_PATH)
        print(f"✅ Games visualization saved to: {OUTPUT_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving Games plot: {e}")
        
    plt.close()
    
    # ----------------------------------------------------
    # ANALYSIS METHOD 6: SCATTER PLOT (Days Played vs. Rating Change)
    # ----------------------------------------------------
    
    plt.figure(figsize=(10, 6))
    
    sns.regplot(
        x='Days_Played', 
        y='Rating_Change', 
        data=df, 
        scatter_kws={'alpha': 0.3, 's': 15}, 
        line_kws={'color': '#1f77b4', 'linewidth': 2} # New color for differentiation
    )

    plt.xlim(0, 31) # Limit X-axis to the days in the month (1-31)
    
    # UPDATED: Added P-value to the title
    plt.title(
        f'Days Played vs. Rating Change ({RATING_RANGE_STR}) | N={TOTAL_PLAYERS_N} Players\nRaw Pearson r: {correlation_days:.4f} (p-value: {p_value_days:.4f})', 
        fontsize=14
    )    
    plt.xlabel('Days Played in January (X)')
    plt.ylabel('Rating Change (Latest RATING - Earliest RATING) (Y)')
    plt.axhline(0, color='gray', linestyle='--', linewidth=1) 
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    
    try:
        plt.savefig(OUTPUT_DAYS_PLOT_PATH)
        print(f"✅ Days visualization saved to: {OUTPUT_DAYS_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving Days plot: {e}")
        
    plt.close()


    # ----------------------------------------------------
    # ANALYSIS METHOD 4: BAR PLOT (Games Played Groups)
    # ----------------------------------------------------
    
    fig, ax1 = plt.subplots(figsize=(12, 6))

    sns.barplot(
        x='Games_Played_Group', 
        y='Average_Rating_Gain', 
        data=grouped_games_analysis,
        palette='viridis', 
        ax=ax1,
    )
    
    # UPDATED: Added N and P-value to the title
    ax1.set_title(
        f'Rating Change vs. Games Played ({RATING_RANGE_STR}) | N={TOTAL_PLAYERS_N} Players \nGrouped Pearson r: {grouped_correlation_games:.4f} (p-value: {grouped_p_value_games:.4f})', 
        fontsize=14, 
        y=1.05 # Adjusted y for multiline title
    )
    
    ax1.set_xlabel('Games Played Group (Volume)')
    ax1.set_ylabel('Glicko-2 Rating Change (Latest - Earliest)', color=sns.color_palette('viridis')[0])
    ax1.tick_params(axis='y', labelcolor=sns.color_palette('viridis')[0])
    ax1.axhline(0, color='red', linestyle='--', linewidth=1) 
    ax1.grid(axis='y', linestyle=':', alpha=0.6)

    ax1.set_xticklabels(grouped_games_analysis['Games_Played_Group'], rotation=45, ha='right')

    ax2 = ax1.twinx() 
    
    line_plot = ax2.plot(
        grouped_games_analysis['Games_Played_Group'], 
        grouped_games_analysis['Player_Count'], 
        color='blue', 
        marker='o',
        linewidth=2,
        label='Player Count (N)'
    )
    
    ax2.set_ylabel('Player Count (N)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    ax2.grid(False) 

    # Annotate player count N on the line plot
    for index, row in grouped_games_analysis.iterrows():
        ax2.text(
            index, 
            row['Player_Count'] * 1.05, 
            f"N={row['Player_Count']}", 
            color='blue', 
            ha="center"
        )
    
    # Annotate average rating gain on the bar plot
    for index, row in grouped_games_analysis.iterrows():
        offset = ax1.get_ylim()[1] * 0.02
        y_pos = row['Average_Rating_Gain'] + offset
        
        ax1.text(
            index, 
            y_pos, 
            f"{row['Average_Rating_Gain']:.1f}", 
            color='black', 
            ha="center"
        )
        
    bar_legend = [plt.Rectangle((0,0),1,1, fc=sns.color_palette('viridis')[0])]
    ax1.legend(bar_legend + line_plot, ['Rating Change', 'Player Count (N)'], loc='upper center')
    
    fig.tight_layout() 
    
    try:
        fig.savefig(OUTPUT_BIN_PLOT_PATH)
        print(f"✅ Games grouped analysis visualization saved to: {OUTPUT_BIN_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving Games grouped plot: {e}")
    plt.close(fig)

    # ----------------------------------------------------
    # ANALYSIS METHOD 7: BAR PLOT (Days Played Groups)
    # ----------------------------------------------------
    
    fig, ax1 = plt.subplots(figsize=(12, 6))

    sns.barplot(
        x='Days_Played_Group', 
        y='Average_Rating_Gain', 
        data=grouped_days_analysis,
        palette='magma', # New color palette for differentiation
        ax=ax1,
    )
    
    # UPDATED: Added N and P-value to the title
    ax1.set_title(
        f'Rating Change vs. Days Played ({RATING_RANGE_STR}) | N={TOTAL_PLAYERS_N} Players \nGrouped Pearson r: {grouped_correlation_days:.4f} (p-value: {grouped_p_value_days:.4f})', 
        fontsize=14, 
        y=1.05 # Adjusted y for multiline title
    )
    
    ax1.set_xlabel('Days Played Group (Volume)')
    ax1.set_ylabel('Glicko-2 Rating Change (Latest - Earliest)', color=sns.color_palette('magma')[0])
    ax1.tick_params(axis='y', labelcolor=sns.color_palette('magma')[0])
    ax1.axhline(0, color='red', linestyle='--', linewidth=1) 
    ax1.grid(axis='y', linestyle=':', alpha=0.6)

    ax1.set_xticklabels(grouped_days_analysis['Days_Played_Group'], rotation=45, ha='right')

    ax2 = ax1.twinx() 
    
    line_plot = ax2.plot(
        grouped_days_analysis['Days_Played_Group'], 
        grouped_days_analysis['Player_Count'], 
        color='blue', 
        marker='o',
        linewidth=2,
        label='Player Count (N)'
    )
    
    ax2.set_ylabel('Player Count (N)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    ax2.grid(False) 

    # Annotate player count N on the line plot
    for index, row in grouped_days_analysis.iterrows():
        ax2.text(
            index, 
            row['Player_Count'] * 1.05, 
            f"N={row['Player_Count']}", 
            color='blue', 
            ha="center"
        )
    
    # Annotate average rating gain on the bar plot
    for index, row in grouped_days_analysis.iterrows():
        offset = ax1.get_ylim()[1] * 0.02
        y_pos = row['Average_Rating_Gain'] + offset
        
        ax1.text(
            index, 
            y_pos, 
            f"{row['Average_Rating_Gain']:.1f}", 
            color='black', 
            ha="center"
        )
        
    bar_legend = [plt.Rectangle((0,0),1,1, fc=sns.color_palette('magma')[0])]
    ax1.legend(bar_legend + line_plot, ['Rating Change', 'Player Count (N)'], loc='upper center')
    
    fig.tight_layout() 
    
    try:
        fig.savefig(OUTPUT_DAYS_BIN_PLOT_PATH)
        print(f"✅ Days grouped analysis visualization saved to: {OUTPUT_DAYS_BIN_PLOT_PATH}")
    except Exception as e:
        print(f"Error saving Days grouped plot: {e}")
    plt.close(fig)


if __name__ == '__main__':
    
    # Check if the input file exists
    if not os.path.exists(INPUT_CSV_PATH):
        print("ERROR: Input CSV not found, please rerun with a valid input file.")
        sys.exit()
    else:
        try:
            df_data = pd.read_csv(INPUT_CSV_PATH)
        except Exception as e:
            print(f"FATAL ERROR: Could not read input CSV file: {e}")
            df_data = None
            
    if df_data is not None:
        if len(df_data) < 2:
            print("ERROR: Not enough data points (need at least 2) to run correlation analysis.")
        else:
            run_correlation_analysis(df_data)