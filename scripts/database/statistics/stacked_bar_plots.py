import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from .color_scheme import (
    AMBIGUOUS_COLOR_RGB,
    MISLABELED_COLOR_RGB,
    NODE_COLOR_DEFAULT_RGB,
    NODE_COLOR_DIS_ONLY_RGB,
    NODE_COLOR_PNM_ONLY_RGB,
    ERROR_COLORS_RGB,
    DIS_HIGHLIGHT_COLORS,
    PNM_HIGHLIGHT_COLORS
)

def plot_stacked_bar(entity_stats):
    # Create figure with two subplots
    fig, (ax_percentages, ax_counts) = plt.subplots(1, 2, figsize=(15, 5), constrained_layout=False)
    # Fine-tune layout
    plt.tight_layout(rect=[0, 0.03, 1, 0.90])  # Make room for shared legend and annotation
    plt.subplots_adjust(wspace=0.05)  # Reduce space between subplots
    # Fine-tune layout - replace existing layout adjustments with these
    plt.tight_layout(rect=[0, 0.03, 1, 0.80])  # Make room for shared legend and annotation
    plt.subplots_adjust(wspace=0.3, left=0.1, right=0.9)  # Increase space between subplots and add margins

    # Set larger font sizes
    SMALL_FONT_SIZE = 12
    MEDIUM_FONT_SIZE = 14
    LARGE_FONT_SIZE = 16
    TITLE_FONT_SIZE = 18

    plt.rc('font', size=MEDIUM_FONT_SIZE)          # default text size
    plt.rc('axes', titlesize=LARGE_FONT_SIZE)      # axes title font size
    plt.rc('axes', labelsize=MEDIUM_FONT_SIZE)     # axes label font size
    plt.rc('xtick', labelsize=SMALL_FONT_SIZE)     # x tick labels font size
    plt.rc('ytick', labelsize=MEDIUM_FONT_SIZE)    # y tick labels font size
    plt.rc('legend', fontsize=MEDIUM_FONT_SIZE)    # legend font size
    plt.rc('figure', titlesize=TITLE_FONT_SIZE)    # figure title size

    # Adjust spacing to reduce vertical padding
    plt.rcParams['figure.constrained_layout.h_pad'] = 0.05
    plt.rcParams['figure.constrained_layout.w_pad'] = 0.05

    # Get the entity classes, percentages and counts
    classes = entity_stats['named_entity_class']
    valid_percentages = entity_stats['Valid_percentage']
    mislab_percentages = entity_stats['Mislab_percentage']
    ambig_percentages = entity_stats['Ambig_percentage']

    # Get the actual counts (either directly from entity_stats or computed from percentages)
    total_counts = entity_stats['fq']
    valid_counts = (valid_percentages * total_counts / 100).astype(int)
    mislab_counts = (mislab_percentages * total_counts / 100).astype(int)
    ambig_counts = (ambig_percentages * total_counts / 100).astype(int)

    # Create positions for the bars
    y_pos = np.arange(len(classes))
    bar_height = 0.7  # Slightly increased bar height

    # Use color scheme variables from color_scheme.py with RGB values
    colors = {
        'valid': NODE_COLOR_DEFAULT_RGB,   # Default gray color for valid entities
        'mislab': MISLABELED_COLOR_RGB,    # Use mislabeled error color
        'ambig': AMBIGUOUS_COLOR_RGB       # Use ambiguous error color
    }

    # Define class-specific colors for label highlights
    highlight_colors = {
        'PNM': PNM_HIGHLIGHT_COLORS[0.2],  # Using pre-computed highlight color with 0.2 alpha
        'DIS': DIS_HIGHLIGHT_COLORS[0.2]   # Using pre-computed highlight color with 0.2 alpha
    }

    # ---- LEFT SUBPLOT: PERCENTAGES (LOG SCALE) ----
    ax_percentages.set_xscale('log')

    # Handle zero values (can't represent in log scale)
    epsilon = 1e-10
    valid_log = np.array([max(v, epsilon) for v in valid_percentages])
    mislab_log = np.array([max(m, epsilon) for m in mislab_percentages])
    ambig_log = np.array([max(a, epsilon) for a in ambig_percentages])

    # Plot stacked percentage bars with errors first (ambig, mislab, valid)
    ambig_bars = ax_percentages.barh(y_pos, ambig_log, bar_height, color=colors['ambig'], label='Ambiguous')
    mislab_bars = ax_percentages.barh(y_pos, mislab_log, bar_height, left=ambig_log, color=colors['mislab'], label='Mislabeled')
    valid_bars = ax_percentages.barh(y_pos, valid_log, bar_height, left=ambig_log + mislab_log, color=colors['valid'], label='Valid')

    # Improved log-space positioning for text labels
    for i, (ambig, mislab, valid) in enumerate(zip(ambig_percentages, mislab_percentages, valid_percentages)):
        # Ambiguous segment - proper log-space calculation
        if ambig > 0.01:
            log_mid_point = np.log10(epsilon + ambig/2) if ambig > 0 else 0
            ax_percentages.text(10**log_mid_point, i, f'{ambig:.2f}%', ha='center', va='center',
                            fontweight='bold', color='black',
                            fontsize=SMALL_FONT_SIZE)

        # Mislabeled segment - proper log-space calculation
        if mislab > 0.01:
            # Find midpoint in original space, then convert to log space
            mid_point_orig = ambig + mislab/2
            log_mid_point = np.log10(mid_point_orig)
            ax_percentages.text(10**log_mid_point, i, f'{mislab:.2f}%', ha='center', va='center',
                            fontweight='bold', color='white' if mislab > 5 else 'black',
                            fontsize=SMALL_FONT_SIZE)

        # Valid segment - proper log-space calculation
        if valid > 0.5:  # Only show if enough space
            # Find midpoint in original space, then convert to log space
            mid_point_orig = ambig + mislab + valid/2
            log_mid_point = np.log10(mid_point_orig)
            ax_percentages.text(10**log_mid_point, i, f'{valid:.1f}%', ha='center', va='center',
                            fontweight='bold', color='black' if valid > 20 else 'white',
                            fontsize=SMALL_FONT_SIZE)

    # Customize percentage subplot
    ax_percentages.set_yticks(y_pos)
    ax_percentages.set_yticklabels(classes, fontsize=MEDIUM_FONT_SIZE)
    ax_percentages.set_xlabel('Percentage (%) - Log Scale', fontsize=MEDIUM_FONT_SIZE, fontweight='bold')
    ax_percentages.set_ylabel('Named Entity Class', fontsize=MEDIUM_FONT_SIZE, fontweight='bold')
    ax_percentages.set_title('A: Distribution by Percentage', fontsize=LARGE_FONT_SIZE, fontweight='bold')

    # Custom x-ticks for percentage subplot
    log_percentage_ticks = [0.001, 0.01, 0.1, 1, 10, 100]
    ax_percentages.set_xticks(log_percentage_ticks)
    ax_percentages.set_xticklabels([f'{x}%' for x in log_percentage_ticks], fontsize=SMALL_FONT_SIZE)
    ax_percentages.grid(True, axis='x', linestyle='--', alpha=0.7)

    # ---- RIGHT SUBPLOT: ABSOLUTE COUNTS (LOG SCALE) WITH REVERSED STACKING ----
    ax_counts.set_xscale('log')

    # Handle zero values for counts
    ambig_counts_log = np.array([max(a, 1) for a in ambig_counts])
    mislab_counts_log = np.array([max(m, 1) for m in mislab_counts])
    valid_counts_log = np.array([max(v, 1) for v in valid_counts])

    # Plot stacked count bars with reversed order (errors first)
    ambig_bars2 = ax_counts.barh(y_pos, ambig_counts_log, bar_height, color=colors['ambig'], label='Ambiguous')
    mislab_bars2 = ax_counts.barh(y_pos, mislab_counts_log, bar_height, left=ambig_counts_log, color=colors['mislab'], label='Mislabeled')
    valid_bars2 = ax_counts.barh(y_pos, valid_counts_log, bar_height, left=ambig_counts_log + mislab_counts_log, color=colors['valid'], label='Valid')

    # Define function to format counts with appropriate suffixes
    def format_count(count):
        if count == 0:
            return "0"
        elif count < 1000:
            return f"{count:,.0f}"
        elif count < 1000000:
            return f"{count/1000:.1f}K"
        else:
            return f"{count/1000000:.1f}M"

    # Improved log-space positioning for count labels
    for i, (ambig, mislab, valid) in enumerate(zip(ambig_counts, mislab_counts, valid_counts)):
        # Ambiguous segment - properly calculate log-space midpoint
        if ambig > 10:
            log_mid_point = np.log10(ambig/2 + 1)
            ax_counts.text(10**log_mid_point, i, format_count(ambig), ha='center', va='center',
                        fontweight='bold', color='black',
                        fontsize=SMALL_FONT_SIZE)

        # Mislabeled segment - properly calculate log-space midpoint
        if mislab > 10:
            mid_point_orig = ambig + mislab/2
            log_mid_point = np.log10(mid_point_orig)
            ax_counts.text(10**log_mid_point, i, format_count(mislab), ha='center', va='center',
                        fontweight='bold', color='white' if mislab > 1000 else 'black',
                        fontsize=SMALL_FONT_SIZE)

        # Valid segment - properly calculate log-space midpoint
        if valid > 100:
            mid_point_orig = ambig + mislab + valid/2
            log_mid_point = np.log10(mid_point_orig)
            ax_counts.text(10**log_mid_point, i, format_count(valid), ha='center', va='center',
                        fontweight='bold', color='white' if valid > 1000000 else 'black',
                        fontsize=SMALL_FONT_SIZE)

    # Customize counts subplot
    ax_counts.set_yticks(y_pos)
    # Add y-labels to right subplot too
    ax_counts.set_yticklabels(classes, fontsize=MEDIUM_FONT_SIZE)
    ax_counts.set_xlabel('Entity Count (log scale)', fontsize=MEDIUM_FONT_SIZE, fontweight='bold')
    ax_counts.set_ylabel('Named Entity Class', fontsize=MEDIUM_FONT_SIZE, fontweight='bold')
    ax_counts.set_title('B: Distribution by Count (Errors First)', fontsize=LARGE_FONT_SIZE, fontweight='bold')

    # Generate appropriate log ticks based on data range
    max_count = max(total_counts)
    magnitude = int(np.log10(max_count)) + 1
    log_count_ticks = [10**i for i in range(0, magnitude)]
    ax_counts.set_xticks(log_count_ticks)
    ax_counts.set_xticklabels([format_count(x) for x in log_count_ticks], fontsize=SMALL_FONT_SIZE)
    ax_counts.grid(True, axis='x', linestyle='--', alpha=0.7)

    # Apply background highlighting for specific class labels
    for i, tl in enumerate(ax_percentages.get_yticklabels()):
        txt = tl.get_text()
        if txt == 'PNM':
            tl.set_backgroundcolor(NODE_COLOR_PNM_ONLY_RGB)

        elif txt == 'DIS':
            tl.set_backgroundcolor(NODE_COLOR_DIS_ONLY_RGB)

    # Also apply highlighting to right subplot labels
    for i, tl in enumerate(ax_counts.get_yticklabels()):
        txt = tl.get_text()
        if txt == 'PNM':
            tl.set_backgroundcolor(NODE_COLOR_PNM_ONLY_RGB)

        elif txt == 'DIS':
            tl.set_backgroundcolor(NODE_COLOR_DIS_ONLY_RGB)

    # Create a single shared legend at the top
    legend_elements = [
        Line2D([0], [0], color=colors['valid'], lw=8, label='Valid'),
        Line2D([0], [0], color=colors['mislab'], lw=8, label='Mislabeled'),
        Line2D([0], [0], color=colors['ambig'], lw=8, label='Ambiguous')
    ]

    fig.legend(handles=legend_elements, loc='upper center', ncol=5, frameon=True,
            bbox_to_anchor=(0.8, 0.96), fontsize=MEDIUM_FONT_SIZE)

    # Main title - positioned to make room for legend
    fig.suptitle('Entity Classification Analysis', fontsize=TITLE_FONT_SIZE, fontweight='bold', y=1.0)



    plt.show()