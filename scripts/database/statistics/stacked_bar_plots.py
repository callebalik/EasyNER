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


class EntityDistributionPlotter:
    """
    Class for creating stacked bar plots showing entity distribution with consistent error-first ordering.
    """

    # Font size constants
    SMALL_FONT_SIZE = 12
    MEDIUM_FONT_SIZE = 14
    LARGE_FONT_SIZE = 16
    TITLE_FONT_SIZE = 18

    # Visual parameters
    BAR_HEIGHT = 0.7
    EPSILON = 1e-10  # Small value to avoid log(0) issues

    def __init__(self, entity_stats, figsize=(15, 5), wspace=0.3, left_margin=0.1, right_margin=0.9):
        """
        Initialize the entity distribution plotter.

        Args:
            entity_stats (pd.DataFrame): DataFrame containing entity statistics
            figsize (tuple): Figure size as (width, height)
            wspace (float): Width spacing between subplots
            left_margin (float): Left margin size (0-1)
            right_margin (float): Right margin size (0-1)
        """
        self.entity_stats = entity_stats
        self.figsize = figsize
        self.wspace = wspace
        self.left_margin = left_margin
        self.right_margin = right_margin

        # Extract data from entity_stats
        self.classes = entity_stats['named_entity_class']
        self.valid_percentages = entity_stats['Valid_percentage']
        self.mislab_percentages = entity_stats['Mislab_percentage']
        self.ambig_percentages = entity_stats['Ambig_percentage']
        self.total_counts = entity_stats['fq']

        self.valid_counts = entity_stats['Valid_count']
        self.mislab_counts = entity_stats['MISLAB']
        self.ambig_counts = entity_stats['AMBIG']

        # Position for bars
        self.y_pos = np.arange(len(self.classes))

        # Define color scheme
        self.colors = {
            'valid': NODE_COLOR_DEFAULT_RGB,  # Default gray color for valid entities
            'mislab': MISLABELED_COLOR_RGB,   # Use mislabeled error color
            'ambig': AMBIGUOUS_COLOR_RGB      # Use ambiguous error color
        }

        # Define class-specific colors for label highlights
        self.highlight_colors = {
            'PNM': PNM_HIGHLIGHT_COLORS[0.2],  # Using pre-computed highlight color with 0.2 alpha
            'DIS': DIS_HIGHLIGHT_COLORS[0.2]   # Using pre-computed highlight color with 0.2 alpha
        }

        # Initialize figure and axes
        self.fig, (self.ax_percentages, self.ax_counts) = plt.subplots(1, 2, figsize=self.figsize, constrained_layout=False)

    def setup_plot_parameters(self):
        """Set up general plot parameters and styling."""
        # Configure font sizes
        plt.rc('font', size=self.MEDIUM_FONT_SIZE)
        plt.rc('axes', titlesize=self.LARGE_FONT_SIZE)
        plt.rc('axes', labelsize=self.MEDIUM_FONT_SIZE)
        plt.rc('xtick', labelsize=self.SMALL_FONT_SIZE)
        plt.rc('ytick', labelsize=self.MEDIUM_FONT_SIZE)
        plt.rc('legend', fontsize=self.MEDIUM_FONT_SIZE)
        plt.rc('figure', titlesize=self.TITLE_FONT_SIZE)

        # Adjust layout
        plt.tight_layout(rect=[0, 0.03, 1, 0.80])  # Make room for shared legend and annotation
        plt.subplots_adjust(
            wspace=self.wspace,
            left=self.left_margin,
            right=self.right_margin
        )

        # Adjust spacing to reduce vertical padding
        plt.rcParams['figure.constrained_layout.h_pad'] = 0.05
        plt.rcParams['figure.constrained_layout.w_pad'] = 0.05

    def create_percentage_subplot(self):
        """Create the percentage distribution subplot (left)."""
        # Configure log scale
        self.ax_percentages.set_xscale('log')

        # Handle zero values (can't represent in log scale)
        valid_log = np.array([max(v, self.EPSILON) for v in self.valid_percentages])
        mislab_log = np.array([max(m, self.EPSILON) for m in self.mislab_percentages])
        ambig_log = np.array([max(a, self.EPSILON) for a in self.ambig_percentages])

        # Plot stacked percentage bars with errors first (ambig, mislab, valid)
        self.ax_percentages.barh(self.y_pos, ambig_log, self.BAR_HEIGHT, color=self.colors['ambig'], label='Ambiguous')
        self.ax_percentages.barh(
            self.y_pos,
            mislab_log,
            self.BAR_HEIGHT,
            left=ambig_log,
            color=self.colors['mislab'],
            label='Mislabeled'
        )
        self.ax_percentages.barh(
            self.y_pos,
            valid_log,
            self.BAR_HEIGHT,
            left=ambig_log + mislab_log,
            color=self.colors['valid'],
            label='Valid'
        )

        # Add text labels
        self._add_text_labels_to_percentage_bars()

        # Customize percentage subplot
        self.ax_percentages.set_yticks(self.y_pos)
        self.ax_percentages.set_yticklabels(self.classes, fontsize=self.MEDIUM_FONT_SIZE)
        self.ax_percentages.set_xlabel('Percentage (%) - Log Scale', fontsize=self.MEDIUM_FONT_SIZE, fontweight='bold')
        self.ax_percentages.set_ylabel('Named Entity Class', fontsize=self.MEDIUM_FONT_SIZE, fontweight='bold')
        self.ax_percentages.set_title('A: Distribution by Percentage (Errors First)', fontsize=self.LARGE_FONT_SIZE, fontweight='bold')

        # Configure x-ticks for percentage subplot - Fixed start at 0.001%
        log_percentage_ticks = [0.001, 0.01, 0.1, 1, 10, 100]
        # self.ax_percentages.set_xticks(log_percentage_ticks)
        # self.ax_percentages.set_xticklabels([f'{x}%' for x in log_percentage_ticks], fontsize=self.SMALL_FONT_SIZE)
        # Explicitly set the x-axis limits to match the desired range
        self.ax_percentages.set_xlim(0.0008, 110)  # Slightly wider than ticks for visual padding


        self.ax_percentages.grid(True, axis='x', linestyle='--', alpha=0.7)

    def _add_text_labels_to_percentage_bars(self):
        """Add text labels to percentage bars with proper positioning."""
        for i, (ambig, mislab, valid) in enumerate(zip(self.ambig_percentages, self.mislab_percentages, self.valid_percentages)):
            # Ambiguous segment
            if ambig > 0.01:
                log_mid_point = np.log10(self.EPSILON + ambig/2) if ambig > 0 else 0
                self.ax_percentages.text(10**log_mid_point, i, f'{ambig:.2f}%', ha='center', va='center',
                                    fontweight='bold', color='black', fontsize=self.SMALL_FONT_SIZE)

            # Mislabeled segment
            if mislab > 0.01:
                mid_point_orig = ambig + mislab/2
                log_mid_point = np.log10(mid_point_orig)
                self.ax_percentages.text(10**log_mid_point, i, f'{mislab:.2f}%', ha='center', va='center',
                                    fontweight='bold', color='white' if mislab > 5 else 'black',
                                    fontsize=self.SMALL_FONT_SIZE)

            # Valid segment
            if valid > 0.5:  # Only show if enough space
                mid_point_orig = ambig + mislab + valid/2
                log_mid_point = np.log10(mid_point_orig)
                self.ax_percentages.text(10**log_mid_point, i, f'{valid:.1f}%', ha='center', va='center',
                                    fontweight='bold', color='black' if valid > 20 else 'white',
                                    fontsize=self.SMALL_FONT_SIZE)

    def create_counts_subplot(self):
        """Create the counts distribution subplot (right)."""
        # Configure log scale
        self.ax_counts.set_xscale('log')

        # Handle zero values for counts
        ambig_counts_log = np.array([max(a, 1) for a in self.ambig_counts])
        mislab_counts_log = np.array([max(m, 1) for m in self.mislab_counts])
        valid_counts_log = np.array([max(v, 1) for v in self.valid_counts])

        # Plot stacked count bars with errors first ordering
        self.ax_counts.barh(self.y_pos, ambig_counts_log, self.BAR_HEIGHT, color=self.colors['ambig'], label='Ambiguous')
        self.ax_counts.barh(
            self.y_pos,
            mislab_counts_log,
            self.BAR_HEIGHT,
            left=ambig_counts_log,
            color=self.colors['mislab'],
            label='Mislabeled'
        )
        self.ax_counts.barh(
            self.y_pos,
            valid_counts_log,
            self.BAR_HEIGHT,
            left=ambig_counts_log + mislab_counts_log,
            color=self.colors['valid'],
            label='Valid'
        )

        # Add text labels
        self._add_text_labels_to_count_bars()

        # Customize counts subplot
        self.ax_counts.set_yticks(self.y_pos)
        self.ax_counts.set_yticklabels(self.classes, fontsize=self.MEDIUM_FONT_SIZE)
        self.ax_counts.set_xlabel('Entity Count (log scale)', fontsize=self.MEDIUM_FONT_SIZE, fontweight='bold')
        self.ax_counts.set_ylabel('Named Entity Class', fontsize=self.MEDIUM_FONT_SIZE, fontweight='bold')
        self.ax_counts.set_title('B: Distribution by Count (Errors First)', fontsize=self.LARGE_FONT_SIZE, fontweight='bold')

        # Generate appropriate log ticks based on data range
        max_count = max(self.total_counts)
        magnitude = int(np.log10(max_count)) + 1
        log_count_ticks = [10**i for i in range(0, magnitude)]
        self.ax_counts.set_xticks(log_count_ticks)
        self.ax_counts.set_xticklabels([self._format_count(x) for x in log_count_ticks], fontsize=self.SMALL_FONT_SIZE)
        self.ax_counts.grid(True, axis='x', linestyle='--', alpha=0.7)

    def _add_text_labels_to_count_bars(self):
        """Add text labels to count bars with proper positioning."""
        for i, (ambig, mislab, valid) in enumerate(zip(self.ambig_counts, self.mislab_counts, self.valid_counts)):
            # Ambiguous segment
            if ambig > 10:
                log_mid_point = np.log10(ambig/2 + 1)
                self.ax_counts.text(10**log_mid_point, i, self._format_count(ambig), ha='center', va='center',
                                fontweight='bold', color='black', fontsize=self.SMALL_FONT_SIZE)

            # Mislabeled segment
            if mislab > 10:
                mid_point_orig = ambig + mislab/2
                log_mid_point = np.log10(mid_point_orig)
                self.ax_counts.text(10**log_mid_point, i, self._format_count(mislab), ha='center', va='center',
                                fontweight='bold', color='white' if mislab > 1000 else 'black',
                                fontsize=self.SMALL_FONT_SIZE)

            # Valid segment
            if valid > 100:
                mid_point_orig = ambig + mislab + valid/2
                log_mid_point = np.log10(mid_point_orig)
                self.ax_counts.text(10**log_mid_point, i, self._format_count(valid), ha='center', va='center',
                                fontweight='bold', color='white' if valid > 1000000 else 'black',
                                fontsize=self.SMALL_FONT_SIZE)

    @staticmethod
    def _format_count(count):
        """Format large counts with appropriate suffixes (K, M)."""
        if count == 0:
            return "0"
        elif count < 1000:
            return f"{count:,.0f}"
        elif count < 1000000:
            return f"{count/1000:.1f}K"
        else:
            return f"{count/1000000:.1f}M"

    def apply_label_highlighting(self):
        """Apply background highlighting for specific class labels."""
        # Apply highlighting to left subplot labels
        for i, tl in enumerate(self.ax_percentages.get_yticklabels()):
            txt = tl.get_text()
            if txt == 'PNM':
                tl.set_backgroundcolor(NODE_COLOR_PNM_ONLY_RGB)
            elif txt == 'DIS':
                tl.set_backgroundcolor(NODE_COLOR_DIS_ONLY_RGB)

        # Apply highlighting to right subplot labels
        for i, tl in enumerate(self.ax_counts.get_yticklabels()):
            txt = tl.get_text()
            if txt == 'PNM':
                tl.set_backgroundcolor(NODE_COLOR_PNM_ONLY_RGB)
            elif txt == 'DIS':
                tl.set_backgroundcolor(NODE_COLOR_DIS_ONLY_RGB)

    def add_legend_and_title(self):
        """Add shared legend and main title."""
        # Create legend elements
        legend_elements = [
            Line2D([0], [0], color=self.colors['valid'], lw=8, label='Valid'),
            Line2D([0], [0], color=self.colors['mislab'], lw=8, label='Mislabeled'),
            Line2D([0], [0], color=self.colors['ambig'], lw=8, label='Ambiguous')
        ]

        # Add the legend
        self.fig.legend(
            handles=legend_elements,
            loc='upper center',
            ncol=5,
            frameon=True,
            bbox_to_anchor=(0.8, 0.96),
            fontsize=self.MEDIUM_FONT_SIZE
        )

        # Add main title
        self.fig.suptitle(
            'Entity Classification Analysis',
            fontsize=self.TITLE_FONT_SIZE,
            fontweight='bold',
            y=1.0
        )

    def create_plot(self):
        """Create the complete visualization."""
        self.setup_plot_parameters()
        self.create_percentage_subplot()
        self.create_counts_subplot()
        self.apply_label_highlighting()
        self.add_legend_and_title()

    def show(self):
        """Show the plot."""
        plt.show()

    def save(self, filename, dpi=300):
        """Save the plot to a file."""
        plt.savefig(filename, dpi=dpi, bbox_inches='tight')


def plot_stacked_bar(entity_stats):
    """
    Create and display stacked bar plots for entity distribution.

    Args:
        entity_stats (pd.DataFrame): DataFrame containing entity statistics
    """
    plotter = EntityDistributionPlotter(entity_stats, figsize=(15, 7))
    plotter.create_plot()
    plotter.show()