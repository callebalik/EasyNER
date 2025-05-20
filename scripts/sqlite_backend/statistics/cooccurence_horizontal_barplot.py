import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.gridspec as gridspec
from matplotlib.patches import Rectangle
from scripts.sqlite_backend.statistics.color_scheme import *


class CooccurenceHorizontalBarplot:
    """
    A class for visualizing entity pairs (e.g., Disease-Phenomenon relationships)
    with customizable appearance for each entity type in a seamless grid layout.
    """

    def __init__(
        self,
        first_entity_color="#e6f2ff",
        second_entity_color="#e6ffe6",
        first_entity_name="Disease",
        second_entity_name="Phenomenon",
        separator="|",
        fontsize=9,
        column_spacing=0.1,
        label_opacity=0.9,  # Increased opacity for label backgrounds
        title_opacity=0.95,
    ):  # Increased opacity for title backgrounds):
        """
        Initialize the visualizer with styling preferences.

        Args:
            first_entity_color: Background color for first entity (default: light blue)
            second_entity_color: Background color for second entity (default: light green)
            first_entity_name: Label for the first entity category
            second_entity_name: Label for the second entity category
            separator: Text separator between entity pairs
            fontsize: Size of text in labels
            column_spacing: Spacing between grid columns
            label_opacity: Opacity for label backgrounds 0.0-1.0
            title_opacity: Opacity for title backgrounds 0.0-1.0
        """
        self.first_entity_color = first_entity_color
        self.second_entity_color = second_entity_color
        self.first_entity_name = first_entity_name
        self.second_entity_name = second_entity_name
        self.separator = separator
        self.fontsize = fontsize
        self.column_spacing = column_spacing
        self.label_opacity = label_opacity
        self.title_opacity = title_opacity

    def create_bar_plot(
        self, df, x_column, figsize=(14, 10), title=None, palette="viridis"
    ):
        """
        Create a horizontal bar plot with seamless entity label columns and the plot.

        Args:
            df: DataFrame containing entity pair data
            x_column: Column name to plot on the x-axis
            figsize: Figure size as tuple (width, height)
            title: Plot title
            palette: Color palette for bars

        Returns:
            fig: The created figure object
        """
        # Create figure with gridspec for layout control
        fig = plt.figure(figsize=figsize)

        # Configure GridSpec with explicit spacing parameters
        gs = gridspec.GridSpec(
            1,
            3,
            width_ratios=[2, 2, 6],
            figure=fig,
            wspace=self.column_spacing,  # Use the column spacing parameter
        )

        # Create three axes: first_entity_ax, second_entity_ax, plot_ax
        first_entity_ax = fig.add_subplot(gs[0])
        second_entity_ax = fig.add_subplot(gs[1], sharey=first_entity_ax)
        plot_ax = fig.add_subplot(gs[2], sharey=first_entity_ax)

        # Set up y-positions and order
        y_positions = np.arange(len(df))

        # Create bar plot in the main plot area
        bars = plot_ax.barh(
            y_positions, df[x_column], color=sns.color_palette(palette, len(df))
        )
        plot_ax.set_xlim(left=0)  # Force x-axis to start at 0
        plot_ax.set_xlabel(x_column)
        plot_ax.grid(axis="x")

        # Configure y-ticks on the plot
        plot_ax.set_yticks(y_positions)
        plot_ax.set_yticklabels([])  # No labels on plot y-axis

        # Set title if provided
        if title:
            fig.suptitle(
                title, fontsize=14, y=0.98
            )  # Adjust title position to prevent layout shifts

        # Draw First Entity labels with edge alignment
        self._draw_entity_labels(
            first_entity_ax,
            y_positions,
            df[self.first_entity_name],
            self.first_entity_color,
            label_alignment="right",
            edge_alignment="right",
        )

        # Add styled title with background instead of simple set_title
        self._add_styled_title(
            first_entity_ax, self.first_entity_name, self.first_entity_color
        )

        # Draw Second Entity labels with edge alignment on both sides
        self._draw_entity_labels(
            second_entity_ax,
            y_positions,
            df[self.second_entity_name],
            self.second_entity_color,
            label_alignment="left",
            edge_alignment="both",
            add_separator=True,
        )

        # Add styled title with background instead of simple set_title
        self._add_styled_title(
            second_entity_ax, self.second_entity_name, self.second_entity_color
        )

        # Remove unnecessary elements and spines
        for ax in [first_entity_ax, second_entity_ax]:
            ax.set_xticks([])
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["bottom"].set_visible(False)
            ax.spines["left"].set_visible(False)

        # Draw connecting horizontal lines across all panels
        for y_pos in y_positions:
            # Draw light horizontal lines across all axes for better tracking
            first_entity_ax.axhline(
                y=y_pos, color="#dddddd", linestyle="-", linewidth=0.5, alpha=0.7
            )
            second_entity_ax.axhline(
                y=y_pos, color="#dddddd", linestyle="-", linewidth=0.5, alpha=0.7
            )

        # Layout management - use a single approach to avoid conflicts
        # First apply overall layout adjustments
        fig.subplots_adjust(wspace=self.column_spacing, left=0.05, right=0.98)

        return fig

    def _draw_entity_labels(
        self,
        ax,
        y_positions,
        entity_names,
        background_color,
        label_alignment="left",
        edge_alignment="right",
        add_separator=False,
    ):
        """
        Draw entity labels with colored backgrounds that align perfectly with neighbors.

        Args:
            ax: Axis to draw labels on
            y_positions: Y-coordinates for labels
            entity_names: List of entity names
            background_color: Background color for labels
            label_alignment: Text alignment ('left', 'right', or 'center')
            edge_alignment: How to align rectangle edges ('left', 'right', 'both', or 'none')
            add_separator: Whether to add a separator line at the edge
        """
        # Clear any existing elements
        ax.clear()

        # Set y-ticks with empty labels (managed in plot_ax)
        ax.set_yticks(y_positions)
        ax.set_yticklabels([])

        # Set x-limits for proper alignment
        ax.set_xlim(0, 1)

        # Add background rectangles and text for each entity
        for i, (y_pos, entity_name) in enumerate(zip(y_positions, entity_names)):
            # Background rectangle - extend to edges based on alignment
            rect = Rectangle(
                xy=(0, y_pos - 0.4),
                width=1,
                height=0.8,
                facecolor=background_color,
                edgecolor="#aaaaaa",  # Lighter border
                alpha=self.label_opacity,
                linewidth=0.5,
            )
            ax.add_patch(rect)

            # Text label - position based on label_alignment
            text_x = 0.5
            if label_alignment == "right":
                text_x = 0.95
            elif label_alignment == "left":
                text_x = 0.05

            ax.text(
                text_x,
                y_pos,
                str(entity_name),
                va="center",
                ha=label_alignment,
                fontsize=self.fontsize,
                fontweight="normal",
            )

        # Add separator line if requested (typically for the second entity)
        if add_separator:
            ax.axvline(x=0, color="black", linestyle="-", linewidth=1)

    def _add_styled_title(self, ax, title_text, bg_color):
        """
        Add a styled title with background color to an axis.

        Args:
            ax: The axis to add the title to
            title_text: The title text
            bg_color: Background color for the title
        """
        # Remove the default title
        ax.set_title("")

        # Get the width of the axis
        bbox = ax.get_window_extent().transformed(ax.figure.dpi_scale_trans.inverted())
        width = bbox.width

        # Add a rectangle at the top of the axis for the background
        rect = Rectangle(
            xy=(0, 1.02),  # Position just above the plot area
            width=1,
            height=0.1,
            transform=ax.transAxes,  # Use axis coordinates (0-1)
            facecolor=bg_color,
            edgecolor="#aaaaaa",
            alpha=self.title_opacity,
            linewidth=0.5,
        )
        ax.add_patch(rect)

        # Add the title text on top of the rectangle
        ax.text(
            0.5,
            1.03,  # Centered horizontally, positioned vertically within the rectangle
            title_text,
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=11,
            fontweight="bold",
        )


if __name__ == "__main__":
    # Example usage
    import pandas as pd

    # Sample DataFrame
    data = {
        "Disease": ["Disease A", "Disease B", "Disease C"],
        "Phenomenon": ["Phenomenon X", "Phenomenon Y", "Phenomenon Z"],
        "NPMI": [0.5, 0.7, 0.9],
        "FQ_DOC_LEVEL": [10, 20, 30],
    }
    df_npmi = pd.DataFrame(data)
    df_fq_doc_level = pd.DataFrame(data)
    # Initialize the visualizer with appropriate colors
    entity_pair_visualizer = CooccurenceHorizontalBarplot(
        first_entity_color=NODE_COLOR_DIS_ONLY_RGB,  # Light blue for diseases
        second_entity_color=NODE_COLOR_PNM_ONLY_RGB,  # Light green for phenomena
        separator="|",  # Vertical line separator
        column_spacing=0.02,  # Minimal spacing between columns
        label_opacity=0.6,  # Increased opacity for label backgrounds
        title_opacity=0.95,  # Increased opacity for title backgrounds
    )

    # Create and show the NPMI plot
    npmi_fig = entity_pair_visualizer.create_bar_plot(
        df=df_npmi,
        x_column="NPMI",
    )
    plt.show()

    # Create and show the Document Frequency plot
    frequency_fig = entity_pair_visualizer.create_bar_plot(
        df=df_fq_doc_level,
        x_column="FQ_DOC_LEVEL",
        title="Document Frequency by Entity Pair",
        palette="pastel",
    )
    plt.show()
