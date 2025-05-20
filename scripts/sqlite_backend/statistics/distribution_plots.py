from matplotlib import pyplot as plt
import seaborn as sns
import numpy as np


class DualDistributionVisualizer:
    """Class for creating and annotating statistical distributions."""

    def __init__(self, data_array, primary_column_index=0, secondary_column_index=1):
        """
        Initialize with data and column indices.

        Args:
            data_array: NumPy array with data columns
            primary_column_index: Index for primary variable (NPMI)
            secondary_column_index: Index for secondary variable (Unique Documents)
        """
        self.data = data_array
        self.primary_data = data_array[:, primary_column_index]
        self.secondary_data = data_array[:, secondary_column_index]
        self.fig = None
        self.ax_primary = None
        self.ax_secondary = None

    def calculate_percentiles(self, percentile_values=[25, 50, 75, 90]):
        """
        Calculate percentiles for the primary data.

        Args:
            percentile_values: List of percentiles to calculate

        Returns:
            Dictionary mapping percentile values to their corresponding data values
        """
        return {p: np.percentile(self.primary_data, p) for p in percentile_values}

    def find_percentile_rank(self, value):
        """
        Find the percentile rank of a specific value in primary data.

        Args:
            value: The value to find the percentile rank for

        Returns:
            The percentile rank (0-100) of the given value
        """
        # Calculate rank using the percent of values that are less than the given value
        return 100 * (self.primary_data < value).mean()

    def create_dual_axis_plot(self, figsize=(10, 7), bins=100, show_kde=True):
        """
        Create dual-axis histogram for primary and secondary data.

        Args:
            figsize: Tuple of (width, height) for figure size
            bins: Number of histogram bins
            show_kde: Whether to show KDE line

        Returns:
            Self for method chaining
        """
        self.fig, self.ax_primary = plt.subplots(figsize=figsize)

        # Configure primary axis (NPMI)
        primary_color = "tab:blue"
        label_color = "black"
        self.ax_primary.set_xlabel("Normalized Pointwise Mutual Information (NPMI)")
        self.ax_primary.xaxis.label.set_bbox(
            dict(facecolor=primary_color, alpha=0.5, pad=5)
        )
        self.ax_primary.set_ylabel("Frequency", color=label_color, fontsize=14)
        sns.histplot(
            self.primary_data,
            bins=bins,
            kde=show_kde,
            ax=self.ax_primary,
            color=primary_color,
        )
        self.ax_primary.tick_params(axis="y", labelcolor=label_color)
        self.ax_primary.locator_params(axis="y", nbins=30)
        self.ax_primary.grid(alpha=0.3)
        self.ax_primary.xaxis.labelpad = 20

        # Configure secondary axis (Unique Documents)
        self.ax_secondary = self.ax_primary.twiny()
        secondary_color = "tab:green"
        self.ax_secondary.set_xlabel("Unique Documents")
        self.ax_secondary.xaxis.label.set_bbox(
            dict(facecolor=secondary_color, alpha=0.5, pad=5)
        )
        sns.histplot(
            self.secondary_data,
            bins=bins,
            kde=show_kde,
            ax=self.ax_secondary,
            color=secondary_color,
        )
        self.ax_secondary.tick_params(axis="x", labelcolor=secondary_color)
        self.ax_secondary.xaxis.labelpad = 20

        return self

    def add_percentile_annotations(
        self,
        percentiles=None,
        colors=None,
        linestyles=None,
        text_y_offset=0.1,
        include_zero_pmi=True,
    ):
        """
        Add vertical lines and text annotations for percentiles.

        Args:
            percentiles: Dictionary of percentile values or list of percentiles to calculate
            colors: Dictionary mapping percentiles to colors or default color
            linestyles: Dictionary mapping percentiles to linestyles or default style
            text_y_offset: Vertical offset factor for annotation text
            include_zero_pmi: Whether to include annotation for percentile where PMI equals 0

        Returns:
            Self for method chaining
        """
        if self.ax_primary is None:
            raise ValueError("Create plot first using create_dual_axis_plot()")

        # Calculate percentiles if not provided
        if percentiles is None:
            percentiles = [25, 50, 75, 90]

        if isinstance(percentiles, list):
            percentiles = self.calculate_percentiles(percentiles)

        # Add zero PMI percentile if requested
        if include_zero_pmi:
            zero_percentile = self.find_percentile_rank(0)
            # Only add if zero is within the range of data
            if 0 <= zero_percentile <= 100:
                percentiles["zero_pmi"] = 0

        # Default settings
        if colors is None:
            colors = {p: "red" for p in percentiles.keys()}
        elif isinstance(colors, str):
            colors = {p: colors for p in percentiles.keys()}

        if linestyles is None:
            linestyles = {p: "--" for p in percentiles.keys()}
        elif isinstance(linestyles, str):
            linestyles = {p: linestyles for p in percentiles.keys()}

        # Get y-axis limits for text positioning
        y_max = self.ax_primary.get_ylim()[1]

        # Add lines and annotations for each percentile
        for i, (percentile, value) in enumerate(percentiles.items()):
            color = colors.get(percentile, "rgb(76, 157, 142)")
            linestyle = linestyles.get(percentile, "--")

            # Add vertical line
            self.ax_primary.axvline(
                x=value, color=color, linestyle=linestyle, linewidth=1, alpha=0.7
            )

            # Add text annotation
            text_y = y_max * (1 - text_y_offset * (i + 1))

            # Special formatting for zero PMI
            if percentile == "zero_pmi":
                label = f"PMI=0 \n ({zero_percentile:.1f}th percentile)"
            else:
                label = f"{percentile}th percentile: \n{value:.3f}"

            self.ax_primary.text(
                value,
                text_y,
                label,
                color=color,
                fontweight="normal",
                fontsize=8,
                ha="center",
                bbox=dict(facecolor="white", alpha=0.7, boxstyle="round,pad=0.5"),
            )

        return self

    def adjust_layout(self, width_scale=1.0):
        """
        Adjust the layout of the plot.

        Args:
            width_scale: Scaling factor for the width (0-1)

        Returns:
            Self for method chaining
        """
        plt.subplots_adjust(bottom=0.15)
        self.fig.tight_layout(rect=[0, 0.05, width_scale, 0.95])
        return self

    def show(self):
        """Display the plot."""
        plt.show()
        return self


if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import sqlite3

    # Example database connection and data retrieval
    conn = sqlite3.connect("example.db")
    cursor = conn.cursor()

    # Example usage:
    cursor.execute(
        "SELECT npmi, uniq_docs FROM v_DIS_PNM_AGGR_ROW_FACTORY WHERE fq_doc_level > 30"
    )
    data = np.array(cursor.fetchall())

    visualizer = DualDistributionVisualizer(data)
    visualizer.create_dual_axis_plot().add_percentile_annotations(
        percentiles=[50, 75], colors="darkred", include_zero_pmi=True
    ).adjust_layout(width_scale=0.7).show()
