from typing import List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import ScalarFormatter


class DocumentMetricsVisualizer:
    """Class for visualizing document-level metrics distributions."""

    def __init__(self, data: np.ndarray, metric_name: str):
        """Initialize with document metric data.

        Args:
            data: Array of [doc_id, metric_value] pairs
            metric_name: Name of the metric (e.g., "word", "sentence")

        """
        self.data = data
        self.metric_name = metric_name
        self.doc_ids = data[:, 0]
        self.metric_values = data[:, 1].astype(float)  # Convert to numeric safely

        # Check for data validity
        self._validate_data()

    def _validate_data(self) -> None:
        """Validate data and print basic information."""
        print(f"Total documents: {len(self.metric_values)}")
        print(
            f"Value range: {np.min(self.metric_values)} - {np.max(self.metric_values)}",
        )
        print(f"Unique values: {len(np.unique(self.metric_values))}")

    def plot_histogram(
        self,
        color: tuple[float, float, float] = (197 / 255, 191 / 255, 191 / 255),
        figsize: tuple[int, int] = (7, 6),
        bins: int | list[float] | str = 100,
        show_stats: bool = True,
        show_std_dev: bool = True,
        show_quartiles: bool = False,  # New parameter for quartiles
        show_percentiles: list[int] | None = None,  # New parameter for percentiles
        x_min: float | None = None,
        x_max: float | None = None,
    ) -> None:
        """Create a histogram plot for the metric distribution.

        Args:
            color: RGB color tuple for bars
            figsize: Figure dimensions (width, height)
            bins: Number of bins or bin edges for histogram
            show_stats: Whether to show mean and median lines
            show_std_dev: Whether to show standard deviation lines
            show_quartiles: Whether to show quartile lines (Q1, Q3)
            show_percentiles: List of percentiles to show (e.g. [10, 90] for 10th and 90th percentiles)
            x_min: Minimum value for x-axis (None for auto)
            x_max: Maximum value for x-axis (None for auto)

        """
        # Calculate basic statistics
        mean_val = np.mean(self.metric_values)
        median_val = np.median(self.metric_values)
        std_dev_val = np.std(self.metric_values)

        # Create figure
        plt.figure(figsize=figsize)

        # Plot histogram
        plt.hist(
            self.metric_values, bins=bins, color=color, alpha=0.8, edgecolor="black",
        )

        # Configure axes
        plt.xlabel(f"Number of {self.metric_name.title()}s")
        plt.ylabel("Frequency (Number of Documents)")
        plt.grid(alpha=0.3, axis="y")

        # Set x-axis limits if provided
        if x_min is not None or x_max is not None:
            # Use current limits for any unspecified bound
            current_xlim = plt.xlim()
            x_min = x_min if x_min is not None else current_xlim[0]
            x_max = x_max if x_max is not None else current_xlim[1]
            plt.xlim(x_min, x_max)

        # Format y-axis to avoid scientific notation
        y_formatter = ScalarFormatter(useOffset=False)
        y_formatter.set_scientific(False)
        plt.gca().yaxis.set_major_formatter(y_formatter)

        # Add statistics reference lines
        if show_stats:
            plt.axvline(
                mean_val,
                color="red",
                linestyle="dashed",
                linewidth=1,
                label=f"Mean: {mean_val:.2f}",
            )
            plt.axvline(
                median_val,
                color="green",
                linestyle="dashed",
                linewidth=1,
                label=f"Median: {median_val:.2f}",
            )

            # Add standard deviation lines
            if show_std_dev:
                plt.axvline(
                    mean_val + std_dev_val,
                    color="blue",
                    linestyle="dashed",
                    linewidth=1,
                    label=f"Mean + StdDev: {(mean_val + std_dev_val):.2f}",
                )
                plt.axvline(
                    mean_val - std_dev_val,
                    color="blue",
                    linestyle="dashed",
                    linewidth=1,
                    label=f"Mean - StdDev: {(mean_val - std_dev_val):.2f}",
                )

            # Add quartile lines
            if show_quartiles:
                q1 = np.percentile(self.metric_values, 25)
                q3 = np.percentile(self.metric_values, 75)
                plt.axvline(
                    q1,
                    color="purple",
                    linestyle="dashed",
                    linewidth=1,
                    label=f"Q1 (25th): {q1:.2f}",
                )
                plt.axvline(
                    q3,
                    color="orange",
                    linestyle="dashed",
                    linewidth=1,
                    label=f"Q3 (75th): {q3:.2f}",
                )

            # Add custom percentile lines
            if show_percentiles:
                for p in show_percentiles:
                    p_val = np.percentile(self.metric_values, p)
                    plt.axvline(
                        p_val,
                        color="gray",
                        linestyle="dotted",
                        linewidth=1,
                        label=f"P{p}: {p_val:.2f}",
                    )

            plt.legend()

        plt.tight_layout()
        plt.show()
