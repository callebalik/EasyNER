# Add text labels

from typing import Any

import pandas as pd
import plotly.graph_objects as go

from easyner.database.sqlite_backend.db_statistics.color_scheme import (
    NODE_COLOR_BOTH_DIS_PNM,
    NODE_COLOR_DIS_ONLY,
    NODE_COLOR_PNM_ONLY,
    NODE_COLOR_TOTAL_DOCUMENTS,
)


class IcicleDataVisualizer:
    """Plotly's icicle plot with enhanced visibility for small segments."""

    DEFAULT_COLOR_SCHEME = {
        # Node-specific colors #Todo should not be part of class probably as it's to use-case specific.
        "node_colors": {
            "Total Documents": NODE_COLOR_TOTAL_DOCUMENTS,  # Light gray
            "Containing NE": "hsl(171, 18%, 63%)",  # Teal
            "Documents without Named Entities": "hsl(142, 6%, 35%)",  # Dark green
            "DIS Only": NODE_COLOR_DIS_ONLY,  # Reddish
            "PNM Only": NODE_COLOR_PNM_ONLY,  # Yellow/gold
            "DIS and PNM": NODE_COLOR_BOTH_DIS_PNM,
        },
        "default_color": "hsl(210, 8%, 75%)",  # Light blue-gray default
    }

    def __init__(self, plotly_data: dict[str, list], color_scheme: dict | None = None):
        """Initialize with pre-formatted data in plotly's expected format.

        Args:
            plotly_data: Dictionary containing 'names', 'parents', and 'values' lists
            color_scheme: Optional custom color mapping for nodes

        """
        self.plotly_data = plotly_data.copy()
        self.color_scheme = color_scheme if color_scheme else self.DEFAULT_COLOR_SCHEME

        # Create a dataframe for easier manipulation
        self.df = pd.DataFrame(
            {
                "node_name": self.plotly_data["names"],
                "source": self.plotly_data["parents"],
                "count": self.plotly_data["values"],
            },
        )

        # Build parent-child mapping for enhancement calculations
        self.parent_child_mapping = self._build_parent_child_mapping()

    def _build_parent_child_mapping(self) -> dict[str, list[str]]:
        """Build a mapping of parent nodes to their child nodes.

        Returns:
            Dictionary mapping parent node names to list of child node names

        """
        parent_child_map = {}

        for idx, row in self.df.iterrows():
            if pd.notna(row["source"]):
                parent_name = row["source"]
                if parent_name not in parent_child_map:
                    parent_child_map[parent_name] = []
                parent_child_map[parent_name].append(row["node_name"])

        return parent_child_map

    def _calculate_relative_percentages(self) -> pd.DataFrame:
        """Calculate percentages of each node relative to its parent.

        Returns:
            DataFrame with added percentage columns

        """
        df = self.df.copy()
        df["percentage"] = (df["count"] / df["count"].max()) * 100
        df["parent_relative_pct"] = df["percentage"].copy()

        # Calculate parent-relative percentages
        for idx, row in df.iterrows():
            if pd.notna(row["source"]):
                parent_name = row["source"]
                parent_data = df[df["node_name"] == parent_name]

                if not parent_data.empty:
                    parent_count = parent_data.iloc[0]["count"]
                    relative_pct = (row["count"] / parent_count) * 100
                    df.at[idx, "parent_relative_pct"] = relative_pct

        return df

    def create_vertical_icicle(
        self,
        title: str = "Document Distribution",
        enhance_small_segments: bool = True,
        min_segment_size: float = 3.0,
        show_color_bar: bool = False,
        enhancement_threshold_pct: float | None = None,
        text_size: int = 14,
        diagram_width: int = 600,
        diagram_height: int = 400,
    ) -> Any:
        """Create a vertical icicle plot with enhanced visibility for small segments.

        Args:
            title: Title for the plot
            enhance_small_segments: Whether to enhance visibility of small segments
            min_segment_size: Minimum visual size for small segments (in %)
            show_color_bar: Whether to display the color scale legend
            enhancement_threshold_pct: Threshold below which nodes are enhanced
            text_size: Font size for text labels
            diagram_width: Width of the diagram in pixels
            diagram_height: Height of the diagram in pixels

        Returns:
            Plotly figure object

        """
        # Calculate percentages for enhancement logic
        df = self._calculate_relative_percentages()
        enhanced_nodes = []

        # Create a copy of the original counts
        plot_data = self.plotly_data.copy()
        df["visual_count"] = df["count"].copy()

        # Apply visual enhancement for small segments if enabled
        if enhance_small_segments:
            # Use provided threshold or default to 3.0%
            threshold = (
                enhancement_threshold_pct
                if enhancement_threshold_pct is not None
                else 3.0
            )

            # Process each parent-children group
            for parent_name, child_names in self.parent_child_mapping.items():
                parent_data = df[df["node_name"] == parent_name]
                if parent_data.empty:
                    continue

                parent_count = parent_data.iloc[0]["count"]
                children_data = df[df["node_name"].isin(child_names)]

                # Identify children to enhance
                enhanced_children = []
                unchanged_children = []
                total_original_count = 0
                total_enhanced_count = 0

                for idx, child in children_data.iterrows():
                    child_name = child["node_name"]
                    relative_pct = child["parent_relative_pct"]
                    child_count = child["count"]

                    if relative_pct < threshold:
                        # Calculate minimum size based on parent's count
                        min_count = (min_segment_size * parent_count) / 100
                        enhanced_children.append((idx, child_name, min_count))
                        total_enhanced_count += min_count
                        enhanced_nodes.append(child_name)
                    else:
                        unchanged_children.append((idx, child_name, child_count))
                        total_original_count += child_count

                # Skip if no children need enhancement
                if not enhanced_children:
                    continue

                # Calculate scaling factor for unchanged children
                remaining_proportion = 1.0 - (total_enhanced_count / parent_count)

                if total_original_count > 0 and remaining_proportion > 0:
                    scaling_factor = (
                        parent_count * remaining_proportion
                    ) / total_original_count
                else:
                    scaling_factor = 1.0

                # Apply visual counts
                for idx, _, min_count in enhanced_children:
                    df.at[idx, "visual_count"] = min_count

                for idx, _, count in unchanged_children:
                    df.at[idx, "visual_count"] = count * scaling_factor

            # Update values in plot data with visual counts
            plot_data["values"] = df["visual_count"].tolist()
            # Keep original counts for hover display
            df["real_count"] = df["count"]
        else:
            # Use actual counts
            df["real_count"] = df["count"]

        # Create color list based on node names and color scheme with fallback
        colors = [self._get_node_color(name) for name in plot_data["names"]]

        # Create figure with explicit color assignment using graph_objects
        fig = go.Figure(
            go.Icicle(
                labels=plot_data["names"],
                parents=plot_data["parents"],
                values=plot_data["values"],
                branchvalues="total",
                marker=dict(colors=colors),
                customdata=list(
                    zip(
                        df["parent_relative_pct"].tolist(),
                        df["percentage"].tolist(),
                        df["real_count"].tolist(),
                        df["node_name"].tolist(),
                        strict=False,
                    ),
                ),
            ),
        )

        # Build hover template with enhancement indicator
        hover_template = (
            "<b>%{label}</b><br>"
            + "Count: %{customdata[2]:,}<br>"
            + "Percentage of Parent: %{customdata[0]:.2f}%<br>"
            + "Percentage of Total: %{customdata[1]:.2f}%"
        )

        # Add visual enhancement note if applicable
        if enhance_small_segments and enhanced_nodes:
            enhanced_nodes_str = repr(enhanced_nodes)
            hover_template += f"<br>{{%if customdata[3] in {enhanced_nodes_str}%}}<i>(Visually enhanced for clarity)</i>{{%endif%}}<extra></extra>"
        else:
            hover_template += "<extra></extra>"

        # Add text labels inside segments
        text_template = "<b>%{label}</b><br>%{customdata[2]:,} (%{customdata[0]:.1f}%)"

        # Apply templates
        fig.update_traces(
            hovertemplate=hover_template,
            texttemplate=text_template,
            textposition="middle center",
        )

        # Configure layout
        layout_settings = {
            "margin": dict(t=50, l=25, r=25, b=50),
            # "title": title,
        }

        # Control color bar visibility
        if not show_color_bar:
            layout_settings["coloraxis_showscale"] = False
            layout_settings["showlegend"] = False

        fig.update_layout(**layout_settings)
        return fig

    def _get_node_color(self, node_name: str) -> str:
        """Get color for a specific node, falling back to default if not defined.

        Args:
            node_name: Name of the node to get color for

        Returns:
            Color string for the node

        """
        node_colors = self.color_scheme.get("node_colors", {})
        default_color = self.color_scheme.get("default_color", "hsl(210, 8%, 75%)")

        return node_colors.get(node_name, default_color)
