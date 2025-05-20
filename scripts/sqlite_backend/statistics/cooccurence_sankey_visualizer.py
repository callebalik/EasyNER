import plotly.graph_objects as go
import numpy as np
import random
import colorsys
import re
from typing import List, Dict, Tuple, Any
from scripts.sqlite_backend.data_model.entity_cooccurrence import Cooccurrence
from scripts.sqlite_backend.statistics.color_scheme import (
    NODE_COLOR_DIS_ONLY,
    NODE_COLOR_PNM_ONLY,
    NODE_COLOR_BOTH_DIS_PNM,
    NODE_COLOR_DEFAULT,
    convert_color_to_rgb,
)


class CooccurrenceSankeyVisualizer:
    """
    A class for visualizing entity co-occurrences using Sankey diagrams.
    """

    def __init__(
        self,
        source_category_name="Disease",
        target_category_name="Phenomenon",
        source_base_color=None,
        target_base_color=None,
        link_opacity=0.6,
    ):
        """
        Initialize the Sankey diagram visualizer with category names and colors.

        Args:
            source_category_name: Name for the source category
            target_category_name: Name for the target category
            source_base_color: Base color for source nodes (defaults to DIS color)
            target_base_color: Base color for target nodes (defaults to PNM color)
            link_opacity: Opacity for link colors
        """
        self.source_category_name = source_category_name
        self.target_category_name = target_category_name

        # Use color scheme colors or fallback to defaults
        self.source_base_color = source_base_color or NODE_COLOR_DIS_ONLY
        self.target_base_color = target_base_color or NODE_COLOR_PNM_ONLY
        self.link_opacity = link_opacity
        self.figure = None

    def extract_unique_entities(self, source_entities, target_entities):
        """
        Extract unique entities while preserving their original order.

        Args:
            source_entities: List of source entity names
            target_entities: List of target entity names

        Returns:
            Tuple of (unique_sources, unique_targets)
        """
        unique_sources = []
        for entity in source_entities:
            if entity not in unique_sources:
                unique_sources.append(entity)

        unique_targets = []
        for entity in target_entities:
            if entity not in unique_targets:
                unique_targets.append(entity)

        return unique_sources, unique_targets

    def create_node_mapping(self, unique_sources, unique_targets):
        """
        Create mappings from entity names to node indices.

        Args:
            unique_sources: List of unique source entities
            unique_targets: List of unique target entities

        Returns:
            Tuple of (node_labels, source_indices, target_indices)
        """
        # Create node labels with categories
        node_labels = [f"{s} ({self.source_category_name})" for s in unique_sources] + [
            f"{t} ({self.target_category_name})" for t in unique_targets
        ]

        # Create index mappings
        source_indices = {s: i for i, s in enumerate(unique_sources)}
        target_indices = {
            t: i + len(unique_sources) for i, t in enumerate(unique_targets)
        }

        return node_labels, source_indices, target_indices

    def transform_links(self, links, source_indices, target_indices):
        """
        Transform links to use node indices instead of entity names.

        Args:
            links: List of dictionaries with 'source', 'target', and 'value' keys
            source_indices: Mapping of source names to indices
            target_indices: Mapping of target names to indices

        Returns:
            Tuple of (link_sources, link_targets, link_values)
        """
        link_sources = []
        link_targets = []
        link_values = []

        for link in links:
            source_name = link["source"]
            target_name = link["target"]

            source_idx = source_indices[source_name]
            target_idx = target_indices[target_name]

            link_sources.append(source_idx)
            link_targets.append(target_idx)
            link_values.append(link["value"])

        return link_sources, link_targets, link_values

    def _generate_color_gradient(
        self, base_color: str, count: int, randomize: bool = True
    ) -> List[str]:
        """
        Generate a gradient of colors based on the given base color.

        Args:
            base_color: The base color to create gradient from
            count: Number of colors to generate
            randomize: Whether to randomize the color order

        Returns:
            List of color strings in 'rgba(r,g,b,a)' format
        """
        # Convert base color to RGB
        rgb_color = convert_color_to_rgb(base_color)

        # Extract RGB components
        match = None
        if rgb_color.startswith("rgb("):
            match = re.match(r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", rgb_color)
        elif rgb_color.startswith("rgba("):
            match = re.match(
                r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\s*\)",
                rgb_color,
            )

        if not match:
            # Fallback to a default color if parsing fails
            return [NODE_COLOR_DEFAULT] * count

        # Get RGB values
        r = int(match.group(1)) / 255.0
        g = int(match.group(2)) / 255.0
        b = int(match.group(3)) / 255.0

        # Convert to HSL for better gradient generation
        h, l, s = colorsys.rgb_to_hls(r, g, b)

        colors = []
        for i in range(count):
            # Vary lightness and saturation to create gradient
            # Keep hue the same to maintain the base color's character
            ratio = (i + 1) / (count + 1)  # avoid extremes (0 and 1)

            # Adjust lightness and saturation
            new_l = max(0.2, min(0.8, l + (ratio - 0.5) * 0.3))
            new_s = max(0.3, min(0.9, s * (0.85 + ratio * 0.3)))

            # Convert back to RGB
            new_r, new_g, new_b = colorsys.hls_to_rgb(h, new_l, new_s)

            # Create rgba string
            colors.append(
                f"rgba({int(new_r * 255)}, {int(new_g * 255)}, {int(new_b * 255)}, 0.8)"
            )

        # Randomize color order if requested
        if randomize:
            random.shuffle(colors)

        return colors

    def _mix_colors(
        self, source_color: str, target_color: str, mix_ratio: float = 0.5
    ) -> str:
        """
        Mix two colors together with the given ratio.

        Args:
            source_color: Source color string in rgba format
            target_color: Target color string in rgba format
            mix_ratio: Mixing ratio (0 = all source, 1 = all target)

        Returns:
            Mixed color in rgba format
        """
        # Parse source color
        source_match = re.match(
            r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\s*\)", source_color
        )
        if not source_match:
            source_match = re.match(
                r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", source_color
            )
            if source_match:
                sr, sg, sb = map(
                    int,
                    [
                        source_match.group(1),
                        source_match.group(2),
                        source_match.group(3),
                    ],
                )
                sa = 1.0
            else:
                sr, sg, sb, sa = 128, 128, 128, self.link_opacity  # Default gray
        else:
            sr, sg, sb, sa = map(
                float,
                [
                    source_match.group(1),
                    source_match.group(2),
                    source_match.group(3),
                    source_match.group(4),
                ],
            )
            sr, sg, sb = int(sr), int(sg), int(sb)

        # Parse target color
        target_match = re.match(
            r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\s*\)", target_color
        )
        if not target_match:
            target_match = re.match(
                r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", target_color
            )
            if target_match:
                tr, tg, tb = map(
                    int,
                    [
                        target_match.group(1),
                        target_match.group(2),
                        target_match.group(3),
                    ],
                )
                ta = 1.0
            else:
                tr, tg, tb, ta = 128, 128, 128, self.link_opacity  # Default gray
        else:
            tr, tg, tb, ta = map(
                float,
                [
                    target_match.group(1),
                    target_match.group(2),
                    target_match.group(3),
                    target_match.group(4),
                ],
            )
            tr, tg, tb = int(tr), int(tg), int(tb)

        # Mix colors
        mr = int(sr * (1 - mix_ratio) + tr * mix_ratio)
        mg = int(sg * (1 - mix_ratio) + tg * mix_ratio)
        mb = int(sb * (1 - mix_ratio) + tb * mix_ratio)
        ma = self.link_opacity  # Fixed opacity for links

        return f"rgba({mr}, {mg}, {mb}, {ma})"

    def create_diagram(self, source_entities, target_entities, links, title=None):
        """
        Create a Sankey diagram visualization from the provided data.

        Args:
            source_entities: List of source entity names
            target_entities: List of target entity names
            links: List of dictionaries with 'source', 'target', and 'value' keys
            title: Optional title for the diagram

        Returns:
            self (for method chaining)
        """
        # Process data
        unique_sources, unique_targets = self.extract_unique_entities(
            source_entities, target_entities
        )
        node_labels, source_indices, target_indices = self.create_node_mapping(
            unique_sources, unique_targets
        )
        link_sources, link_targets, link_values = self.transform_links(
            links, source_indices, target_indices
        )

        # Generate color gradients for source and target nodes
        source_colors = self._generate_color_gradient(
            self.source_base_color, len(unique_sources)
        )
        target_colors = self._generate_color_gradient(
            self.target_base_color, len(unique_targets)
        )

        # Combine node colors
        node_colors = source_colors + target_colors

        # Generate link colors by mixing source and target colors
        link_colors = []
        for i in range(len(link_sources)):
            source_idx = link_sources[i]
            target_idx = link_targets[i]

            # The target index needs to be adjusted to get the correct color from node_colors
            source_color = source_colors[source_idx]
            target_color = target_colors[target_idx - len(unique_sources)]

            # Add some randomization to the mix ratio
            mix_ratio = 0.3 + random.random() * 0.4  # Between 0.3 and 0.7

            link_colors.append(self._mix_colors(source_color, target_color, mix_ratio))

        # Create the Sankey diagram
        self.figure = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=node_labels,
                        color=node_colors,
                    ),
                    link=dict(
                        source=link_sources,
                        target=link_targets,
                        value=link_values,
                        color=link_colors,
                        hovertemplate="%{source.label} → %{target.label}<br>NPMI: %{value:.4f}<extra></extra>",
                    ),
                )
            ]
        )

        # Update layout
        diagram_title = (
            title
            if title
            else f"{self.source_category_name}-{self.target_category_name} Co-occurrence Network"
        )
        self.figure.update_layout(
            title_text=diagram_title,
            font=dict(size=12),
            autosize=True,
            height=600,
            margin=dict(t=60, l=20, r=20, b=20),
        )

        return self

    def customize_layout(self, **layout_params):
        """
        Customize the layout of the diagram with additional parameters.

        Args:
            **layout_params: Keyword arguments to pass to update_layout()

        Returns:
            self (for method chaining)
        """
        if self.figure is not None:
            self.figure.update_layout(**layout_params)
        return self

    def display(self):
        """
        Display the Sankey diagram.

        Returns:
            self (for method chaining)
        """
        if self.figure is not None:
            self.figure.show()
        return self  # Return self for method chaining instead of self.figure

    def save(self, filepath, format=None):
        """
        Save the diagram to a file.

        Args:
            filepath: Path where to save the figure
            format: Optional file format

        Returns:
            self (for method chaining)
        """
        if self.figure is not None:
            self.figure.write_image(filepath, format=format)
        else:
            raise ValueError(
                "The figure has not been created. Please create the diagram before saving."
            )
        return self

    def create_diagram_from_cooccurrences(
        self, cooccurrences: list[Cooccurrence], title=None
    ):
        """
        Create a Sankey diagram visualization directly from cooccurrence objects.

        Args:
            cooccurrences: List of cooccurrence objects from the database
            title: Optional title for the diagram

        Returns:
            self (for method chaining)
        """
        # Extract diseases (e1) and phenomena (e2)
        diseases = []
        phenomena = []
        links = []

        for co in cooccurrences:
            diseases.append(co.e1.txt)
            phenomena.append(co.e2.txt)
            links.append({"source": co.e1.txt, "target": co.e2.txt, "value": co.npmi})

        # Now call the existing create_diagram method with extracted data
        return self.create_diagram(
            source_entities=diseases,
            target_entities=phenomena,
            links=links,
            title=title,
        )
