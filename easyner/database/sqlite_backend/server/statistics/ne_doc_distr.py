"""Get's statistics via DBStatistics.

Data content:
documenents total -> with and without named entities -> named entity distribution, docs with class 1, docs with class 2, docs with both 1 and 2.
"""

import pandas as pd
import plotly.graph_objects as go
from db_statistics import DBStatistics

from ...statistics.color_scheme import (
    LINK_COLOR_DEFAULT,
    LINK_COLOR_TOTAL_TO_PNM_ONLY,
    LINK_COLOR_TOTAL_TO_WITH_ENTITIES,
    LINK_COLOR_TOTAL_TO_WITHOUT_ENTITIES,
    LINK_COLOR_WITH_ENTITIES_TO_BOTH,
    LINK_COLOR_WITH_ENTITIES_TO_DIS_ONLY,
    LINK_COLOR_WITH_ENTITIES_TO_PNM_ONLY,
    NODE_COLOR_BOTH_DIS_PNM,
    NODE_COLOR_DEFAULT,
    NODE_COLOR_DIS_ONLY,
    NODE_COLOR_PNM_ONLY,
    NODE_COLOR_TOTAL_DOCUMENTS,
    NODE_COLOR_WITH_ENTITIES,
    NODE_COLOR_WITHOUT_ENTITIES,
)


class Flowchart:
    """Provides data for document distribution flowchart and tables based on named entities."""

    def __init__(self, db_statistics: DBStatistics):
        """Initialize with a DBStatistics instance."""
        self.db_stats = db_statistics
        self.data = self._get_data_()

    # def get_entity_class_distribution(self):
    #     """Get distribution of documents by entity class"""
    #     # We'll implement this method in db_statistics.py
    #     return self.db_stats.get_entity_class_document_distribution()

    # def get_class_combinations(self, top_n=10):
    #     """Get most common entity class combinations in documents"""
    #     # We'll implement this method in db_statistics.py
    #     return self.db_stats.get_documents_with_entity_class_combinations().head(top_n)

    def _get_data_(self) -> pd.DataFrame:
        """Get raw document distribution data as a properly formatted DataFrame.

        Returns:
            pd.DataFrame: DataFrame with documents distribution statistics and percentages

        """
        stats = self.db_stats
        total_docs = stats.document_count

        # Collect document counts
        with_entities = stats.documents_with_entities()
        without_entities = total_docs - with_entities
        with_dis = stats.documents_with_entities(included_ne_class_id="DIS")
        with_pnm = stats.documents_with_entities(included_ne_class_id="PNM")
        with_dis_and_pnm = stats.get_entity_cooccurrence_count
        with_dis_only = stats.documents_with_entities(
            included_ne_class_id="DIS",
            excluded_ne_class_id="PNM",
        )
        with_pnm_only = stats.documents_with_entities(
            included_ne_class_id="PNM",
            excluded_ne_class_id="DIS",
        )

        # Create a structured DataFrame
        data = {
            "category": [
                "Total Documents",
                "Documents with Named Entities",
                "Documents without Named Entities",
                "Documents with DIS Entities",
                "Documents with PNM Entities",
                "Documents with both DIS and PNM",
                "Documents with DIS only",
                "Documents with PNM only",
            ],
            "count": [
                total_docs,
                with_entities,
                without_entities,
                with_dis,
                with_pnm,
                with_dis_and_pnm,
                with_dis_only,
                with_pnm_only,
            ],
        }

        # Create DataFrame
        df = pd.DataFrame(data)

        # Calculate percentage of total documents
        df["percentage"] = (df["count"] / total_docs * 100).round(2)

        # Store raw counts in a separate attribute (for backward compatibility)
        self.counts = {
            "total_docs": total_docs,
            "with_entities": with_entities,
            "without_entities": without_entities,
            "with_dis": with_dis,
            "with_pnm": with_pnm,
            "with_dis_pnm": with_dis_and_pnm,
            "with_dis_without_pnm": with_dis_only,
            "with_pnm_without_dis": with_pnm_only,
        }

        return df

    # def get_tabular_data(self):
    #     """Get data formatted for table display"""

    #     counts = self._get_data_()

    #     # Format for table display
    #     basic_stats = [
    #         {"metric": "Total Documents", "value": counts['total_docs']},
    #         {"metric": "Documents with Named Entities", "value": counts['with_entities'], "percentage": f"{(counts['with_entities']/counts['total_docs']*100):.2f}%"},
    #         {"metric": "Documents without Named Entities", "value": counts['without_entities'], "percentage": f"{(counts['without_entities']/counts['total_docs']*100):.2f}%"}
    #     ]

    #     if hasattr(self.db_stats, 'get_entity_class_distribution'):
    #         class_distribution = self.db_stats.get_entity_class_distribution().to_dict('records')
    #         basic_stats.extend(class_distribution)

    #     # Create a dictionary for the final output
    #     return {
    #         "basic_stats": basic_stats,
    #         "class_stats": class_distribution if 'class_distribution' in locals() else []
    #     }

    def create_sankey_layers_dataframe(self):
        """Transform the base data DataFrame into a structured three-layer Sankey diagram DataFrame.

        Returns:
            pd.DataFrame: DataFrame with node and layer information for Sankey diagram

        """
        base_df = self.data
        total_docs = base_df[base_df["category"] == "Total Documents"]["count"].iloc[0]

        # Extract values using more robust DataFrame filtering
        with_entities = base_df[base_df["category"] == "Documents with Named Entities"][
            "count"
        ].iloc[0]
        without_entities = base_df[
            base_df["category"] == "Documents without Named Entities"
        ]["count"].iloc[0]
        with_dis_only = base_df[base_df["category"] == "Documents with DIS only"][
            "count"
        ].iloc[0]
        with_pnm_only = base_df[base_df["category"] == "Documents with PNM only"][
            "count"
        ].iloc[0]
        with_both = base_df[base_df["category"] == "Documents with both DIS and PNM"][
            "count"
        ].iloc[0]

        # Create node records: layer, node name, count, percentage
        nodes = [
            # Layer 1 - Total Documents
            {
                "layer": 1,
                "node_name": "Total Documents",
                "count": total_docs,
                "percentage": 100.0,
            },
            # Layer 2 - With/Without Entities
            {
                "layer": 2,
                "node_name": "Documents with Named Entities",
                "count": with_entities,
                "percentage": round((with_entities / total_docs) * 100, 2),
                "source": "Total Documents",
            },
            {
                "layer": 2,
                "node_name": "Documents without Named Entities",
                "count": without_entities,
                "percentage": round((without_entities / total_docs) * 100, 2),
                "source": "Total Documents",
            },
            # Layer 3 - Entity Classes
            {
                "layer": 3,
                "node_name": "DIS Only",
                "count": with_dis_only,
                "percentage": round((with_dis_only / total_docs) * 100, 2),
                "source": "Documents with Named Entities",
            },
            {
                "layer": 3,
                "node_name": "Both DIS and PNM",
                "count": with_both,
                "percentage": round((with_both / total_docs) * 100, 2),
                "source": "Documents with Named Entities",
            },
            {
                "layer": 3,
                "node_name": "PNM Only",
                "count": with_pnm_only,
                "percentage": round((with_pnm_only / total_docs) * 100, 2),
                "source": "Documents with Named Entities",
            },
        ]

        # Create DataFrame from nodes
        sankey_df = pd.DataFrame(nodes)

        # Validate layer percentages
        self._validate_layer_percentages(sankey_df)

        return sankey_df

    def _validate_layer_percentages(self, df):
        """Validate that percentages within each layer sum to approximately 100%.

        Args:
            df (pd.DataFrame): DataFrame with layer and percentage columns

        Raises:
            Warning: If percentages don't sum close to 100%

        """
        for layer in df["layer"].unique():
            layer_df = df[df["layer"] == layer]
            layer_sum = layer_df["percentage"].sum()

            # Allow small floating-point error (0.1%)
            if not (99.9 <= layer_sum <= 100.1):
                print(
                    f"WARNING: Layer {layer} percentages sum to {layer_sum:.2f}%, expected 100%",
                )

            # For layers 2 and 3, also validate counts
            if layer > 1:
                source_nodes = df[df["layer"] == layer]["source"].unique()
                for source in source_nodes:
                    target_counts = df[
                        (df["layer"] == layer) & (df["source"] == source)
                    ]["count"].sum()
                    source_count = df[
                        (df["layer"] == layer - 1) & (df["node_name"] == source)
                    ]["count"].iloc[0]

                    if target_counts != source_count:
                        print(
                            f"WARNING: Flow from '{source}' ({source_count}) doesn't match sum of targets ({target_counts})",
                        )

    def get_sankey_data(self):
        """Convert the layered Sankey DataFrame to the format required for Plotly Sankey diagram.

        Returns:
            dict: Dictionary with source, target and value arrays

        """
        # Get the layered Sankey DataFrame
        df = self.create_sankey_layers_dataframe()

        # Initialize data structure for Sankey diagram
        sankey_data = {"source": [], "target": [], "value": []}

        # Add connections between layers
        for _, row in df.iterrows():
            if "source" in row and pd.notna(row["source"]):
                sankey_data["source"].append(row["source"])
                sankey_data["target"].append(row["node_name"])
                sankey_data["value"].append(row["count"])

        return sankey_data

    def get_sankey_diagram(self):
        """Generate a Sankey diagram with external labels showing counts and percentages.

        Returns:
            str: HTML representation of the Sankey diagram

        """
        try:
            # Get Sankey data and layered DataFrame
            sankey_data = self.get_sankey_data()
            layered_df = self.create_sankey_layers_dataframe()

            # Get unique node names
            unique_nodes = list(set(sankey_data["source"] + sankey_data["target"]))
            node_to_idx = {node: i for i, node in enumerate(unique_nodes)}

            # Convert sources and targets to indices
            source_idx = [node_to_idx[s] for s in sankey_data["source"]]
            target_idx = [node_to_idx[t] for t in sankey_data["target"]]

            # Define node positions and colors
            x_positions = []
            y_positions = []
            node_colors = []

            # Color scheme
            colors = {
                1: "#1f77b4",  # Blue for Total Documents
                2: [
                    "#2ca02c",
                    "#d62728",
                ],  # Green for With Entities, Red for Without Entities
                3: [
                    "#ff7f0e",
                    "#9467bd",
                    "#8c564b",
                ],  # Orange, Purple, Brown for Entity Classes
            }

            # Calculate positions for each node
            for node in unique_nodes:
                node_info = layered_df[layered_df["node_name"] == node]

                if node_info.empty:
                    # Fallback for unexpected nodes
                    x_positions.append(0.5)
                    y_positions.append(0.5)
                    node_colors.append("gray")
                    continue

                layer = node_info["layer"].iloc[0]

                # X position based on layer
                x_pos = 0.1 if layer == 1 else (0.5 if layer == 2 else 0.9)

                # Y position based on position within layer
                layer_nodes = layered_df[layered_df["layer"] == layer][
                    "node_name"
                ].tolist()
                node_idx = layer_nodes.index(node)
                layer_size = len(layer_nodes)

                # Calculate y position with spacing
                if layer_size == 1:
                    y_pos = 0.5
                else:
                    spacing = 0.8 / (layer_size - 1) if layer_size > 1 else 0
                    y_pos = 0.1 + (node_idx * spacing)

                # Set node color
                if layer == 1:
                    color = colors[1]
                elif layer == 2:
                    color = colors[2][node_idx % len(colors[2])]
                else:  # layer 3
                    color = colors[3][node_idx % len(colors[3])]

                x_positions.append(x_pos)
                y_positions.append(y_pos)
                node_colors.append(color)

            # Create Sankey diagram with invisible internal labels
            fig = go.Figure(
                data=[
                    go.Sankey(
                        textfont=dict(
                            color="rgba(0,0,0,0)",
                            size=1,
                        ),  # Hide internal labels
                        node=dict(
                            pad=15,
                            thickness=20,
                            line=dict(color="black", width=0.5),
                            label=unique_nodes,
                            x=x_positions,
                            y=y_positions,
                            color=node_colors,
                        ),
                        link=dict(
                            source=source_idx,
                            target=target_idx,
                            value=sankey_data["value"],
                            color="rgba(100, 100, 100, 0.2)",  # Semi-transparent gray links
                        ),
                    ),
                ],
            )

            # Add layer labels at the top of the diagram
            layer_labels = {
                1: "Documents",
                2: "Named Entity Distribution",
                3: "Named Entity Subdivision",
            }

            for layer, label in layer_labels.items():
                x_pos = 0.1 if layer == 1 else (0.5 if layer == 2 else 0.9)
                fig.add_annotation(
                    x=x_pos,
                    y=1.05,  # Position above the chart
                    text=f"<b>{label}</b>",
                    showarrow=False,
                    font=dict(size=14),
                    align="center",
                    xanchor="center",
                    yanchor="bottom",
                )

            # Add external labels with counts and percentages
            for i, node in enumerate(unique_nodes):
                node_info = layered_df[layered_df["node_name"] == node]

                if not node_info.empty:
                    count = node_info["count"].iloc[0]
                    percentage = node_info["percentage"].iloc[0]
                    layer = node_info["layer"].iloc[0]

                    # Position annotation based on layer
                    x_offset = -0.05 if layer == 1 else (0 if layer == 2 else 0.05)

                    # Create text with node name, count and percentage
                    text = f"<b>{node}</b><br>{count:,} ({percentage:.1f}%)"

                    # Add annotation
                    fig.add_annotation(
                        x=x_positions[i] + x_offset,
                        y=y_positions[i],
                        text=text,
                        showarrow=False,
                        font=dict(size=12),
                        align=(
                            "center"
                            if layer == 2
                            else ("right" if layer == 1 else "left")
                        ),
                        xanchor=(
                            "center"
                            if layer == 2
                            else ("right" if layer == 1 else "left")
                        ),
                    )

            # Set layout properties
            fig.update_layout(
                title_text="Document Distribution by Named Entity Classes",
                font=dict(size=14, family="Arial"),
                paper_bgcolor="white",
                height=600,
                width=900,
                margin=dict(l=50, r=50, t=50, b=50),
            )

            # Return HTML representation
            return fig.to_html(include_plotlyjs="cdn", full_html=False)

        except Exception as e:
            # Return error message as HTML
            return f"""
            <div class="alert alert-danger">
              <h4>Error generating Sankey diagram</h4>
              <p>{str(e)}</p>
            </div>
            """

    def render_sankey_diagram(self) -> go.Figure:
        """Render the Sankey diagram using the color scheme module variables.

        Returns:
            go.Figure: Plotly Figure object representing the Sankey diagram

        """
        # Get Sankey data and layered DataFrame
        sankey_data = self.get_sankey_data()
        layered_df = self.create_sankey_layers_dataframe()

        # Get unique node names
        unique_nodes = list(set(sankey_data["source"] + sankey_data["target"]))
        node_to_idx = {node: i for i, node in enumerate(unique_nodes)}

        # Convert sources and targets to indices
        source_idx = [node_to_idx[s] for s in sankey_data["source"]]
        target_idx = [node_to_idx[t] for t in sankey_data["target"]]

        # Define node positions and colors
        x_positions = []
        y_positions = []
        node_colors = []
        node_labels = []

        # Calculate positions for each node
        for node in unique_nodes:
            node_info = layered_df[layered_df["node_name"] == node]

            if node_info.empty:
                # Fallback for unexpected nodes
                x_positions.append(0.5)
                y_positions.append(0.5)
                node_colors.append(NODE_COLOR_DEFAULT)
                node_labels.append(node)
                continue

            layer = node_info["layer"].iloc[0]

            # X position based on layer
            x_pos = 0.1 if layer == 1 else (0.5 if layer == 2 else 0.9)

            # Y position based on position within layer
            layer_nodes = layered_df[layered_df["layer"] == layer]["node_name"].tolist()
            node_idx = layer_nodes.index(node)
            layer_size = len(layer_nodes)

            # Calculate y position with spacing
            if layer_size == 1:
                y_pos = 0.5
            else:
                spacing = 0.8 / (layer_size - 1) if layer_size > 1 else 0
                y_pos = 0.1 + (node_idx * spacing)

            # Set node color
            if node == "Total Documents":
                node_colors.append(NODE_COLOR_TOTAL_DOCUMENTS)
            elif node == "Documents with Named Entities":
                node_colors.append(NODE_COLOR_WITH_ENTITIES)
            elif node == "Documents without Named Entities":
                node_colors.append(NODE_COLOR_WITHOUT_ENTITIES)
            elif node == "DIS Only":
                node_colors.append(NODE_COLOR_DIS_ONLY)
            elif node == "PNM Only":
                node_colors.append(NODE_COLOR_PNM_ONLY)
            elif node == "Both DIS and PNM":
                node_colors.append(NODE_COLOR_BOTH_DIS_PNM)
            else:
                node_colors.append(NODE_COLOR_DEFAULT)

            count = node_info["count"].iloc[0]
            percentage = node_info["percentage"].iloc[0]
            label = f"{node}<br>{count:,} ({percentage:.1f}%)"
            node_labels.append(label)

            x_positions.append(x_pos)
            y_positions.append(y_pos)

        # Prepare custom link colors
        link_colors = []
        for src, tgt in zip(sankey_data["source"], sankey_data["target"], strict=False):
            if src == "Total Documents" and tgt == "Documents with Named Entities":
                link_colors.append(LINK_COLOR_TOTAL_TO_WITH_ENTITIES)
            elif src == "Total Documents" and tgt == "Documents without Named Entities":
                link_colors.append(LINK_COLOR_TOTAL_TO_WITHOUT_ENTITIES)
            elif src == "Total Documents" and tgt == "PNM Only":
                link_colors.append(LINK_COLOR_TOTAL_TO_PNM_ONLY)
            elif tgt == "DIS Only":
                link_colors.append(LINK_COLOR_WITH_ENTITIES_TO_DIS_ONLY)
            elif tgt == "Both DIS and PNM":
                link_colors.append(LINK_COLOR_WITH_ENTITIES_TO_BOTH)
            elif tgt == "PNM Only":
                link_colors.append(LINK_COLOR_WITH_ENTITIES_TO_PNM_ONLY)
            else:
                link_colors.append(LINK_COLOR_DEFAULT)

        # Create Sankey diagram with invisible internal labels
        fig = go.Figure(
            data=[
                go.Sankey(
                    textfont=dict(
                        color="rgba(0,0,0,0)",
                        size=1,
                    ),  # Hide internal labels
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=node_labels,
                        x=x_positions,
                        y=y_positions,
                        color=node_colors,
                    ),
                    link=dict(
                        source=source_idx,
                        target=target_idx,
                        value=sankey_data["value"],
                        color=link_colors,
                    ),
                ),
            ],
        )

        # Add layer labels at the top of the diagram
        layer_labels = {
            1: "Documents",
            2: "Named Entity Distribution",
            3: "Named Entity Subdivision",
        }

        for layer, label in layer_labels.items():
            x_pos = 0.1 if layer == 1 else (0.5 if layer == 2 else 0.9)
            fig.add_annotation(
                x=x_pos,
                y=1.05,  # Position above the chart
                text=f"<b>{label}</b>",
                showarrow=False,
                font=dict(size=14),
                align="center",
                xanchor="center",
                yanchor="bottom",
            )

        # Add external labels with counts and percentages
        for i, node in enumerate(unique_nodes):
            node_info = layered_df[layered_df["node_name"] == node]

            if not node_info.empty:
                count = node_info["count"].iloc[0]
                percentage = node_info["percentage"].iloc[0]
                layer = node_info["layer"].iloc[0]

                # Position annotation based on layer
                x_offset = -0.05 if layer == 1 else (0 if layer == 2 else 0.05)

                # Create text with node name, count and percentage
                text = f"<b>{node}</b><br>{count:,} ({percentage:.1f}%)"

                # Add annotation
                fig.add_annotation(
                    x=x_positions[i] + x_offset,
                    y=y_positions[i],
                    text=text,
                    showarrow=False,
                    font=dict(size=12),
                    align=(
                        "center" if layer == 2 else ("right" if layer == 1 else "left")
                    ),
                    xanchor=(
                        "center" if layer == 2 else ("right" if layer == 1 else "left")
                    ),
                )

        # Set layout properties
        fig.update_layout(
            title_text="Document Distribution by Named Entity Classes",
            font=dict(size=14, family="Arial"),
            paper_bgcolor="white",
            height=600,
            width=900,
            margin=dict(l=50, r=50, t=50, b=50),
        )

        return fig
