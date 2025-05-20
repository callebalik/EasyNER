"""
Get's statistics via DBStatistics
Data content:
documenents total -> with and without named entities -> named entity distribution, docs with class 1, docs with class 2, docs with both 1 and 2
"""

from .db_statistics import DBStatistics
import pandas as pd
import plotly.graph_objects as go


class Flowchart:
    """
    Provides data for document distribution flowchart and tables based on named entities.
    """

    def __init__(self, db_statistics: DBStatistics):
        """Initialize with a DBStatistics instance"""
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
        """
        Get raw document distribution data as a properly formatted DataFrame.

        Returns:
            pd.DataFrame: DataFrame with documents distribution statistics and percentages
        """
        stats = self.db_stats
        total_docs = stats.document_count

        # Collect document counts
        with_entities = stats.documents_with_entities()

        without_entities = total_docs - with_entities
        with_dis = stats.documents_with_entities(included_ne_classes=["DIS"])
        with_pnm = stats.documents_with_entities(included_ne_classes=["PNM"])
        with_dis_and_pnm = stats.documents_with_entities(
            included_ne_classes=["DIS", "PNM"]
        )
        with_dis_only = stats.documents_with_entities(
            included_ne_classes=["DIS"], excluded_ne_classes=["PNM"]
        )
        with_pnm_only = stats.documents_with_entities(
            included_ne_classes=["PNM"], excluded_ne_classes=["DIS"]
        )

        # Validate counts
        assert (
            total_docs == with_entities + without_entities
        ), "Total documents count mismatch"
        print(with_entities - with_dis_only - with_dis_and_pnm - with_pnm_only)
        assert (
            with_entities == with_dis_only + with_dis_and_pnm + with_pnm_only
        ), "With entities count mismatch"

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
        """
        Transform the base data DataFrame into a structured three-layer Sankey diagram DataFrame.

        Returns:
            pd.DataFrame: DataFrame with node and layer information for Sankey diagram
        """
        base_df = self.data

        # Extract values using proper column names and safe access patterns
        def get_count_for_category(category_name):
            """Helper function to safely extract counts from base DataFrame"""
            matching_rows = base_df[base_df["category"] == category_name]
            if matching_rows.empty:
                print(f"WARNING: Category '{category_name}' not found in data")
                return 0
            return matching_rows["count"].iloc[0]

        # Get required counts
        total_docs = get_count_for_category("Total Documents")
        with_entities = get_count_for_category("Documents with Named Entities")
        without_entities = get_count_for_category("Documents without Named Entities")
        with_dis_only = get_count_for_category("Documents with DIS only")
        with_pnm_only = get_count_for_category("Documents with PNM only")
        with_both = get_count_for_category("Documents with both DIS and PNM")

        # Verify data consistency
        expected_with_entities = with_dis_only + with_pnm_only + with_both
        if with_entities != expected_with_entities:
            print(
                f"WARNING: Documents with entities ({with_entities}) doesn't match sum of entity classes ({expected_with_entities})"
            )

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
                "node_name": "Documents without Named Entities",
                "count": without_entities,
                "percentage": round((without_entities / total_docs) * 100, 2),
                "source": "Total Documents",
            },
            {
                "layer": 2,
                "node_name": "Documents with Named Entities",
                "count": with_entities,
                "percentage": round((with_entities / total_docs) * 100, 2),
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
        """
        Validate that percentages within each layer sum to approximately 100%.

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
                    f"WARNING: Layer {layer} percentages sum to {layer_sum:.2f}%, expected 100%"
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
                            f"WARNING: Flow from '{source}' ({source_count}) doesn't match sum of targets ({target_counts})"
                        )

    def get_sankey_data(self) -> dict:
        """
        Convert the layered Sankey DataFrame to the format required for Plotly Sankey diagram.

        Returns:
            dict: Dictionary with source, target and value arrays
        """
        # Get the layered Sankey DataFrame
        df = self.create_sankey_layers_dataframe()

        # Initialize data structure for Sankey diagram
        sankey_data = {"source": [], "target": [], "value": []}

        # Add connections between layers using explicit source relationships
        for _, row in df.iterrows():
            if "source" in row and pd.notna(row["source"]):
                # Find the source node in our DataFrame
                source_node = row["source"]
                target_node = row["node_name"]

                sankey_data["source"].append(source_node)
                sankey_data["target"].append(target_node)
                sankey_data["value"].append(row["percentage"])

        # Debug: Print connections to verify correctness
        print("\nSankey Diagram Connections:")
        for i, (src, tgt, val) in enumerate(
            zip(sankey_data["source"], sankey_data["target"], sankey_data["value"])
        ):
            print(f"  {i+1}. {src} → {tgt}: {val:,}")

        return sankey_data

    def render_sankey_diagram(self) -> go.Figure:
        """
        Create a Plotly Sankey diagram with custom node and link colors.

        Returns:
            go.Figure: Plotly figure object for the Sankey diagram
        """
        # Get the data in the required format for Sankey diagram
        data = self.get_sankey_data()

        # Get the layered Sankey DataFrame for node information
        sankey_df = self.create_sankey_layers_dataframe()

        # Get unique node names for labeling
        all_nodes = list(set(data["source"] + data["target"]))

        # Create node indices mapping (required by Plotly)
        node_indices = {node: i for i, node in enumerate(all_nodes)}

        # Convert source and target names to indices
        source_indices = [node_indices[src] for src in data["source"]]
        target_indices = [node_indices[tgt] for tgt in data["target"]]

        # Prepare enhanced node labels and colors
        node_colors = []
        node_labels = []

        for node in all_nodes:
            # Get the node's data from the DataFrame
            node_data = sankey_df[sankey_df["node_name"] == node]

            if not node_data.empty:
                # Create enhanced label with count information
                count = node_data["count"].iloc[0]
                percentage = node_data["percentage"].iloc[0]
                label = f"{node}<br>{count:,} ({percentage:.1f}%)"

                # Define colors based on specific node names rather than layer
                if node == "Total Documents":
                    node_colors.append(
                        "hsl(0, 5%, 76%)"
                    )  # Light gray for Total Documents
                elif node == "Documents with Named Entities":
                    node_colors.append(
                        "hsl(171, 18%, 63%)"
                    )  # Tan/gold for With Named Entities
                elif node == "Documents without Named Entities":
                    node_colors.append(
                        "hsl(142, 6%, 35%)"
                    )  # Same as "with named entities" from previous version
                elif node == "DIS Only":
                    node_colors.append(
                        "hsl(12, 48%, 43%)"
                    )  # More intense pink/red for DIS Only
                elif node == "PNM Only":
                    node_colors.append("hsl(38, 100%, 68%)")  # Light green for PNM Only
                elif node == "Both DIS and PNM":
                    # Create a blended color between the new DIS and PNM
                    node_colors.append(
                        "hsl(230, 55%, 65%)"
                    )  # Mix of intense pink and light green
                else:
                    node_colors.append("rgba(150, 150, 150, 0.8)")  # Default gray
            else:
                # Fallback for any nodes not in the dataframe
                label = node
                node_colors.append("rgba(150, 150, 150, 0.8)")  # Default gray

            node_labels.append(label)

        # Prepare custom link colors - update to match new node colors
        link_colors = []
        for src, tgt in zip(data["source"], data["target"]):
            if src == "Total Documents" and tgt == "Documents with Named Entities":
                link_colors.append("hsl(171, 18%, 63%)")  # Lighter version of tan/gold
            elif src == "Total Documents" and tgt == "Documents without Named Entities":
                link_colors.append(
                    "rgba(204, 168, 108, 0.5)"
                )  # Now matching "with named entities" link
            elif src == "Total Documents" and tgt == "PNM Only":
                link_colors.append("hsl(145, 7%, 78%)")
            else:
                # Default colors for other links (can be adjusted as needed)
                if tgt == "DIS Only":
                    link_colors.append(
                        "hsl(16, 41%, 58%)"
                    )  # Light version of intense DIS
                elif tgt == "Both DIS and PNM":
                    link_colors.append(
                        "rgba(140, 150, 210, 0.4)"
                    )  # Matching the mixed color
                elif tgt == "PNM Only":
                    link_colors.append("hsl(44, 60%, 58%)")  # Light green, matching PNM
                else:
                    link_colors.append("hsl(171, 17%, 60%)")  # Light gray

        # Create figure with Sankey diagram and custom link colors
        fig = go.Figure(
            data=[
                go.Sankey(
                    arrangement="perpendicular",  # Use 'snap' for better layout balance
                    node=dict(
                        pad=40,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=node_labels,  # Use enhanced labels with counts
                        color=node_colors,
                    ),
                    link=dict(
                        source=source_indices,
                        target=target_indices,
                        value=data["value"],
                        color=link_colors,  # Add custom link colors
                        hovertemplate="%{source.label} → %{target.label}: %{value:.2f}%<extra></extra>",
                    ),
                )
            ]
        )

        # Configure layout
        fig.update_layout(
            # title_text="<b>Document Distribution</b><br>by Named Entities",
            font_size=14,
            autosize=False,
            height=600,
            width=1000,
            margin=dict(l=100, r=100, t=120, b=20),
        )

        return fig
