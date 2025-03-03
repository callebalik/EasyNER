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
        with_dis_and_pnm = stats.documents_with_entities(included_ne_classes=["DIS", "PNM"])
        with_dis_only = stats.documents_with_entities(included_ne_classes=["DIS"], excluded_ne_classes=["PNM"])
        with_pnm_only = stats.documents_with_entities(included_ne_classes=["PNM"], excluded_ne_classes=["DIS"])

        # Validate counts
        assert total_docs == with_entities + without_entities, "Total documents count mismatch"
        print(with_entities - with_dis_only - with_dis_and_pnm - with_pnm_only)
        assert with_entities == with_dis_only + with_dis_and_pnm + with_pnm_only, "With entities count mismatch"

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
                "Documents with PNM only"
            ],
            "count": [
                total_docs,
                with_entities,
                without_entities,
                with_dis,
                with_pnm,
                with_dis_and_pnm,
                with_dis_only,
                with_pnm_only
            ]
        }

        # Create DataFrame
        df = pd.DataFrame(data)

        # Calculate percentage of total documents
        df['percentage'] = (df['count'] / total_docs * 100).round(2)

        # Store raw counts in a separate attribute (for backward compatibility)
        self.counts = {
            'total_docs': total_docs,
            'with_entities': with_entities,
            'without_entities': without_entities,
            'with_dis': with_dis,
            'with_pnm': with_pnm,
            'with_dis_pnm': with_dis_and_pnm,
            'with_dis_without_pnm': with_dis_only,
            'with_pnm_without_dis': with_pnm_only
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
            matching_rows = base_df[base_df['category'] == category_name]
            if matching_rows.empty:
                print(f"WARNING: Category '{category_name}' not found in data")
                return 0
            return matching_rows['count'].iloc[0]

        # Get required counts
        total_docs = get_count_for_category('Total Documents')
        with_entities = get_count_for_category('Documents with Named Entities')
        without_entities = get_count_for_category('Documents without Named Entities')
        with_dis_only = get_count_for_category('Documents with DIS only')
        with_pnm_only = get_count_for_category('Documents with PNM only')
        with_both = get_count_for_category('Documents with both DIS and PNM')

        # Verify data consistency
        expected_with_entities = with_dis_only + with_pnm_only + with_both
        if with_entities != expected_with_entities:
            print(f"WARNING: Documents with entities ({with_entities}) doesn't match sum of entity classes ({expected_with_entities})")

        # Create node records: layer, node name, count, percentage
        nodes = [
            # Layer 1 - Total Documents
            {
                'layer': 1,
                'node_name': 'Total Documents',
                'count': total_docs,
                'percentage': 100.0
            },
            # Layer 2 - With/Without Entities
            {
                'layer': 2,
                'node_name': 'Documents with Named Entities',
                'count': with_entities,
                'percentage': round((with_entities/total_docs) * 100, 2),
                'source': 'Total Documents'
            },
            {
                'layer': 2,
                'node_name': 'Documents without Named Entities',
                'count': without_entities,
                'percentage': round((without_entities/total_docs) * 100, 2),
                'source': 'Total Documents'
            },
            # Layer 3 - Entity Classes
            {
                'layer': 3,
                'node_name': 'DIS Only',
                'count': with_dis_only,
                'percentage': round((with_dis_only/total_docs) * 100, 2),
                'source': 'Documents with Named Entities'
            },
            {
                'layer': 3,
                'node_name': 'Both DIS and PNM',
                'count': with_both,
                'percentage': round((with_both/total_docs) * 100, 2),
                'source': 'Documents with Named Entities'
            },
            {
                'layer': 3,
                'node_name': 'PNM Only',
                'count': with_pnm_only,
                'percentage': round((with_pnm_only/total_docs) * 100, 2),
                'source': 'Documents with Named Entities'
            }
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
        for layer in df['layer'].unique():
            layer_df = df[df['layer'] == layer]
            layer_sum = layer_df['percentage'].sum()

            # Allow small floating-point error (0.1%)
            if not (99.9 <= layer_sum <= 100.1):
                print(f"WARNING: Layer {layer} percentages sum to {layer_sum:.2f}%, expected 100%")

            # For layers 2 and 3, also validate counts
            if layer > 1:
                source_nodes = df[df['layer'] == layer]['source'].unique()
                for source in source_nodes:
                    target_counts = df[(df['layer'] == layer) & (df['source'] == source)]['count'].sum()
                    source_count = df[(df['layer'] == layer-1) & (df['node_name'] == source)]['count'].iloc[0]

                    if target_counts != source_count:
                        print(f"WARNING: Flow from '{source}' ({source_count}) doesn't match sum of targets ({target_counts})")

    def get_sankey_data(self):
        """
        Convert the layered Sankey DataFrame to the format required for Plotly Sankey diagram.

        Returns:
            dict: Dictionary with source, target and value arrays
        """
        # Get the layered Sankey DataFrame
        df = self.create_sankey_layers_dataframe()

        # Initialize data structure for Sankey diagram
        sankey_data = {
            "source": [],
            "target": [],
            "value": []
        }

           # Add connections between layers using explicit source relationships
        for _, row in df.iterrows():
            if 'source' in row and pd.notna(row['source']):
                # Find the source node in our DataFrame
                source_node = row['source']
                target_node = row['node_name']

                sankey_data['source'].append(source_node)
                sankey_data['target'].append(target_node)
                sankey_data['value'].append(row['count'])  # Use 'count' field for value

        # Debug: Print connections to verify correctness
        print("\nSankey Diagram Connections:")
        for i, (src, tgt, val) in enumerate(zip(
                sankey_data['source'],
                sankey_data['target'],
                sankey_data['value'])):
            print(f"  {i+1}. {src} → {tgt}: {val:,}")

        return sankey_data

    def get_sankey_diagram(self) -> go.Figure:
        """
        Generate a Sankey diagram with external labels showing counts and percentages.
        Uses deterministic node ordering to ensure correct flow connections.

        Returns:
            go.Figure: Figure object of the Sankey diagram
        """
        try:
            # Get Sankey data and layered DataFrame
            sankey_data = self.get_sankey_data()
            layered_df = self.create_sankey_layers_dataframe()

            # Build ordered node list layer by layer for deterministic ordering
            ordered_nodes = []

            # Add nodes in layer order (1, 2, 3)
            for layer in sorted(layered_df['layer'].unique()):
                # Get nodes in this layer, sorted by their position within layer
                layer_df = layered_df[layered_df['layer'] == layer]
                layer_nodes = layer_df['node_name'].tolist()
                ordered_nodes.extend(layer_nodes)

            # Map nodes to indices in a deterministic order
            node_to_idx = {node: i for i, node in enumerate(ordered_nodes)}

            # Debug: Print the ordered nodes and their indices
            print("\nOrdered Node Mapping:")
            for node, idx in node_to_idx.items():
                node_info = layered_df[layered_df['node_name'] == node]
                layer = node_info['layer'].iloc[0] if not node_info.empty else "Unknown"
                print(f"  {idx}: Layer {layer} - {node}")

            # Convert sources and targets to indices using this ordered mapping
            source_idx = [node_to_idx[s] for s in sankey_data['source']]
            target_idx = [node_to_idx[t] for t in sankey_data['target']]

            # Debug: Print the connections with indices
            print("\nSankey Connection Indices:")
            for i, (src, tgt, src_idx, tgt_idx, val) in enumerate(zip(
                    sankey_data['source'],
                    sankey_data['target'],
                    source_idx,
                    target_idx,
                    sankey_data['value'])):
                print(f"  {i+1}. {src} ({src_idx}) → {tgt} ({tgt_idx}): {val:,}")

            # Define node positions and colors
            x_positions = []
            y_positions = []
            node_colors = []

            # Color scheme (keeping your existing colors)
            colors = {
                1: "#1f77b4",  # Blue for Total Documents
                2: ["#2ca02c", "#d62728"],  # Green for With Entities, Red for Without Entities
                3: ["#ff7f0e", "#9467bd", "#8c564b"]  # Orange, Purple, Brown for Entity Classes
            }

            # Calculate positions for each node in our ordered list
            for node in ordered_nodes:
                node_info = layered_df[layered_df['node_name'] == node]

                if node_info.empty:
                    # Fallback for unexpected nodes
                    x_positions.append(0.5)
                    y_positions.append(0.5)
                    node_colors.append("gray")
                    continue

                layer = node_info['layer'].iloc[0]

                # X position based on layer
                x_pos = 0.1 if layer == 1 else (0.5 if layer == 2 else 0.9)

                # Y position based on position within layer
                layer_nodes = layered_df[layered_df['layer'] == layer]['node_name'].tolist()
                node_idx = layer_nodes.index(node)
                layer_size = len(layer_nodes)

                # Calculate y position with spacing (keep your existing logic)
                if layer_size == 1:
                    y_pos = 0.5
                else:
                    spacing = 0.8 / (layer_size - 1) if layer_size > 1 else 0
                    y_pos = 0.1 + (node_idx * spacing)

                # Set node color (keep your existing color scheme)
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
            fig = go.Figure(data=[go.Sankey(
                textfont=dict(color="rgba(0,0,0,0)", size=1),  # Hide internal labels
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=ordered_nodes,  # Use our ordered node list here
                    x=x_positions,
                    y=y_positions,
                    color=node_colors
                ),
                link=dict(
                    source=source_idx,
                    target=target_idx,
                    value=sankey_data['value'],
                    color="rgba(100, 100, 100, 0.2)"  # Semi-transparent gray links
                )
            )])

            # The rest of your existing code follows unchanged...
            # Add layer labels at the top of the diagram
            layer_labels = {
                1: "Documents",
                2: "Named Entity Distribution",
                3: "Named Entity Subdivision"
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
                    yanchor="bottom"
                )

            # Add external labels with counts and percentages
            # Note: We need to iterate through ordered_nodes now, not unique_nodes
            for i, node in enumerate(ordered_nodes):
                node_info = layered_df[layered_df['node_name'] == node]

                if not node_info.empty:
                    count = node_info['count'].iloc[0]
                    percentage = node_info['percentage'].iloc[0]
                    layer = node_info['layer'].iloc[0]

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
                        align="center" if layer == 2 else ("right" if layer == 1 else "left"),
                        xanchor="center" if layer == 2 else ("right" if layer == 1 else "left")
                    )

            # Set layout properties
            fig.update_layout(
                title_text="Document Distribution by Named Entity Classes",
                font=dict(size=14, family="Arial"),
                paper_bgcolor='white',
                height=1000,
                width=1000,
                margin=dict(l=100, r=150, t=50, b=50)
            )

            return fig

        except Exception as e:
            # Return error message as figure with text
            print(f"Error generating Sankey diagram: {str(e)}")
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error generating Sankey diagram:<br>{str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14, color="red")
            )
            return fig

