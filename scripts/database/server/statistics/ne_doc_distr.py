"""
Get's statistics via DBStatistics
Data content:
documenents total -> with and without named entities -> named entity distribution, docs with class 1, docs with class 2, docs with both 1 and 2
"""
from ...db_statistics import DBStatistics
from ...db_server import get_db_easyner_context_connection
import plotly.graph_objects as go

class Flowchart:
    """
    Provides data for document distribution flowchart and tables based on named entities.
    """

    def __init__(self, db_statistics: DBStatistics):
        """Initialize with a DBStatistics instance"""
        self.db_stats = db_statistics
        self.counts = self.__get_data()

    # def get_entity_class_distribution(self):
    #     """Get distribution of documents by entity class"""
    #     # We'll implement this method in db_statistics.py
    #     return self.db_stats.get_entity_class_document_distribution()

    # def get_class_combinations(self, top_n=10):
    #     """Get most common entity class combinations in documents"""
    #     # We'll implement this method in db_statistics.py
    #     return self.db_stats.get_documents_with_entity_class_combinations().head(top_n)


    def __get_data(self):
        """Get raw data"""
        counts = self.db_stats.documents_entity_distribution

        return counts

    def get_tabular_data(self):
        """Get data formatted for table display"""

        counts = self.counts

        # Format for table display
        basic_stats = [
            {"metric": "Total Documents", "value": counts['total']},
            {"metric": "Documents with Named Entities", "value": counts['with_entities'], "percentage": f"{(counts['with_entities']/counts['total']*100):.2f}%"},
            {"metric": "Documents without Named Entities", "value": counts['without_entities'], "percentage": f"{(counts['without_entities']/counts['total']*100):.2f}%"}
        ]


        # distribution = self.get_entity_class_distribution()
        # # Entity class distribution data
        # class_stats = distribution.to_dict('records')

        return {
            "basic_stats": basic_stats,
            # "class_stats": class_stats
        }

    def get_sankey_diagram(self) -> str:
        """
        Render data as a sankey diagram with external labels and return html

        Returns:
            str: HTML for the Sankey diagram
        """
        # Get data from database statistics
        sankey_data = self.db_stats.get_doc_entity_distribution_data_for_sankey()
        counts = self.counts

        # Node configuration
        unique_labels = list(set(sankey_data['source'] + sankey_data['target']))
        label_to_idx = {label: i for i, label in enumerate(unique_labels)}

        # Convert data to indices
        source_idx = [label_to_idx[s] for s in sankey_data['source']]
        target_idx = [label_to_idx[t] for t in sankey_data['target']]

        # Better node positioning
        x_positions = [0.01, 0.98, 0.98]  # Align left and right edges
        y_positions = [0.5, 0.3, 0.7]    # Vertically spaced

        # Enhanced color scheme
        node_colors = ["steelblue", "forestgreen", "firebrick"]
        link_colors = ["rgba(173, 216, 230, 0.6)", "rgba(255, 160, 122, 0.6)"]

        # Create Sankey with invisible internal labels
        fig = go.Figure(data=[go.Sankey(
            textfont=dict(color="rgba(0,0,0,0)", size=1),
            node=dict(
                pad=30,
                thickness=25,
                line=dict(color="black", width=0.5),
                label=unique_labels,
                x=x_positions,
                y=y_positions,
                color=node_colors
            ),
            link=dict(
                source=source_idx,
                target=target_idx,
                value=sankey_data['value'],
                color=link_colors
            )
        )])

        # Clean layout with white background
        fig.update_layout(
            font=dict(family="Arial, sans-serif", size=12),
            paper_bgcolor='white',
            height=550,
            width=900,
            margin=dict(l=25, r=25, t=40, b=25)
        )

        # Extract document counts for labels
        total_docs = counts['total']
        with_entities = counts['with_entities']
        without_entities = counts['without_entities']
        with_entities_pct = round((with_entities/total_docs) * 100, 1)
        without_entities_pct = round((without_entities/total_docs) * 100, 1)

        # Total Documents annotation (left side)
        fig.add_annotation(dict(
            font=dict(color="black", size=16, family="Arial, sans-serif"),
            x=-0.04,  # Position left of node
            y=0.5,
            showarrow=False,
            text=f"<b>Total Documents</b>",
            align="right",
            xanchor="right"
        ))

        fig.add_annotation(dict(
            font=dict(color="steelblue", size=14),
            x=-0.04,  # Position left of node
            y=0.45,
            showarrow=False,
            text=f"<b>{total_docs:,}</b>",
            align="right",
            xanchor="right"
        ))

        # Documents with named entities (right top)
        fig.add_annotation(dict(
            font=dict(color="black", size=16, family="Arial, sans-serif"),
            x=1.04,  # Position right of node
            y=0.3,
            showarrow=False,
            text=f"<b>Documents with<br>Named Entities</b>",
            align="left",
            xanchor="left"
        ))

        fig.add_annotation(dict(
            font=dict(color="forestgreen", size=14),
            x=1.04,
            y=0.22,
            showarrow=False,
            text=f"<b>{with_entities:,}</b>",
            align="left",
            xanchor="left"
        ))

        fig.add_annotation(dict(
            font=dict(color="black", size=12),
            x=1.04,
            y=0.17,
            showarrow=False,
            text=f"{with_entities_pct}% of total",
            align="left",
            xanchor="left"
        ))

        # Documents without named entities (right bottom)
        fig.add_annotation(dict(
            font=dict(color="black", size=16, family="Arial, sans-serif"),
            x=1.04,  # Position right of node
            y=0.7,
            showarrow=False,
            text=f"<b>Documents without<br>Named Entities</b>",
            align="left",
            xanchor="left"
        ))

        fig.add_annotation(dict(
            font=dict(color="firebrick", size=14),
            x=1.04,
            y=0.62,
            showarrow=False,
            text=f"<b>{without_entities:,}</b>",
            align="left",
            xanchor="left"
        ))

        fig.add_annotation(dict(
            font=dict(color="black", size=12),
            x=1.04,
            y=0.57,
            showarrow=False,
            text=f"{without_entities_pct}% of total",
            align="left",
            xanchor="left"
        ))

        return fig.to_html(include_plotlyjs='cdn', full_html=False)