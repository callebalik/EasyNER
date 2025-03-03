"""Handles creation of Sankey diagrams for disease-phenomena relationships."""
from ..data_model.entity_cooccurrence import Cooccurrence, DataExchanger
from ..data_model.schema import *
from ..db_main import EasyNerDBHandler
import plotly.graph_objects as go
import numpy as np
import logging

logger = logging.getLogger("EasyNerDB")

class CooccurenceSankey:
    """Class that handles the creation of Sankey diagrams for coocurrences, such as disease-phenomena relationships.
    This class encapsulates the visualization logic

    The class is designed to only handle visualization without any direct database access.
    Data should be provided as a list of Cooccurrence objects.
    """

    # Default values for filtering - kept for reference by importing modules
    DEFAULT_MIN_NPMI = 0
    DEFAULT_MIN_PMI = None
    DEFAULT_MIN_DOC_FQ = 5
    DEFAULT_LIMIT = 30

    def __init__(self, logger=None):
        """Initialize the Sankey diagram generator.

        Args:
            logger: Optional logger instance. If None, uses the module logger.
        """
        self.logger = logger or logging.getLogger("EasyNerDB")

    def create_sankey_diagram(self, cooccurrences: list[Cooccurrence]) -> go.Figure:
        """Create a Sankey diagram from the provided cooccurrence data.

        Args:
            cooccurrences: List of Cooccurrence objects

        Returns:
            HTML representation of the Sankey diagram
        """
        try:
            if not cooccurrences:
                return """
                    <div style="text-align: center; padding: 20px;">
                        <p>No data available for visualization with the current filters.</p>
                    </div>
                """

            processed_data = self._process_data(cooccurrences)
            color_data = self._create_node_colors(processed_data)
            link_data = self._create_link_data(processed_data)
            node_thicknesses = self._calculate_node_thicknesses(processed_data)

            # Get data for tooltips
            labels = processed_data['labels']
            e1_data = processed_data['disease_data']
            e2_data = processed_data['phenomenon_data']

            # Create customdata for node hover information
            customdata = []
            for entity in labels:
                if entity in e1_data:
                    customdata.append([
                        e1_data[entity][FQ_DOCUMENT_LEVEL],
                        e1_data[entity][UNIQ_DOCS]
                    ])
                else:
                    customdata.append([
                        e2_data[entity][FQ_DOCUMENT_LEVEL],
                        e2_data[entity][UNIQ_DOCS]
                    ])

            # Create Sankey diagram
            # Use the average of node thicknesses since Plotly expects a single number, not a list
            avg_thickness = sum(node_thicknesses) / len(node_thicknesses) if node_thicknesses else 20

            sankey = go.Sankey(
                link={
                    "source": link_data['source'],
                    "target": link_data['target'],
                    "value": link_data['value'],
                    "color": link_data['link_colors']
                },
                node={
                    "pad": 5,
                    "thickness": avg_thickness,  # Use average thickness instead of list
                    "line": {"color": "black", "width": 0.5},
                    "label": labels,
                    "color": color_data['node_colors'],
                    "hovertemplate": "%{label}<br>Document Frequency: %{customdata[0]}<br>Unique Documents: %{customdata[1]}<extra>Entity Data</extra>",
                    "customdata": customdata
                },
                textfont=dict(
                    family="Tahoma",
                    size=10,
                    color="black"
                ),
                arrangement="snap",
                orientation="h",
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=10
                )
            )

            # Create figure with layout
            fig = go.Figure(
                sankey,
                layout=dict(
                    height=1000,
                    width=800,
                    font=dict(size=10),
                    margin=dict(l=50, r=50, t=100, b=50),
                    annotations=[
                        dict(
                            x=-0.05, y=1.075,
                            xref="paper", yref="paper",
                            text="Diseases",
                            showarrow=False,
                            font=dict(
                                family="Tahoma",
                                size=16,
                                color="black"
                            ),
                            align="left",
                        ),
                        dict(
                            x=1.05, y=1.075,
                            xref="paper", yref="paper",
                            text="Phenomena",
                            showarrow=False,
                            font=dict(
                                family="Tahoma",
                                size=16,
                                color="black"
                            ),
                            align="left",
                        )
                    ]
                )
            )

            return fig

        except Exception as e:
            self.logger.error(f"Error generating Sankey diagram: {str(e)}", exc_info=True)
            return f"""
                <div style="text-align: center; padding: 20px; color: red;">
                    <p>Error generating visualization: {str(e)}</p>
                </div>
            """

    def get_sankey_html(self, cooccurrences: list[Cooccurrence]) -> str:
        """Get HTML representation of the Sankey diagram.

        Args:
            cooccurrences: List of Cooccurrence objects
        Returns:
            HTML representation of the Sankey diagram
        """
        fig = self.create_sankey_diagram(cooccurrences)
        return fig.to_html(full_html=False, include_plotlyjs='cdn')

    def export_as_html(self, cooccurrences: list[Cooccurrence], filename: str):
        """Export the Sankey diagram as an HTML file.

        Args:
            cooccurrences: List of Cooccurrence objects
            filename: Name of the HTML file to save
        """
        fig = self.create_sankey_diagram(cooccurrences)
        try:
            fig.write_html(filename)
        except Exception as e:
            self.logger.error(f"Error exporting Sankey diagram: {str(e)}", exc_info=True)

    def export_as_image(self, cooccurrences: list[Cooccurrence], filename: str = "sankey_diagram.png"):
        """Export the Sankey diagram as an image file.

        Args:
            cooccurrences: List of Cooccurrence objects
            filename: Name of the image file to save
        """
        fig = self.create_sankey_diagram(cooccurrences)
        try:
            fig.write_image(filename)
        except Exception as e:
            self.logger.error(f"Error exporting Sankey diagram: {str(e)}", exc_info=True)

    def _process_data(self, cooccurrences: list[Cooccurrence]):
        """Process Cooccurrence objects for Sankey diagram.

        Args:
            cooccurrences: List of Cooccurrence objects

        Returns:
            Processed data for Sankey diagram
        """
        if not cooccurrences:
            return None

        # Use dictionaries to track unique entities and their frequencies
        e1_data = {}  # Maps disease name to entity frequency data
        e2_data = {}  # Maps phenomenon name to entity frequency data


        # Build sets of unique diseases and phenomena
        for cooc in cooccurrences:
            # Access object properties directly instead of using dictionary access
            e1_txt = cooc.e1.txt if cooc.e1 else ""
            e2_txt = cooc.e2.txt if cooc.e2 else ""

            # Get entity specific frequency data from entity objects
            e1_fq = cooc.e1.fq if cooc.e1 and hasattr(cooc.e1, 'fq') else cooc.fq_doc_level
            e1_uniq_docs = cooc.e1.uniq_docs if cooc.e1 and hasattr(cooc.e1, 'uniq_docs') else cooc.uniq_docs
            e2_fq = cooc.e2.fq if cooc.e2 and hasattr(cooc.e2, 'fq') else cooc.fq_doc_level
            e2_uniq_docs = cooc.e2.uniq_docs if cooc.e2 and hasattr(cooc.e2, 'uniq_docs') else cooc.uniq_docs

            e1_data[e1_txt] = {
                FQ_DOCUMENT_LEVEL: e1_fq,
                UNIQ_DOCS: e1_uniq_docs
            }
            e2_data[e2_txt] = {
                FQ_DOCUMENT_LEVEL: e2_fq,
                UNIQ_DOCS: e2_uniq_docs
            }

        # Sort entity names for consistent display
        e1_dict = sorted(list(e1_data.keys()))
        e2_dict = sorted(list(e2_data.keys()))
        labels = e1_dict + e2_dict

        return {
            'cooccurrences': cooccurrences,
            'diseases': e1_dict,
            'phenomena': e2_dict,
            'labels': labels,
            'disease_data': e1_data,
            'phenomenon_data': e2_data
        }

    def _create_node_colors(self, processed_data: dict):
        """Create node colors based on document frequencies."""
        diseases = processed_data['diseases']
        phenomena = processed_data['phenomena']
        disease_data = processed_data['disease_data']
        phenomenon_data = processed_data['phenomenon_data']

        def _generate_colors(nodes, coloring_parameter: str, hue: int, saturation_range: tuple):
            """Generate colors for nodes based on a parameter."""
            colors = []
            max_count = max((data[coloring_parameter] for data in nodes.values()), default=1)

            for node in nodes:
                count = nodes[node][coloring_parameter]
                # Normalize count logarithmically and scale saturation
                # Ensure saturation doesn't exceed 100%
                saturation = min(saturation_range[1] + (np.log1p(count) / np.log1p(max_count)) * saturation_range[0], 100)
                colors.append(f"hsl({hue}, {saturation}%, 70%)")
            return colors

        disease_colors = _generate_colors(disease_data, UNIQ_DOCS, hue=210, saturation_range=(50, 70))
        phenomenon_colors = _generate_colors(phenomenon_data, FQ_DOCUMENT_LEVEL, hue=210, saturation_range=(50, 70))
        node_colors = disease_colors + phenomenon_colors

        return {
            'disease_colors': disease_colors,
            'phenomenon_colors': phenomenon_colors,
            'node_colors': node_colors
        }

    def _create_link_data(self, processed_data):
        """Create link data for the Sankey diagram."""
        cooccurrences = processed_data['cooccurrences']
        labels = processed_data['labels']

        # Create mappings from entity to index
        entity_to_index = {entity: i for i, entity in enumerate(labels)}

        # Prepare link data
        source = []
        target = []
        value = []
        link_colors = []

        # Get max frequency for normalization - ensure we never get None by using or 1
        max_fq_doc_level = max((getattr(cooc, 'fq_doc_level', 1) or 1 for cooc in cooccurrences), default=1)
        max_uniq_docs = max((getattr(cooc, 'uniq_docs', 1) or 1 for cooc in cooccurrences), default=1)

        # Create edges
        for cooc in cooccurrences:
            # Use object properties instead of dictionary access
            e1_txt = cooc.e1.txt if cooc.e1 else ""
            e2_txt = cooc.e2.txt if cooc.e2 else ""

            source.append(entity_to_index[e1_txt])
            target.append(entity_to_index[e2_txt])

            # Use normalized PMI if available, fallback to other metrics
            if hasattr(cooc, 'npmi') and cooc.npmi is not None:
                value.append(cooc.npmi)
            else:
                # Fallback to document frequency normalized between 0.1-1.0
                uniq_docs = getattr(cooc, 'uniq_docs', 1) or 1  # Ensure not None
                norm_value = 0.1 + (uniq_docs / max_uniq_docs) * 0.9
                value.append(norm_value)

            # Color intensity based on unique document count
            uniq_docs = getattr(cooc, 'uniq_docs', 1) or 1  # Ensure not None
            intensity = 0.3 + (uniq_docs / max_uniq_docs) * 0.7
            r, g, b = 22, 50, 80  # Base color grey
            link_colors.append(f"rgba({r},{g},{b},{intensity})")

        return {
            'source': source,
            'target': target,
            'value': value,
            'link_colors': link_colors
        }

    def _calculate_node_thicknesses(self, processed_data):
        """Calculate node thicknesses based on document counts."""
        e1_data = processed_data['disease_data']
        e2_data = processed_data['phenomenon_data']
        labels = processed_data['labels']

        # Find maximum document frequency for normalization
        max_disease_fq = max((data[FQ_DOCUMENT_LEVEL] for data in e1_data.values()), default=1)
        max_phenomenon_fq = max((data[FQ_DOCUMENT_LEVEL] for data in e2_data.values()), default=1)
        max_count = max(max_disease_fq, max_phenomenon_fq)

        node_thicknesses = []

        # Calculate thickness for each node
        for entity in labels:
            if entity in e1_data:
                count = e1_data[entity][FQ_DOCUMENT_LEVEL]
            else:
                count = e2_data[entity][FQ_DOCUMENT_LEVEL]

            # Add 1 to avoid log(0), then normalize to reasonable thickness range (10-50)
            thickness = (np.log1p(count) / np.log1p(max_count)) * 40 + 10
            node_thicknesses.append(thickness)

        return node_thicknesses


if __name__ == "__main__":
    # Example usage for quick testing
    db = EasyNerDBHandler()
    sankey = CooccurenceSankey()
    data_exchanger = DataExchanger(db)

    try:
        cooccurrences = data_exchanger.get_cooccurrences(limit=30)
        fig = sankey.create_sankey_diagram(cooccurrences)
        # Cache the figure for future use
        fig.wri
        with open("sankey_diagram.html", "w") as f:
            f.write(fig)
    except Exception as e:
        logger.error(f"Error generating test visualization: {str(e)}", exc_info=True)
    finally:
        if hasattr(db, 'close'):
            db.close()