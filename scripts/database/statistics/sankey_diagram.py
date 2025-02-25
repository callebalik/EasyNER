import plotly.graph_objects as go
import numpy as np
from ..db_main import EasyNerDBHandler

def generate_mellow_hsl(hue_range, count, saturation=30, lightness=70):
    """Generate mellow HSL colors for the diagram"""
    return [
        f"hsl({hue}, {saturation}%, {lightness}%)"
        for hue in np.linspace(hue_range[0], hue_range[1], count, dtype=int)
    ]

def create_disease_phenomena_sankey(db : EasyNerDBHandler):
    """Create a Sankey diagram for disease-phenomena relationships with customized edge and node properties"""
    try:
        # Query the view_disease_phenomena_summary
        query = """--sql
            SELECT disease, phenomenon, fq_document_level, pmi,
                fq_disease, fq_phenomenon,
                uniq_documents_disease, uniq_documents_phenomenon
            FROM filtered_view_disease_phenomena_summary
            ORDER BY fq_document_level DESC
            LIMIT 50
        """
        print("Executing query...")
        cursor = db.cursor.execute(query)
        data = cursor.fetchall()

        # Check if data is empty
        if not data:
            return """
                <div style="text-align: center; padding: 20px;">
                    <p>No data available for visualization. Please ensure there are disease-phenomena relationships with PMI > 9.</p>
                </div>
            """

        # Prepare data for Sankey diagram
        diseases_set = set()
        phenomena_set = set()
        disease_doc_counts = {} # Dictionary to store document counts for diseases
        phenomenon_doc_counts = {} # Dictionary to store document counts for phenomena

        for row in data:
            disease, phenomenon, fq_doc_level, pmi, fq_disease, fq_phenomenon, uniq_docs_disease, uniq_docs_phenomenon = row
            diseases_set.add(disease)
            phenomena_set.add(phenomenon)
            disease_doc_counts[disease] = uniq_docs_disease # Store document counts
            phenomenon_doc_counts[phenomenon] = uniq_docs_phenomenon # Store document counts

        diseases = sorted(list(diseases_set))
        phenomena = sorted(list(phenomena_set))
        label = diseases + phenomena

        # Generate color schemes with dynamic saturation based on document frequencies
        disease_colors = []
        phenomenon_colors = []

        # Get max counts for normalization
        max_disease_docs = max(disease_doc_counts.values()) if disease_doc_counts else 1
        max_phenomenon_docs = max(phenomenon_doc_counts.values()) if phenomenon_doc_counts else 1

        # Generate colors for diseases
        for disease in diseases:
            count = disease_doc_counts[disease]
            # Normalize count logarithmically and scale saturation to 20-80 range
            saturation = 20 + (np.log1p(count) / np.log1p(max_disease_docs)) * 60
            hue = 210  # blue-ish base for diseases
            disease_colors.append(f"hsl({hue}, {saturation}%, 70%)")

        # Generate colors for phenomena
        for phenomenon in phenomena:
            count = phenomenon_doc_counts[phenomenon]
            # Normalize count logarithmically and scale saturation to 20-80 range
            saturation = 20 + (np.log1p(count) / np.log1p(max_phenomenon_docs)) * 60
            hue = 30  # orange-ish base for phenomena
            phenomenon_colors.append(f"hsl({hue}, {saturation}%, 70%)")

        node_colors = disease_colors + phenomenon_colors

        # Create mappings from entity to index
        entity_to_index = {entity: i for i, entity in enumerate(label)}

        # Prepare link data
        source = []
        target = []
        value = []
        link_colors = []

        # Get max PMI and max fq_document_level for normalization
        max_pmi = max(row[3] for row in data) if data else 1 # Avoid division by zero if no data
        max_fq_doc_level = max(row[2] for row in data) if data else 1 # Avoid division by zero if no data


        for row in data:
            disease, phenomenon, fq_doc_level, pmi, fq_disease, fq_phenomenon, uniq_docs_disease, uniq_docs_phenomenon = row
            source.append(entity_to_index[disease])
            target.append(entity_to_index[phenomenon])

            # Normalize PMI value for edge width (value in Sankey)
            normalized_pmi = (pmi / max_pmi) * 100  # Scale to 0-100 range
            value.append(normalized_pmi)

            # Color intensity based on document frequency (fq_document_level)
            intensity = 0.3 + (fq_doc_level / max_fq_doc_level) * 0.7
            r, g, b = 22, 50, 80 # Base color grey
            link_colors.append(f"rgba({r},{g},{b},{intensity})") # Use intensity as opacity

        # Calculate node thicknesses using logarithmic normalization
        node_thicknesses = []
        for entity in label:
            if entity in disease_doc_counts:
                count = disease_doc_counts[entity]
            else:
                count = phenomenon_doc_counts[entity]
            # Add 1 to avoid log(0), then normalize to reasonable thickness range (10-50)
            thickness = (np.log1p(count) / np.log1p(max(max(disease_doc_counts.values()), max(phenomenon_doc_counts.values())))) * 40 + 10
            node_thicknesses.append(thickness)

        # Create Sankey diagram
        sankey = go.Sankey(
            link={
                "source": source,
                "target": target,
                "value": value,
                "color": link_colors
            },
            node={
                "pad": 10,
                "thickness": 20,  # Dynamic node thickness
                "line": {"color": "black", "width": 0.5},
                "label": label,
                "color": node_colors,
                "hovertemplate": "%{label}<br>Unique Documents: <br>Disease: %{customdata[0]}<br>Phenomenon: %{customdata[1]}<extra>Node Data</extra>",
                "customdata": [[disease_doc_counts.get(entity, 0), phenomenon_doc_counts.get(entity, 0)] for entity in label] # Pass document counts to tooltip
            }
        )

        # Create figure with layout
        fig = go.Figure(
            sankey,
            layout=dict(
                title="Disease-Phenomena Sankey Diagram",
                height=1472,
                width=950,
                font=dict(size=10)
            )
        )

        return fig.to_html(full_html=False)

    except Exception as e:
        raise Exception(f"Error generating Sankey diagram: {str(e)}")

if __name__ == "__main__":
    db = EasyNerDBHandler()
    fig = create_disease_phenomena_sankey(db)
    # Cache the figure for future use
    with open("sankey_diagram.html", "w") as f:
        f.write(fig)