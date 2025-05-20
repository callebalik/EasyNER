# filepath: /home/carloa/Desktop/EasyNer/scripts/database/routes/cooccurrence_routes.py
from flask import render_template, request, jsonify
from ..statistics.cooccurrence_network import CooccurrenceNetwork


def register_cooccurrence_routes(app, get_db):
    """
    Register co-occurrence network routes with the Flask application.

    Args:
        app: Flask application instance
        get_db: Function to get database connection
    """

    @app.route("/cooccurrence-network")
    def cooccurrence_network_page():
        """Display the co-occurrence network page with filtering options."""
        try:
            return render_template("cooccurrence_network.html")
        except Exception as e:
            app.logger.error(f"Error loading co-occurrence network page: {e}")
            return (
                render_template(
                    "error.html", message="Error loading co-occurrence network page"
                ),
                500,
            )

    @app.route("/cooccurrence-network/generate")
    def generate_cooccurrence_network():
        """Generate a co-occurrence network with the specified parameters."""
        try:
            db = get_db()
            min_frequency = int(request.args.get("min_frequency", 10))
            entity1_type = request.args.get("entity1_type", "phenomenon")
            entity2_type = request.args.get("entity2_type", "disease")
            entity1_filter = request.args.get("entity1_filter", "")
            entity2_filter = request.args.get("entity2_filter", "")

            # Parse excluded entities
            excluded_entity1 = request.args.getlist("excluded_entity1[]")
            excluded_entity2 = request.args.getlist("excluded_entity2[]")

            # Generate the network visualization
            network = CooccurrenceNetwork(db)
            html_content = network.create_cooccurrence_network(
                min_frequency=min_frequency,
                entity1_type=entity1_type,
                entity2_type=entity2_type,
                entity1_filter=entity1_filter if entity1_filter else None,
                entity2_filter=entity2_filter if entity2_filter else None,
                excluded_entity1=excluded_entity1,
                excluded_entity2=excluded_entity2,
            )

            if html_content is None:
                return (
                    jsonify({"error": "No data available for the specified filters"}),
                    404,
                )

            return html_content

        except Exception as e:
            app.logger.error(f"Error generating co-occurrence network: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/cooccurrence-network/entities")
    def get_cooccurrence_entities():
        """Get available entities for filtering."""
        try:
            db = get_db()
            min_frequency = int(request.args.get("min_frequency", 5))
            entity1_type = request.args.get("entity1_type", "phenomenon")
            entity2_type = request.args.get("entity2_type", "disease")

            network = CooccurrenceNetwork(db)
            entities = network.get_available_entities(
                min_frequency=min_frequency,
                entity1_type=entity1_type,
                entity2_type=entity2_type,
            )

            return jsonify(entities)

        except Exception as e:
            app.logger.error(f"Error fetching co-occurrence entities: {e}")
            return jsonify({"error": str(e)}), 500
