import logging
import threading
from contextlib import contextmanager

from flask import render_template, request

from easyner.database.sqlite_backend.data_model.entity_cooccurrence import (
    EntityCooccurrence,
)
from easyner.database.sqlite_backend.statistics.sankey_diagram import CooccurenceSankey
from easyner.database.sqlite_backend.statistics.visualization_manager import (
    VisualizationManager,
)

logger = logging.getLogger("EasyNerDB")


def register_disease_phenomena_routes(
    app,
    get_db_simple_connection,
    visualization_manager: VisualizationManager,
) -> None:
    """Register disease phenomena routes with dependency injection."""

    # Add a helper context manager for safe database operations
    @contextmanager
    def safe_db_connection():
        """Context manager for safe database connection handling."""
        conn = None
        try:
            conn = get_db_simple_connection()
            yield conn
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            raise
        finally:
            if conn and hasattr(conn, "close"):
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

    @app.route("/disease-phenomena", endpoint="disease_phenomena.index")
    def disease_phenomena():
        """Display the Disease-Phenomenon associations page with Sankey diagram.
        Handles filtering by NPMI, frequency, and text search with proper validation.
        """
        try:
            logger.info(
                "Accessing disease-phenomena page",
                extra={
                    "request_id": request.environ.get("HTTP_X_REQUEST_ID", "-"),
                    "remote_addr": request.remote_addr,
                },
            )

            # Define default values
            DEFAULT_MIN_NPMI = 0
            DEFAULT_MIN_PMI = None
            DEFAULT_MIN_DOC_FQ = 5
            DEFAULT_LIMIT = 30

            # Get and validate filter parameters with detailed logging
            disease_search = request.args.get("disease_search", "").strip() or None
            phenomenon_search = (
                request.args.get("phenomenon_search", "").strip() or None
            )

            logger.debug(
                "Received search parameters",
                extra={
                    "disease_search": disease_search,
                    "phenomenon_search": phenomenon_search,
                },
            )

            # Get numeric filters with validation and logging
            min_npmi = request.args.get("min_npmi", DEFAULT_MIN_NPMI)
            max_npmi = request.args.get("max_npmi", None)
            min_pmi = request.args.get("min_pmi", DEFAULT_MIN_PMI)
            max_pmi = request.args.get("max_pmi", None)
            min_doc_fq = request.args.get("min_fq", DEFAULT_MIN_DOC_FQ)
            max_doc_fq = request.args.get("max_fq", None)
            min_uniq_docs = request.args.get("min_uniq_docs", None)
            max_uniq_docs = request.args.get("max_uniq_docs", None)

            logger.debug(
                "Filter parameters",
                extra={
                    "min_npmi": min_npmi,
                    "max_npmi": max_npmi,
                    "min_pmi": min_pmi,
                    "max_pmi": max_pmi,
                    "min_doc_fq": min_doc_fq,
                    "max_doc_fq": max_doc_fq,
                    "min_uniq_docs": min_uniq_docs,
                },
            )

            # Create and log cache key based on applied filters
            filters = []
            if disease_search:
                filters.append(f"d_{disease_search}")
            if phenomenon_search:
                filters.append(f"p_{phenomenon_search}")
            if min_pmi != DEFAULT_MIN_PMI:
                filters.append(f"minp_{min_pmi}")
            if max_pmi is not None:
                filters.append(f"maxp_{max_pmi}")
            if min_npmi != DEFAULT_MIN_NPMI:
                filters.append(f"minn_{min_npmi}")
            if max_npmi is not None:
                filters.append(f"maxn_{max_npmi}")
            if min_doc_fq != DEFAULT_MIN_DOC_FQ:
                filters.append(f"minf_{min_doc_fq}")
            if max_doc_fq is not None:
                filters.append(f"maxf_{max_doc_fq}")
            if min_uniq_docs is not None:
                filters.append(f"minu_{min_uniq_docs}")
            if max_uniq_docs is not None:
                filters.append(f"maxu_{max_uniq_docs}")

            cache_key = "sankey_" + ("base" if not filters else "_".join(filters))
            logger.debug(
                "Generated cache key",
                extra={"cache_key": cache_key, "filters": filters},
            )

            diagram_html = None
            try:

                def generate_sankey_visualization(_):
                    """Generate visualization with data fetched from the database."""
                    thread_id = threading.get_ident()
                    logger.debug(
                        f"Starting sankey diagram generation in thread {thread_id}",
                    )

                    try:
                        # Build filter dict for the data exchanger - standardize to lowercase
                        filter_dict = {}
                        if disease_search:
                            filter_dict["disease_like"] = f"%{disease_search}%"
                        if phenomenon_search:
                            filter_dict["phenomenon_like"] = f"%{phenomenon_search}%"
                        if max_pmi is not None:
                            filter_dict["max_pmi"] = (
                                float(max_pmi) if max_pmi != "" else None
                            )
                        if min_npmi is not None:
                            filter_dict["min_npmi"] = (
                                float(min_npmi) if min_npmi != "" else None
                            )
                        if max_npmi is not None:
                            filter_dict["max_npmi"] = (
                                float(max_npmi) if max_npmi != "" else None
                            )
                        if min_doc_fq is not None:
                            # Use lowercase column name that matches the database schema
                            filter_dict["min_fq_doc_level"] = (
                                float(min_doc_fq) if min_doc_fq != "" else None
                            )
                        if max_doc_fq is not None:
                            # Use lowercase column name that matches the database schema
                            filter_dict["max_fq_doc_level"] = (
                                float(max_doc_fq) if max_doc_fq != "" else None
                            )
                        if min_uniq_docs is not None:
                            filter_dict["min_uniq_docs"] = (
                                float(min_uniq_docs) if min_uniq_docs != "" else None
                            )
                        if max_uniq_docs is not None:
                            filter_dict["max_uniq_docs"] = (
                                float(max_uniq_docs) if max_uniq_docs != "" else None
                            )
                        logger.debug(
                            f"Filter dictionary for data exchange: {filter_dict}",
                        )

                        logger.debug(
                            f"Fetching cooccurrence data in thread {thread_id} with filters: {filter_dict}",
                        )
                        limit = int(request.args.get("limit", DEFAULT_LIMIT))

                        cursor = get_db_simple_connection().cursor()
                        cooccurrences = EntityCooccurrence.get_cooccurrences(
                            cursor=cursor,
                            filters=filter_dict,
                            limit=limit,
                            offset=0,
                        )

                        if not cooccurrences:
                            logger.warning(
                                f"No cooccurrences found with current filters in thread {thread_id}",
                            )
                            return """
                                <div style="text-align: center; padding: 20px;">
                                    <p>No data available for visualization with the current filters.</p>
                                </div>
                            """

                        logger.info(
                            f"Found {len(cooccurrences)} cooccurrences in thread {thread_id}",
                        )

                        # Create the visualization using the refactored class
                        sankey = CooccurenceSankey(logger=app.logger)
                        result = sankey.create_sankey_diagram(cooccurrences)
                        logger.info(
                            f"Sankey diagram generation complete in thread {thread_id}",
                        )
                        return result
                    except Exception as e:
                        logger.error(
                            f"Error in thread {thread_id}: {str(e)}",
                            exc_info=True,
                        )
                        return f"""
                            <div class="alert alert-danger">
                                Error generating Sankey diagram: {str(e)}
                            </div>
                        """

                # # Use the visualization manager
                # diagram_html = visualization_manager.get_cached_visualization(
                #     cache_key,
                #     get_db_simple_connection,
                #     generate_sankey_visualization
                # )
                # logger.info("Successfully retrieved/generated visualization", extra={'cache_key': cache_key})

            except Exception as e:
                logger.error(
                    "Error generating sankey diagram",
                    exc_info=True,
                    extra={"cache_key": cache_key, "error": str(e)},
                )
                diagram_html = f"""
                    <div class="alert alert-danger">
                        Error generating Sankey diagram: {str(e)}
                    </div>
                """

            # Pass defaults and render template
            defaults = {
                "min_pmi": DEFAULT_MIN_PMI,
                "min_npmi": DEFAULT_MIN_NPMI,
                "min_fq": DEFAULT_MIN_DOC_FQ,
                "limit": DEFAULT_LIMIT,
            }

            logger.debug(
                "Rendering template with parameters",
                extra={"defaults": defaults, "has_diagram": diagram_html is not None},
            )

            return render_template(
                "disease_phenomena.html",
                disease_search=disease_search or "",
                phenomenon_search=phenomenon_search or "",
                min_pmi=min_pmi,
                max_pmi=max_pmi or "",
                min_npmi=min_npmi,
                max_npmi=max_npmi,
                min_fq=min_doc_fq,
                max_fq=max_doc_fq or "",
                limit=DEFAULT_LIMIT,
                defaults=defaults,
                sankey_html=diagram_html,
            )
        except Exception as e:
            logger.error(
                "Error rendering disease phenomena page",
                exc_info=True,
                extra={
                    "error": str(e),
                    "request_path": request.path,
                    "request_args": dict(request.args),
                },
            )
            return (
                render_template(
                    "error.html",
                    error_code=500,
                    error_name="Internal Server Error",
                    error_description="Error loading disease phenomena page",
                    error_details=str(e) if app.debug else None,
                ),
                500,
            )

    # @app.route('/api/disease-phenomena', methods=['GET'])
    # def api_disease_phenomena():
    #     """Return raw disease-phenomena cooccurrence data in JSON format."""
    #     try:
    #         logger.info("API request received", extra={
    #             'request_id': request.environ.get('HTTP_X_REQUEST_ID', '-'),
    #             'remote_addr': request.remote_addr,
    #             'query_params': dict(request.args)
    #         })

    #         # Log filter parameters
    #         filter_params = {
    #             'disease_search': request.args.get('disease_search', '').strip() or None,
    #             'phenomenon_search': request.args.get('phenomenon_search', '').strip() or None,
    #             'min_pmi': request.args.get('min_pmi'),
    #             'max_pmi': request.args.get('max_pmi'),
    #             'min_npmi': request.args.get('min_npmi'),
    #             'max_npmi': request.args.get('max_npmi'),
    #             'min_doc_fq': request.args.get('min_fq'),
    #             'max_doc_fq': request.args.get('max_fq'),
    #             'min_uniq_docs': request.args.get('min_uniq_docs'),
    #             'limit': request.args.get('limit', 30)
    #         }

    #         logger.debug("Setting API filters", extra={'filters': filter_params})

    #         # Convert to proper filters dictionary - standardize to lowercase
    #         filter_dict = {}
    #         if filter_params['disease_search']:
    #             filter_dict['disease_like'] = f"%{filter_params['disease_search']}%"
    #         if filter_params['phenomenon_search']:
    #             filter_dict['phenomenon_like'] = f"%{filter_params['phenomenon_search']}%"
    #         if filter_params['min_pmi']:
    #             filter_dict['min_pmi'] = float(filter_params['min_pmi'])
    #         if filter_params['max_pmi']:
    #             filter_dict['max_pmi'] = float(filter_params['max_pmi'])
    #         if filter_params['min_npmi']:
    #             filter_dict['min_npmi'] = float(filter_params['min_npmi'])
    #         if filter_params['max_npmi']:
    #             filter_dict['max_npmi'] = float(filter_params['max_npmi'])
    #         if filter_params['min_doc_fq']:
    #             # Updated to use consistent lowercase column name
    #             filter_dict['min_fq_doc_level'] = float(filter_params['min_doc_fq'])
    #         if filter_params['max_doc_fq']:
    #             # Updated to use consistent lowercase column name
    #             filter_dict['max_fq_doc_level'] = float(filter_params['max_doc_fq'])
    #         if filter_params['min_uniq_docs']:
    #             filter_dict['min_uniq_docs'] = float(filter_params['min_uniq_docs'])
    #             limit = int(filter_params['limit'])

    #         # Fetch cooccurrences with filters
    #         cooccurrences = EntityCooccurrence.get_cooccurrences(
    #                         cursor=get_db_simple_connection().cursor(),
    #                         filters=filter_dict,
    #                         limit=limit,
    #                         offset=0
    #                     )
    #         result_count = len(cooccurrences) if cooccurrences else 0

    #         logger.info("Data fetch complete", extra={
    #             'result_count': result_count,
    #             'has_results': bool(cooccurrences)
    #         })

    #         if not cooccurrences:
    #             return jsonify({
    #                 "status": "success",
    #                 "data": [],
    #                 "message": "No data found matching the specified criteria."
    #             })

    #         # Convert cooccurrences to dict
    #         result = []
    #         for cooc in cooccurrences:
    #             try:
    #                 # Use to_dict method if available
    #                 if hasattr(cooc, 'to_dict') and callable(getattr(cooc, 'to_dict')):
    #                     result.append(cooc.to_dict())
    #                 else:
    #                     # Otherwise create a basic dict with essential properties
    #                     result.append({
    #                         "disease": cooc.disease,
    #                         "phenomenon": cooc.phenomenon,
    #                         "fq_document_level": cooc.fq_doc_level,
    #                         "fq_sentence_level": getattr(cooc, "fq_sent_level", 0),
    #                         "uniq_docs": cooc.uniq_docs,
    #                         "pmi": getattr(cooc, "pmi", 0),
    #                         "npmi": getattr(cooc, "npmi", 0),
    #                         "sent_dist": getattr(cooc, "sent_dist", 0)
    #                     })
    #             except AttributeError as ae:
    #                 # Log the specific missing attribute
    #                 logger.error(f"Missing attribute in cooccurrence: {str(ae)}", extra={
    #                     'cooccurrence': str(cooc)
    #                 })
    #                 # Continue with the next item
    #                 continue

    #         logger.info("API request successful", extra={
    #             'result_count': len(result),
    #             'response_size': sum(len(str(r)) for r in result)
    #         })

    #         return jsonify({"data": result})
    #     except AttributeError as e:
    #         logger.error("Data structure error in disease-phenomena API", exc_info=True, extra={
    #             'error_type': 'AttributeError',
    #             'error': str(e)
    #         })
    #         return jsonify({"error": "Data structure error", "details": str(e)}), 500
    #     except Exception as e:
    #         logger.error("Error in disease-phenomena API", exc_info=True, extra={
    #             'error_type': type(e).__name__,
    #             'error': str(e),
    #             'request_args': dict(request.args)
    #         })
    #         return jsonify({"error": "Server error", "details": str(e)}), 500

    # @app.route('/api/disease-phenomena/stats', methods=['GET'])
    # def api_disease_phenomena_stats():
    #     """Return statistics about disease-phenomena cooccurrences for range sliders."""
    #     try:
    #         logger.info("Stats API request received", extra={
    #             'request_id': request.environ.get('HTTP_X_REQUEST_ID', '-'),
    #             'remote_addr': request.remote_addr
    #         })

    #         try:
    #             cursor = get_db_simple_connection().cursor()

    #             # Use view name consistently - using `.name` property as it's a View object
    #             view_name = VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY.name

    #             # Get NPMI statistics - using lowercase column names consistently
    #             cursor.execute(f"""
    #                 SELECT
    #                     MIN(npmi) as min_npmi,
    #                     MAX(npmi) as max_npmi,
    #                     AVG(npmi) as avg_npmi,
    #                     COUNT(*) as total_count
    #                 FROM {view_name}
    #                 WHERE npmi IS NOT NULL
    #             """)
    #             # Store results in npmi_stats variable (this was missing)
    #             npmi_stats = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))

    #             # Get frequency statistics - using more robust percentile calculation
    #             cursor.execute(f"""
    #                 SELECT
    #                     MIN(fq_doc_level) as min_freq,
    #                     MAX(fq_doc_level) as max_freq,
    #                     AVG(fq_doc_level) as avg_freq
    #                 FROM {view_name}
    #             """)
    #             freq_stats = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))

    #             # Calculate percentiles in a separate query that's more resilient
    #             # This approach handles possible empty results better
    #             cursor.execute(f"""
    #                 SELECT COUNT(*) FROM {view_name}
    #             """)
    #             total_count = cursor.fetchone()[0]

    #             if total_count > 4:  # Only calculate percentiles if we have enough data
    #                 # Get 25th percentile
    #                 p25_offset = max(0, int(total_count * 0.25) - 1)
    #                 cursor.execute(f"""
    #                     SELECT fq_doc_level FROM {view_name}
    #                     ORDER BY fq_doc_level ASC
    #                     LIMIT 1 OFFSET {p25_offset}
    #                 """)
    #                 result = cursor.fetchone()
    #                 freq_stats['p25_freq'] = result[0] if result else freq_stats['min_freq']

    #                 # Get 50th percentile
    #                 p50_offset = max(0, int(total_count * 0.5) - 1)
    #                 cursor.execute(f"""
    #                     SELECT fq_doc_level FROM {view_name}
    #                     ORDER BY fq_doc_level ASC
    #                     LIMIT 1 OFFSET {p50_offset}
    #                 """)
    #                 result = cursor.fetchone()
    #                 freq_stats['p50_freq'] = result[0] if result else freq_stats['min_freq']

    #                 # Get 75th percentile
    #                 p75_offset = max(0, int(total_count * 0.75) - 1)
    #                 cursor.execute(f"""
    #                     SELECT fq_doc_level FROM {view_name}
    #                     ORDER BY fq_doc_level ASC
    #                     LIMIT 1 OFFSET {p75_offset}
    #                 """)
    #                 result = cursor.fetchone()
    #                 freq_stats['p75_freq'] = result[0] if result else freq_stats['max_freq']
    #             else:
    #                 # Not enough data for meaningful percentiles
    #                 freq_stats['p25_freq'] = freq_stats['min_freq']
    #                 freq_stats['p50_freq'] = freq_stats['min_freq']
    #                 freq_stats['p75_freq'] = freq_stats['max_freq']

    #             # Get entity counts - using lowercase column names
    #             cursor.execute(f"""
    #                 SELECT COUNT(DISTINCT e1_norm_id) as disease_count,
    #                     COUNT(DISTINCT e2_norm_id) as phenomenon_count
    #                 FROM {view_name}
    #             """)
    #             entity_counts = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))

    #             # Build complete statistics object
    #             stats = {
    #                 "npmi": npmi_stats,
    #                 "frequency": freq_stats,
    #                 "entity_counts": {
    #                     "diseases": entity_counts["disease_count"],
    #                     "phenomena": entity_counts["phenomenon_count"],
    #                     "total_relationships": npmi_stats["total_count"]
    #                 }
    #             }

    #             # Add useful presets for UI components
    #             stats["presets"] = {
    #                 "npmi": {
    #                     "positive_assoc": {"min": 0.3, "max": None},
    #                     "strong_assoc": {"min": 0.6, "max": None},
    #                     "negative_assoc": {"min": None, "max": -0.3},
    #                     "neutral": {"min": -0.2, "max": 0.2}
    #                 },
    #                 "frequency": {
    #                     "common": {"min": freq_stats['p75_freq'], "max": None},
    #                     "rare": {"min": None, "max": freq_stats['p25_freq']},
    #                     "moderate": {"min": freq_stats['p25_freq'], "max": freq_stats['p75_freq']}
    #                 }
    #             }

    #             logger.info("Statistics retrieved successfully", extra={
    #                 'npmi_range': f"{stats['npmi']['min_npmi']} to {stats['npmi']['max_npmi']}",
    #                 'freq_range': f"{stats['frequency']['min_freq']} to {stats['frequency']['max_freq']}"
    #             })

    #             return jsonify(stats)

    #         except Exception as e:
    #             logger.error("Error retrieving statistics", exc_info=True, extra={'error': str(e)})
    #             return jsonify({"error": "Failed to retrieve statistics", "details": str(e)}), 500

    #     except Exception as e:
    #         logger.error("Error in statistics API", exc_info=True, extra={'error': str(e)})
    #         return jsonify({"error": "Server error", "details": str(e)}), 500
