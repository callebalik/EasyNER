"""
Provides blueprint routes to the flask app to show database characteristics.
Data is fetched mainly via db_statistics.py, which caches most results, to provide quick access
"""

from flask import Blueprint, render_template
from ...statistics.ne_doc_distr import Flowchart
from ...statistics.db_statistics import DBStatistics
from ...db_server import get_db_easyner_context_connection

stats = Blueprint("database_stats", __name__, template_folder="templates", static_folder="../../static", url_prefix="/statistics")

@stats.get("/")
def index():
    return render_template("stats_home.html")  # No need for prefixes

@stats.get("/data-flowchart")
def data_flowchart():
    """Show named entity distribution across documents as a flowchart"""
    with get_db_easyner_context_connection() as db:
        # Create DBStatistics instance
        db_stats = DBStatistics(
            conn=db.conn,
            cursor=db.cursor,
            logger=db.logger,
            data_exchanger=db.data_exchanger
        )

        # Initialize the Flowchart class with our statistics
        flowchart = Flowchart(db_stats)

        # Get tabular data
        # tabular_data = flowchart.get_tabular_data()

        # Get sankey diagram HTML and add diagnostic info
        sankey_html = flowchart.get_sankey_diagram()
        html_length = len(sankey_html) if sankey_html else 0
        diagnostic_html = f"""
        <div class="alert alert-info">
            <p>Sankey HTML generated: {html_length > 0}</p>
            <p>HTML length: {html_length} characters</p>
        </div>
        {sankey_html}
        """

        return render_template(
            "ne_doc_distr.html",
            # basic_stats=tabular_data["basic_stats"],
            sankey_html=sankey_html,  # Now matches template expectation
            title="Named Entity Document Distribution"
        )