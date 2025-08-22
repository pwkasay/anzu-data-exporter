from main import *
import azure.functions as func
import logging
from error_handler import azure_function_error_handler


@azure_function_error_handler
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    # Get date parameters (defaults to last 3 months if not provided)
    start_date = req.params.get("start_date", None)
    end_date = req.params.get("end_date", None)

    # Generate the CSV in-memory
    csv_output, filename = export_csv(start_date, end_date)

    # Return the CSV as an HTTP response with the appropriate headers
    return func.HttpResponse(
        csv_output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
