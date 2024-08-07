from GenerateCSV.main import *
import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    print("Main triggered")
    # If not parameters given - run for 3 months
    # If parameters given - accept
    # Add flags to email out csvs at the two steps or not
    # Initial csv
    # Final csv

    req_body = req.get_json()
    start_date = req_body.get('start_date', None)
    end_date = req_body.get('end_date', None)

    # Generate the CSV in-memory
    try:
        csv_output, filename = export_csv(start_date, end_date)
        # Return the CSV as an HTTP response with the appropriate headers
        return func.HttpResponse(
            csv_output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)