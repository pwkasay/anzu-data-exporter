from main import *
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
#
# start_date = '2024-08-01'
# end_date = '2024-08-14'

    start_date = req.params.get('start_date', None)
    end_date = req.params.get('end_date', None)

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
