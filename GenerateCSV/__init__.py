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

    start_date = req.params.get('start_date', None)
    end_date = req.params.get('end_date', None)

    # Generate the CSV in-memory
    try:
        result = generate_keywords()
        # return func.HttpResponse(result, status_code=200)
        if result.status_code == 200:
            return func.HttpResponse(f"Main 1 - Processed - {result.status_code}", status_code=200)
        elif result.status_code == 500:
            return func.HttpResponse(f"Main 0 Failed to Process - {result.get_body()} -{result.status_code}",
                                     status_code=500)
        else:
            return func.HttpResponse("Main 0 Failed to Process - ")
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)