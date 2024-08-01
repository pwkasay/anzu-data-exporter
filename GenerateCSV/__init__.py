from GenerateCSV.main import *
import azure.functions as func
import logging


# deals = fetch_deals('2024-07-18','2024-07-20')

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
    initial_csv = req_body.get('initial_csv', None)
    final_csv = req_body.get('final_csv', None)

    if not start_date:
        # 3 months etc

    try:
        result =
        # return func.HttpResponse(result, status_code=200)
        if result.status_code == 200:
            return func.HttpResponse(f"Main 1 - Processed - {result.status_code}", status_code=200)
        elif result.status_code == 500:
            return func.HttpResponse(f"Main 0 Failed to Process - {result.get_body()} -{result.status_code}", status_code=500)
        else:
            return func.HttpResponse("Main 0 Failed to Process - ")
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)