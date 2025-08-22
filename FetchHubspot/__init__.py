from main import *
import azure.functions as func
import logging
import asyncio
from error_handler import azure_function_error_handler


@azure_function_error_handler
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    start_date = req.params.get("start_date", None)
    end_date = req.params.get("end_date", None)

    # Fetch deals with basic info
    deals = fetch_deals(start_date=start_date, end_date=end_date)

    # Attach owner details
    deals = fetch_and_attach_owner_details(deals, "hubspot_owner_id")
    deals = fetch_and_attach_owner_details(deals, "team_member_1")

    # Fetch notes, attachments, and engagements asynchronously
    deals = asyncio.run(fetch_notes_attachments_and_engagements(deals))

    json_response = json.dumps(deals)
    return func.HttpResponse(
        json_response, status_code=200, mimetype="application/json"
    )
