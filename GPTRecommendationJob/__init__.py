from main import *
import azure.functions as func
import logging


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    print("Main triggered")
    start_date = req.params.get('start_date', None)
    end_date = req.params.get('end_date', None)

    try:
        deals = fetch_deals(start_date=start_date, end_date=end_date)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        deals = fetch_and_attach_owner_details(deals, "hubspot_owner_id")
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        deals = fetch_and_attach_owner_details(deals, "team_member_1")
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        deals = attach_notes(deals)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        deals = attach_attachments(deals)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        deals = attach_engagements(deals)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    try:
        batch = batch_with_chatgpt(openai_client, deals)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    try:
        results = None
        while not results:
            check = check_gpt(openai_client, batch)
            if check:
                results = poll_gpt_check(check)
                print("Results returned")
            else:
                time.sleep(5)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    try:
        for deal in deals:
            for result in results:
                deal_data = json.loads(result['response']['body']['choices'][0]['message']['content'])
                deal_name = deal_data['dealname']
                if deal['properties']['dealname'] == deal_name:
                    deal['parsed'] = deal_data
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    try:
        for deal in deals:
            update_hubspot_keywords(deal)
        return func.HttpResponse(f"Main 1 - Processed ", status_code=200)
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

