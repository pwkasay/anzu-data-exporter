from main import *
import azure.functions as func
import logging
from error_handler import azure_function_error_handler


@azure_function_error_handler
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    start_date = req.params.get("start_date", None)
    end_date = req.params.get("end_date", None)

    # Fetch deals and enrich with notes/attachments/engagements
    deals = fetch_deals(start_date=start_date, end_date=end_date)
    deals = asyncio.run(fetch_notes_attachments_and_engagements(deals))

    # Process deals through GPT batch API
    batch = batch_with_chatgpt(openai_client, deals)

    # Poll for results
    results = None
    poll_attempts = 0
    max_poll_attempts = 300  # 10 minutes max with 2-second intervals

    while not results and poll_attempts < max_poll_attempts:
        check = check_gpt(openai_client, batch)
        if check:
            results = poll_gpt_check(check)
            logging.info("GPT batch processing completed")
            break
        poll_attempts += 1
        time.sleep(2)

    if not results:
        raise TimeoutError("GPT batch processing timed out after 10 minutes")

    # Match GPT results to deals using O(1) lookup
    deals_by_name = {deal["properties"]["dealname"]: deal for deal in deals}
    matched_count = 0

    for result in results:
        deal_data = json.loads(
            result["response"]["body"]["choices"][0]["message"]["content"]
        )
        deal_name = deal_data.get("dealname")

        if deal_name and deal_name in deals_by_name:
            deals_by_name[deal_name]["parsed"] = deal_data
            matched_count += 1
        else:
            logging.warning(f"Could not match GPT result for deal: {deal_name}")

    # Update HubSpot with keywords
    update_count = 0
    for deal in deals:
        if "parsed" in deal:
            update_hubspot_keywords(deal)
            update_count += 1

    return func.HttpResponse(
        f"Successfully processed {len(deals)} deals. Matched {matched_count} GPT results. Updated {update_count} deals.",
        status_code=200,
    )
