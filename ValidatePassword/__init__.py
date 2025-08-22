import os
import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Password validation request received.")

    # Retrieve the password from the environment variable
    correct_password = os.getenv("APP_PASSWORD")

    try:
        # Parse the JSON body from the request
        password = req.params.get("password", None)

        if not password:
            return func.HttpResponse(
                json.dumps({"success": False, "message": "Password is required"}),
                status_code=400,
                mimetype="application/json",
            )

        if password == correct_password:
            return func.HttpResponse(
                json.dumps({"success": True}),
                status_code=200,
                mimetype="application/json",
            )
        else:
            return func.HttpResponse(
                json.dumps({"success": False, "message": "Invalid password"}),
                status_code=401,
                mimetype="application/json",
            )

    except ValueError:
        return func.HttpResponse(
            json.dumps({"success": False, "message": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json",
        )
