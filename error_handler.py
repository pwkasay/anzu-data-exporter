"""Error handling utilities for Azure Functions."""

import functools
import logging
import azure.functions as func
from typing import Callable, Any


def azure_function_error_handler(func_handler: Callable) -> Callable:
    """
    Decorator for handling errors in Azure Functions.

    Provides consistent error logging and response formatting.
    """

    @functools.wraps(func_handler)
    def wrapper(req: func.HttpRequest) -> func.HttpResponse:
        try:
            return func_handler(req)
        except ValueError as e:
            logging.error(f"Validation error: {e}")
            return func.HttpResponse(f"Bad Request: {str(e)}", status_code=400)
        except PermissionError as e:
            logging.error(f"Permission error: {e}")
            return func.HttpResponse(f"Forbidden: {str(e)}", status_code=403)
        except ConnectionError as e:
            logging.error(f"Connection error: {e}")
            return func.HttpResponse(f"Service Unavailable: {str(e)}", status_code=503)
        except TimeoutError as e:
            logging.error(f"Timeout error: {e}")
            return func.HttpResponse(f"Request Timeout: {str(e)}", status_code=408)
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            return func.HttpResponse(
                f"Internal Server Error: {str(e)}", status_code=500
            )

    return wrapper


def handle_api_error(response, operation_name: str):
    """
    Handle API response errors consistently.

    Args:
        response: The API response object
        operation_name: Name of the operation for logging

    Raises:
        ConnectionError: For network-related errors
        PermissionError: For 403 errors
        ValueError: For 400 errors
        Exception: For other errors
    """
    if response.status_code == 429:
        raise ConnectionError(f"{operation_name}: Rate limit exceeded")
    elif response.status_code == 403:
        raise PermissionError(f"{operation_name}: Access forbidden")
    elif response.status_code == 400:
        raise ValueError(f"{operation_name}: Bad request - {response.text}")
    elif response.status_code >= 500:
        raise ConnectionError(
            f"{operation_name}: Server error - {response.status_code}"
        )
    elif response.status_code >= 400:
        raise Exception(
            f"{operation_name}: Error {response.status_code} - {response.text}"
        )
