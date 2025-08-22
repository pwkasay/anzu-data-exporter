# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Azure Functions Python application that exports and processes HubSpot CRM deal data for Anzu Partners (a venture capital firm). It uses OpenAI GPT-4 to analyze deals and generate categorizations and recommendations.

## Key Commands

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables (copy and fill out)
cp local.settings.json.example local.settings.json  # if example exists
# Or create local.settings.json with required environment variables

# Run Azure Functions locally (requires Azure Functions Core Tools)
func start

# Test individual functions (examples)
curl http://localhost:7071/api/FetchHubspot
curl http://localhost:7071/api/FetchHubspot?start_date=2024-01-01&end_date=2024-12-31
curl http://localhost:7071/api/ParseHubspot
curl http://localhost:7071/api/GPTRecommendationJob
curl http://localhost:7071/api/GenerateCSV
curl http://localhost:7071/api/ValidatePassword
```

### Environment Setup
Create a `local.settings.json` file with required environment variables:
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "HUBSPOT_API_KEY": "<your-hubspot-private-app-token>",
    "OPEN_AI_KEY": "<your-openai-api-key>",
    "CLIENT_ID": "<azure-ad-client-id>",
    "CLIENT_SECRET": "<azure-ad-client-secret>",
    "TENANT_ID": "<azure-ad-tenant-id>",
    "USER_ID": "<user-id>",
    "APP_PASSWORD": "<application-password>",
    "ENVIRONMENT": "dev"
  }
}
```

## Architecture

### Core Components
- **main.py**: Contains all shared business logic and helper functions (595+ lines)
- **error_handler.py**: Centralized error handling decorators and utilities
- **Individual Function Folders**: Each Azure Function has its own folder with `__init__.py` and `function.json`
- **data/**: Contains GPT prompts and configuration files
  - `gpt_prompt.txt`: Main prompt template for deal analysis
  - `system_prompt_for_final.txt`: System prompt for GPT-4
  - `user_prompt_for_final.txt`: User prompt template
  - `deal_result.jsonl`: Sample/cached results

### Azure Functions Endpoints

1. **FetchHubspot** (`/api/FetchHubspot`)
   - Fetches raw deal data from HubSpot API
   - Enriches with owner details
   - Fetches notes, attachments, and engagements asynchronously
   - Parameters: `start_date`, `end_date` (optional)

2. **ParseHubspot** (`/api/ParseHubspot`)
   - Processes deals through GPT-4 batch API
   - Updates HubSpot with generated keywords
   - Returns processed deal data

3. **GPTRecommendationJob** (`/api/GPTRecommendationJob`)
   - Complete pipeline: fetch → process → update
   - Combines FetchHubspot and ParseHubspot functionality
   - Includes timeout handling (10 minutes max)
   - Parameters: `start_date`, `end_date` (optional)

4. **GenerateCSV** (`/api/GenerateCSV`)
   - Exports processed deal data to CSV format
   - Parameters: `start_date`, `end_date` (optional)

5. **ValidatePassword** (`/api/ValidatePassword`)
   - Password validation endpoint for authentication

### Key Functions in main.py

#### Data Fetching
- `fetch_deals(start_date, end_date, include_stage_history)`: Retrieves deals from HubSpot with date filtering
- `fetch_deals_with_stage_history()`: Fetches deals including stage transition history
- `fetch_and_attach_owner_details(deals, owner_field)`: Attaches owner information to deals
- `fetch_notes_attachments_and_engagements(deals)`: Async enrichment of deal data with notes, files, and engagements

#### HubSpot API Operations
- `search_hubspot_object(object_type, search_body)`: Generic HubSpot object search
- `fetch_owner_details_async(session, owner_id)`: Async owner fetching with caching
- `update_hubspot_keywords(deal)`: Updates HubSpot deals with AI-generated tags
- `update_hubspot_with_keywords(openai_client, deals)`: Batch keyword update

#### OpenAI Integration
- `create_openai_client(api_key)`: Initializes OpenAI client
- `batch_with_chatgpt(openai_client, deals)`: Processes deals through OpenAI batch API
- `check_gpt(openai_client, batch)`: Checks batch processing status
- `poll_gpt_check(check)`: Polls for batch completion

#### Data Export
- `export_csv(start_date, end_date)`: Generates CSV reports with analysis

#### Document Processing
- Supports PDF extraction via PyPDF2
- Word document processing via python-docx
- Excel file handling via openpyxl
- PowerPoint presentation extraction via python-pptx

### Data Flow
1. **FetchHubspot** → Fetches raw deal data from HubSpot API with enrichment
2. **ParseHubspot** → Processes deals through GPT-4 for categorization
3. **GPTRecommendationJob** → Complete pipeline (fetch + process + update)
4. **GenerateCSV** → Exports processed data to CSV format

### Important Patterns
- **Async Processing**: Heavy use of asyncio/aiohttp for concurrent API calls
- **Batch Processing**: OpenAI batch API for efficient GPT processing
- **Error Handling**: Decorator-based error handling via `@azure_function_error_handler`
- **Caching**: Owner details cached in memory to reduce API calls
- **Environment Detection**: Auto-loads `.env` file when `ENVIRONMENT=dev`

## Development Notes

### API Integration
- **HubSpot API**: Uses v3 API endpoints with Bearer token authentication
- **OpenAI API**: Batch API for processing multiple deals efficiently
- **Azure AD**: OAuth2 authentication for Microsoft services

### Error Handling
- Custom error handler decorator in `error_handler.py`
- Handles ValueError, PermissionError, ConnectionError, TimeoutError
- Returns appropriate HTTP status codes (400, 403, 408, 500, 503)

### Performance Considerations
- Async operations for concurrent API calls
- In-memory caching for owner details
- Batch processing to avoid rate limits
- 10-minute timeout for Azure Functions (configured in host.json)

### File Processing Support
The application processes various document formats from HubSpot attachments:
- PDF files via PyPDF2
- Word documents via python-docx
- Excel spreadsheets via openpyxl
- PowerPoint presentations via python-pptx

### Testing Approach
No formal test framework is configured. Test by:
1. Running functions locally with `func start`
2. Using curl or Postman to test HTTP endpoints
3. Checking Azure Functions logs for errors
4. Testing with date parameters to limit data scope

### Deployment
- Deployed as Azure Functions App
- Python 3.x runtime required
- Environment variables must be configured in Azure portal
- Function timeout set to 10 minutes in host.json