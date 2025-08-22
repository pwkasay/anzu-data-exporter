# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Azure Functions Python application that exports and processes HubSpot CRM deal data for Anzu Partners (a venture capital firm). It uses OpenAI GPT-4 to analyze deals and generate categorizations and recommendations.

## Key Commands

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run Azure Functions locally
func start

# Test individual functions (examples)
curl http://localhost:7071/api/FetchHubspot
curl http://localhost:7071/api/ParseHubspot
curl http://localhost:7071/api/GenerateCSV
```

### Environment Setup
Create a `local.settings.json` file with required environment variables:
- `HUBSPOT_API_KEY` - HubSpot private app token
- `OPEN_AI_KEY` - OpenAI API key  
- `CLIENT_ID`, `CLIENT_SECRET`, `TENANT_ID` - Azure AD credentials
- `APP_PASSWORD` - Application password

## Architecture

### Core Components
- **main.py**: Contains all shared business logic and helper functions
- **Individual Function Folders**: Each Azure Function has its own folder with `__init__.py` and `function.json`
- **data/**: Contains GPT prompts and configuration files

### Key Functions in main.py
- `fetch_deals()`: Retrieves deals from HubSpot with date filtering
- `fetch_notes_attachments_and_engagements()`: Async enrichment of deal data
- `batch_with_chatgpt()`: Processes deals through OpenAI batch API
- `export_csv()`: Generates CSV reports with analysis
- `update_hubspot_with_keywords()`: Updates HubSpot deals with AI-generated tags

### Data Flow
1. **FetchHubspot** → Fetches raw deal data from HubSpot API
2. **ParseHubspot** → Processes deals through GPT-4 for categorization
3. **GPTRecommendationJob** → Complete pipeline (fetch + process + update)
4. **GenerateCSV** → Exports processed data to CSV format

### Important Patterns
- **Async Processing**: Heavy use of asyncio/aiohttp for concurrent API calls
- **Batch Processing**: OpenAI batch API for efficient GPT processing
- **Rate Limiting**: Built-in delays and batching (see `BATCH_SIZE`, `MAX_CONCURRENT_REQUESTS`)
- **Error Handling**: Exponential backoff for API retries

## Development Notes

### API Rate Limits
- HubSpot: Respect 10 requests/second limit (handled via asyncio semaphores)
- OpenAI: Uses batch API to avoid rate limits
- Document processing: Limited to 10 concurrent file downloads

### File Processing Support
The application processes various document formats:
- PDF files via PyPDF2
- Word documents via python-docx
- Excel spreadsheets via openpyxl
- PowerPoint presentations via python-pptx

### Testing Approach
No formal test framework is configured. Test by:
1. Running functions locally with `func start`
2. Using curl or Postman to test HTTP endpoints
3. Checking Azure Functions logs for errors

### Common Development Tasks
- To add a new Azure Function: Create folder with `__init__.py` and `function.json`
- To modify GPT prompts: Edit files in `data/` directory
- To adjust processing limits: Modify `BATCH_SIZE` and `MAX_CONCURRENT_REQUESTS` in main.py