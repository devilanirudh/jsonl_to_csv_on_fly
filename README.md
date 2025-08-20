# JSONL to CSV Converter - AI-Powered

An intelligent Flask-based web service that converts JSONL (JSON Lines) files to CSV format using AI-generated parsing logic. The service leverages Google Cloud's AI Platform to automatically generate and execute Python code for parsing complex JSON structures.

## Features

- **AI-Powered Parsing**: Uses Google Cloud's Llama 3.1 model to generate custom parsing logic
- **Automatic Retry Logic**: Implements retry mechanisms with error feedback for robust conversion
- **CSV Validation**: Validates output CSV files to ensure data quality
- **Google Cloud Storage Integration**: Automatically uploads results to GCS with signed URLs
- **Flexible Input**: Supports both file uploads and base64-encoded content
- **Custom Instructions**: Allows additional parsing instructions for complex JSON structures

## Architecture

The service is designed as a Google Cloud Function but can also run as a standalone Flask application. It uses:

- **Flask**: Web framework for handling HTTP requests
- **Google Cloud AI Platform**: For generating parsing code
- **Google Cloud Storage**: For storing output files
- **Pandas**: For CSV validation and processing
- **Tenacity**: For retry logic with exponential backoff

## Prerequisites

- Python 3.11+
- Google Cloud Project with AI Platform API enabled
- Google Cloud Storage bucket
- Service account credentials with appropriate permissions

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd jsonl-csv
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the example environment file and update it with your values:

```bash
cp env.example .env
```

Edit `.env` with your actual values:

```bash
# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json
GOOGLE_CLOUD_PROJECT_ID=your-actual-project-id
GOOGLE_CLOUD_REGION=us-central1

# Google Cloud Storage Configuration
GCS_BUCKET_NAME=your-actual-bucket-name

# Load environment variables
source .env
```

## Running with Docker

### Build the Docker Image

```bash
docker build -t jsonl-csv-converter .
```

### Run the Container

```bash
docker run -p 8080:8080 \
  -v /path/to/credentials.json:/app/credentials.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  -e GOOGLE_CLOUD_PROJECT_ID=your-project-id \
  -e GCS_BUCKET_NAME=your-bucket-name \
  jsonl-csv-converter
```

## API Usage

### Endpoint

`POST /jsonl_to_csv`

### Request Formats

#### 1. File Upload

```bash
curl -X POST http://localhost:8080/jsonl_to_csv \
  -F "file=@your_file.jsonl" \
  -F "project_id=your-project-id" \
  -F "additional_instruction=Parse nested objects as separate columns"
```

#### 2. Base64 Encoded Content

```bash
curl -X POST http://localhost:8080/jsonl_to_csv \
  -H "Content-Type: application/json" \
  -d '{
    "file_base64": "base64_encoded_content",
    "file_name": "data.jsonl",
    "project_id": "your-project-id",
    "additional_instruction": "Handle arrays as comma-separated values"
  }'
```

### Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file` | File | Yes* | JSONL file to upload |
| `file_base64` | String | Yes* | Base64 encoded file content |
| `file_name` | String | Yes** | Original filename (required with file_base64) |
| `project_id` | String | No | Google Cloud Project ID (default: "prodloop") |
| `additional_instruction` | String | No | Additional parsing instructions |
| `gcs_bucket` | String | No | GCS bucket name (default: "prodloop") |
| `gcs_folder_path` | String | No | Custom folder path in bucket |
| `signed_url_expiration` | Integer | No | Signed URL expiration in seconds (default: 3600) |

*Either `file` or `file_base64` is required
**Required when using `file_base64`

### Response Format

#### Success Response

```json
{
  "gcs_path": "gs://bucket-name/folder/file.csv",
  "signed_url": "https://storage.googleapis.com/...",
  "signed_url_expiration_seconds": 3600
}
```

#### Error Response

```json
{
  "error": "Error message",
  "error_details": {
    "execution_error": "Python code execution failed",
    "validation_error": "CSV validation failed",
    "file_error": "Output file not created"
  }
}
```

## Input JSONL Format

The service expects JSONL files where each line contains a JSON object. The AI model is specifically trained to handle JSON objects with this structure:

```json
{"response": {"candidates": [{"content": {"parts": [{"text": "{\"field1\": \"value1\", \"field2\": \"value2\"}"}]}}]}, "request": {...}}
```

The AI extracts and parses the inner JSON from the `text` field to create CSV columns.

## Output

- **CSV File**: Generated with columns based on the JSON structure
- **GCS Upload**: Automatically uploaded to Google Cloud Storage
- **Signed URL**: Provides temporary access to the generated CSV file
- **Validation**: Ensures the CSV contains valid data

## Error Handling

The service includes comprehensive error handling:

- **Retry Logic**: Up to 3 attempts with error feedback to the AI model
- **CSV Validation**: Checks for empty files and columns
- **Graceful Degradation**: Continues processing even if some lines fail
- **Detailed Logging**: Comprehensive logging for debugging

## Development

### Local Development

```bash
# Install development dependencies
pip install -r requirements.txt

# Run with Flask development server
export FLASK_APP=main.py
export FLASK_ENV=development
flask run --host=0.0.0.0 --port=8080
```

### Testing

```bash
# Test with sample JSONL file
curl -X POST http://localhost:8080/jsonl_to_csv \
  -F "file=@sample.jsonl" \
  -F "project_id=your-project-id"
```

## Deployment

### Google Cloud Functions

1. Deploy using Google Cloud CLI:
```bash
gcloud functions deploy jsonl-to-csv \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point jsonl_to_csv
```

2. Set environment variables in the Cloud Function configuration

### Docker Deployment

```bash
# Build and push to container registry
docker build -t gcr.io/your-project/jsonl-csv-converter .
docker push gcr.io/your-project/jsonl-csv-converter

# Deploy to Cloud Run
gcloud run deploy jsonl-csv-converter \
  --image gcr.io/your-project/jsonl-csv-converter \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

## Security Considerations

- **Authentication**: Implement proper authentication for production use
- **File Size Limits**: Consider implementing file size limits
- **Rate Limiting**: Add rate limiting to prevent abuse
- **Input Validation**: Validate JSONL format before processing
- **Credential Management**: Use secure credential management in production

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Ensure service account has proper permissions
2. **AI Model Errors**: Check if AI Platform API is enabled
3. **GCS Upload Failures**: Verify bucket permissions and existence
4. **Memory Issues**: Large files may require increased memory limits

### Logs

Enable detailed logging by setting the log level:
```python
logging.basicConfig(level=logging.DEBUG)
```
# jsonl_to_csv_on_fly
