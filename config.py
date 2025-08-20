import os
from typing import Optional

class Config:
    """Configuration class for the JSONL to CSV converter application."""
    
    # Google Cloud Configuration
    GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    GOOGLE_CLOUD_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT_ID', 'your-project-id')
    GOOGLE_CLOUD_REGION = os.getenv('GOOGLE_CLOUD_REGION', 'us-central1')
    
    # AI Platform Configuration
    AI_PLATFORM_ENDPOINT = os.getenv('AI_PLATFORM_ENDPOINT', f'{GOOGLE_CLOUD_REGION}-aiplatform.googleapis.com')
    AI_MODEL_NAME = os.getenv('AI_MODEL_NAME', 'meta/llama-3.1-405b-instruct-maas')
    
    # Google Cloud Storage Configuration
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'your-bucket-name')
    GCS_DEFAULT_FOLDER = os.getenv('GCS_DEFAULT_FOLDER', 'intermediatecsv')
    
    # Application Configuration
    SIGNED_URL_EXPIRATION = int(os.getenv('SIGNED_URL_EXPIRATION', '3600'))
    MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '300'))
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls) -> None:
        """Validate required configuration values."""
        if not cls.GOOGLE_CREDENTIALS_PATH:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is required")
        
        if not os.path.exists(cls.GOOGLE_CREDENTIALS_PATH):
            raise FileNotFoundError(f"Google Cloud credentials file not found: {cls.GOOGLE_CREDENTIALS_PATH}")
        
        if cls.GOOGLE_CLOUD_PROJECT_ID == 'your-project-id':
            raise ValueError("GOOGLE_CLOUD_PROJECT_ID environment variable must be set to a valid project ID")
        
        if cls.GCS_BUCKET_NAME == 'your-bucket-name':
            raise ValueError("GCS_BUCKET_NAME environment variable must be set to a valid bucket name")
    
    @classmethod
    def get_ai_model_config(cls) -> dict:
        """Get AI model configuration."""
        return {
            "model": cls.AI_MODEL_NAME,
            "stream": False,
            "max_tokens": 4096,
            "temperature": 1,
            "top_p": 0.95
        }
    
    @classmethod
    def get_default_prompt(cls) -> str:
        """Get the default prompt for AI model."""
        return """Generate Python code that: 
1. Reads from '/home/user/input.jsonl' (pre-provided, do not modify) 
2. Writes to '/home/user/output.csv' with columns: as per the response json. 
3. For each JSONL line:
   - Extracts JSON string from 'response['candidates'][0]['content']['parts'][0]['text']'
   - Parses this inner JSON to map fields to CSV columns
   - Ignores 'request' field 
4. Maps inner JSON fields as in the sample 
5. Fills missing fields with '' 
6. Handles invalid JSON lines with try-except, logging errors to stderr 
7. Uses only 'json', 'csv', 'sys' modules
8. Output ONLY the Python code (no explanations, no Markdown)
9. Before giving the output run the code once yourself and if the request and the output match then only give the code
10. After that before giving the code check that the generated code will have at least parsed 2 lines, if not, then give the code that can
11. This type of parsing often gives this error "Error parsing inner JSON: Expecting value: line 1 column 1 (char 0)" so please explicitly run the code that you give and output the output also so that it can be checked
12. Never return incorrect code
13. Ensure proper try-except block structure to avoid syntax errors"""
