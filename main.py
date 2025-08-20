import json
import csv
import sys
import os
import tempfile
import subprocess
import logging
import uuid
from datetime import datetime,timedelta
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import requests
import re
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from flask import jsonify, request
from werkzeug.utils import secure_filename
from google.cloud import storage
import base64
from config import Config

# Validate configuration
Config.validate()

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.GOOGLE_CREDENTIALS_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_csv(output_path):
    logger.info(f"Validating CSV at {output_path}")
    try:
        df = pd.read_csv(output_path)
        if df.empty:
            logger.warning("CSV file is empty")
            return False, "CSV file is empty"
        
        columns_status = {}
        all_columns_empty = True
        
        for column in df.columns:
            has_data = df[column].notna() & (df[column] != '') & (df[column] != ' ')
            columns_status[column] = has_data.any()
            if has_data.any():
                all_columns_empty = False
        
        if all_columns_empty:
            logger.warning("All columns in CSV contain no data")
            return False, "All columns in CSV contain no data"
        
        empty_columns = [col for col, has_data in columns_status.items() if not has_data]
        if empty_columns:
            logger.info(f"Warning: The following columns contain no data: {', '.join(empty_columns)}")
            return True, f"Warning: The following columns contain no data: {', '.join(empty_columns)}"
        
        logger.info("CSV validation successful: All columns contain some data")
        return True, "All columns contain some data"
    
    except Exception as e:
        logger.error(f"CSV validation failed: {str(e)}")
        return False, f"CSV validation failed: {str(e)}"

def get_access_token():
    logger.info("Getting Google Cloud access token")
    try:
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        logger.info("Successfully obtained access token")
        return credentials.token
    except Exception as e:
        logger.error(f"Failed to get credentials: {e}")
        return None

def call_ai_model(prompt, jsonl_sample, error_message=None, project_id=None):
    logger.info("Calling AI model API")
    if project_id is None:
        project_id = Config.GOOGLE_CLOUD_PROJECT_ID
    
    endpoint = Config.AI_PLATFORM_ENDPOINT
    region = Config.GOOGLE_CLOUD_REGION
    
    access_token = get_access_token()
    
    if not access_token:
        logger.error("Failed to authenticate with Google Cloud")
        return None
    
    full_prompt = f"""i am having a jsonl file its sample line is 

{jsonl_sample}

{prompt}"""
    
    if error_message:
        logger.info(f"Adding error feedback to prompt: {error_message}")
        full_prompt += f"\n\nPrevious attempt failed with this error: {error_message}\nPlease modify the code to address this issue."
    
    logger.info("Preparing API request")
    request_data = Config.get_ai_model_config()
    request_data["messages"] = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": full_prompt
                }
            ]
        }
    ]
    
    url = f"https://{endpoint}/v1beta1/projects/{project_id}/locations/{region}/endpoints/openapi/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    try:
        logger.info(f"Sending request to {url}")
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        
        result = response.json()
        logger.info("Received response from AI model API")
        
        if "choices" in result and len(result["choices"]) > 0:
            logger.info("Successfully extracted content from 'choices' field")
            return result["choices"][0]["message"]["content"]
        elif "candidates" in result and len(result["candidates"]) > 0:
            logger.info("Successfully extracted content from 'candidates' field")
            return result["candidates"][0]["content"]["parts"][0]["text"]
        else:
            logger.error("Unexpected API response format")
            return None
            
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return None

def extract_python_code(ai_response):
    logger.info("Extracting Python code from AI response")
    code_block_match = re.search(r'```python\s+(.*?)\s+```', ai_response, re.DOTALL)
    if code_block_match:
        logger.info("Found code block with ```python``` markers")
        return code_block_match.group(1)
    
    lines = ai_response.split('\n')
    code_lines = []
    in_code = False
    
    for line in lines:
        if line.strip() == '```python' or line.strip() == '```':
            in_code = not in_code
            continue
        
        if in_code or '```' not in line:
            code_lines.append(line)
    
    if code_lines:
        logger.info("Extracted code without clear markers")
        return '\n'.join(code_lines)
    else:
        logger.warning("No code found in AI response, returning full response")
        return ai_response

def execute_python_code(code, input_path, output_path):
    logger.info(f"Executing Python code with input_path={input_path}, output_path={output_path}")
    with tempfile.NamedTemporaryFile(suffix='.py', mode='w', delete=False) as temp_file:
        temp_file.write(code)
        temp_file_path = temp_file.name
    
    try:
        modified_code = code.replace('/home/user/input.jsonl', input_path)
        modified_code = modified_code.replace('/home/user/output.csv', output_path)
        
        with open(temp_file_path, 'w') as f:
            f.write(modified_code)
        
        logger.info(f"Running Python script at {temp_file_path}")
        result = subprocess.run(['python', temp_file_path], 
                              capture_output=True, 
                              text=True, 
                              check=False)
        
        if result.returncode != 0:
            logger.error(f"Execution failed with error: {result.stderr}")
            return False, result.stderr
        
        logger.info("Code execution successful")
        return True, result.stdout
    
    except Exception as e:
        logger.error(f"Exception during code execution: {str(e)}")
        return False, str(e)
    
    finally:
        if os.path.exists(temp_file_path):
            logger.info(f"Cleaning up temporary file {temp_file_path}")
            os.unlink(temp_file_path)

def is_failure_result(result):
    clean_code, success, message, validation_success, output_exists = result
    if not success or not validation_success or not output_exists:
        logger.warning(f"Retry condition met: success={success}, validation_success={validation_success}, output_exists={output_exists}")
        return True
    return False

@retry(stop=stop_after_attempt(Config.MAX_RETRY_ATTEMPTS), wait=wait_fixed(2), retry=retry_if_result(is_failure_result))
def generate_and_execute(input_path, output_path, prompt, first_line, previous_error=None, project_id=None):
    logger.info("Starting generate_and_execute with retry")
    generated_code = call_ai_model(prompt, first_line, previous_error, project_id)
    if not generated_code:
        logger.error("Failed to generate code from AI model")
        return None, False, "Failed to generate code from AI model", False, False
    
    clean_code = extract_python_code(generated_code)
    success, message = execute_python_code(clean_code, input_path, output_path)
    output_exists = os.path.exists(output_path)
    
    if success and output_exists:
        logger.info(f"Code executed successfully, checking CSV at {output_path}")
        validation_success, validation_message = validate_csv(output_path)
        logger.info(f"Validation result: success={validation_success}, message={validation_message}")
        return clean_code, success, message, validation_success, output_exists
    
    logger.warning(f"Execution or file creation failed: success={success}, output_exists={output_exists}")
    return clean_code, success, message, False, output_exists

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """Uploads a file to the bucket."""
    logger.info(f"Uploading {local_file_path} to gs://{bucket_name}/{destination_blob_name}")
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(local_file_path)

        logger.info(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
        return True, f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logger.error(f"Failed to upload to GCS: {str(e)}")
        return False, str(e)

def generate_signed_url(bucket_name, blob_name, expiration=3600):
    """
    Generates a signed URL for accessing a GCS object
    
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The path to the object within the bucket
        expiration (int): Time in seconds for URL expiration (default: 1 hour)
    
    Returns:
        tuple: (success, url or error message)
    """
    logger.info(f"Generating signed URL for gs://{bucket_name}/{blob_name} with expiration {expiration} seconds")
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.utcnow() + timedelta(seconds=expiration),
            method="GET"
        )
        
        logger.info(f"Successfully generated signed URL with expiration {expiration} seconds")
        return True, url
    except Exception as e:
        logger.error(f"Failed to generate signed URL: {str(e)}")
        return False, str(e)


def jsonl_to_csv(request):
    """
    Cloud function entry point that converts a JSONL file to CSV using AI-generated parsing logic
    
    Args:
        request (flask.Request): HTTP request object containing either:
            - file: JSONL file upload
            - file_base64: Base64 encoded JSONL file content
            - file_name: Original filename when using base64 (required with file_base64)
            - project_id: Google Cloud Project ID (optional, default from GOOGLE_CLOUD_PROJECT_ID env var)
            - additional_instruction: Optional instruction to add to the prompt
            - gcs_bucket: Custom GCS bucket name (optional, default from GCS_BUCKET_NAME env var)
            - gcs_folder_path: Custom folder path within bucket (optional, default: "{run_id}/intermediatecsv/")
            
    Returns:
        flask.Response: JSON response with conversion results
    """
    logger.info("Starting JSONL to CSV cloud function")
    
   
    run_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}"
    logger.info(f"Run ID: {run_id}")
    
    if request.method != 'POST':
        logger.error("Invalid method, only POST supported")
        return jsonify({"error": "Only POST method is supported"}), 400
    
    try:
        if request.is_json:
            logger.info("Processing JSON request")
            request_data = request.get_json()
        else:
            logger.info("Processing form data request")
            request_data = request.form
        
        project_id = request_data.get('project_id', Config.GOOGLE_CLOUD_PROJECT_ID)
        logger.info(f"Using project_id: {project_id}")
        
        additional_instruction = request_data.get('additional_instruction', "")
        logger.info(f"Additional instruction provided: {len(additional_instruction) > 0}")
        
        gcs_bucket = request_data.get('gcs_bucket', Config.GCS_BUCKET_NAME)
        default_folder_path = f"{run_id}/{Config.GCS_DEFAULT_FOLDER}/"
        gcs_folder_path = request_data.get('gcs_folder_path', default_folder_path)
        try:
            signed_url_expiration = int(request_data.get('signed_url_expiration', str(Config.SIGNED_URL_EXPIRATION)))
        except ValueError:
            signed_url_expiration = Config.SIGNED_URL_EXPIRATION 
        
        if gcs_folder_path and not gcs_folder_path.endswith('/'):
            gcs_folder_path += '/'
            
        logger.info(f"Using GCS configuration - Bucket: {gcs_bucket}, Folder Path: {gcs_folder_path}")
        
        if 'file' in request.files and request.files['file'].filename != '':
            logger.info("Processing file upload")
            file = request.files['file']
            filename = secure_filename(file.filename)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.jsonl') as temp_input:
                file.save(temp_input.name)
                input_path = temp_input.name
                
        elif 'file_base64' in request_data:
            logger.info("Processing base64 encoded file")
            
            if 'file_name' not in request_data:
                logger.error("file_name is required when using file_base64")
                return jsonify({"error": "file_name is required when using file_base64"}), 400
                
            base64_content = request_data.get('file_base64')
            filename = secure_filename(request_data.get('file_name'))
            
            try:
                file_content = base64.b64decode(base64_content)
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jsonl') as temp_input:
                    temp_input.write(file_content)
                    input_path = temp_input.name
                    
                logger.info(f"Base64 content decoded and saved to {input_path}")
                
            except Exception as e:
                logger.error(f"Error decoding base64 content: {str(e)}")
                return jsonify({"error": f"Error decoding base64 content: {str(e)}"}), 400
                
        else:
            logger.error("No file provided: either 'file' upload or 'file_base64' with 'file_name' is required")
            return jsonify({"error": "No file provided. Please provide either a file upload or base64 encoded file content with filename."}), 400
        
       
        original_filename = os.path.splitext(filename)[0]
        logger.info(f"Processing file: {filename}")
        
        
        output_path = os.path.join(os.path.dirname(input_path), f"{original_filename}.csv")
        
       
        with open(input_path, 'r') as f:
            first_line = f.readline().strip()
           
            if not first_line:
                logger.error("Input file is empty")
                return jsonify({"error": "Input file is empty"}), 400
        
        logger.info(f"First line of JSONL: {first_line[:100]}...")
        
       
        default_prompt = Config.get_default_prompt()
        
       
        if additional_instruction:
            logger.info("Adding additional instruction to prompt")
            prompt = default_prompt + f"\n14. {additional_instruction}"
        else:
            prompt = default_prompt
        
      
        logger.info("Starting code generation and execution")
        previous_error = None
        try:
            clean_code, success, message, validation_success, output_exists = generate_and_execute(
                input_path, 
                output_path, 
                prompt, 
                first_line,
                previous_error,
                project_id
            )
            
           
            response = {
                # "run_id": run_id,
                # "original_filename": filename,
                # "code": clean_code if clean_code else "No code generated",
                # "success": success and validation_success and output_exists,
                # "message": message
            }
            
            if success and validation_success and output_exists:
                _, validation_message = validate_csv(output_path)
                # response["validation_message"] = validation_message
                
                try:
                    df = pd.read_csv(output_path)
                    # Convert first 10 rows to dict for preview
                    # response["preview"] = df.head(10).to_dict(orient="records")
                    # response["row_count"] = len(df)
                    # response["column_count"] = len(df.columns)
                    # response["columns"] = df.columns.tolist()
                    
                   
                    gcs_path = f"{gcs_folder_path}{original_filename}.csv"
                    
                    upload_success, upload_result = upload_to_gcs(output_path, gcs_bucket, gcs_path)
                    
                    if upload_success:
                        response["gcs_path"] = upload_result
                        logger.info(f"CSV successfully uploaded to {upload_result}")
                        signed_url_success, signed_url_result = generate_signed_url(
                            gcs_bucket, 
                            gcs_path, 
                            expiration=signed_url_expiration
                        )
                        
                        if signed_url_success:
                            response["signed_url"] = signed_url_result
                            response["signed_url_expiration_seconds"] = signed_url_expiration
                            logger.info(f"Generated signed URL with expiration {signed_url_expiration} seconds")
                        else:
                            response["signed_url_error"] = signed_url_result
                            logger.error(f"Failed to generate signed URL: {signed_url_result}")
                    else:
                        response["gcs_error"] = upload_result
                        logger.error(f"Failed to upload CSV to GCS: {upload_result}")
                    
                    logger.info(f"Successfully converted JSONL to CSV with {len(df)} rows and {len(df.columns)} columns")
                    
                except Exception as e:
                    error_msg = f"Error processing CSV: {str(e)}"
                    logger.error(error_msg)
                    response["error"] = error_msg
            else:
                error_details = {}
                if not success:
                    error_details["execution_error"] = message
                if not validation_success:
                    _, validation_message = validate_csv(output_path) if output_exists else ("", "CSV file was not created")
                    error_details["validation_error"] = validation_message
                if not output_exists:
                    error_details["file_error"] = "Output CSV file was not created"
                
                response["error_details"] = error_details
                logger.error(f"Conversion failed: {error_details}")
            
            try:
                if os.path.exists(input_path):
                    os.unlink(input_path)
                    logger.info(f"Deleted temporary input file: {input_path}")
                if os.path.exists(output_path) and response.get("gcs_path"):
                    os.unlink(output_path)
                    logger.info(f"Deleted temporary output file: {output_path}")
                logger.info("Temporary files cleaned up")
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup: {str(cleanup_error)}")
            
            logger.info("Function execution completed")
            return jsonify(response)
            
        except Exception as e:
            error_msg = f"All retry attempts failed with unexpected error: {str(e)}"
            logger.error(error_msg)
            return jsonify({"error": error_msg, "success": False, "run_id": run_id}), 500
    
    except Exception as e:
        error_msg = f"Unexpected error in cloud function: {str(e)}"
        logger.error(error_msg)
        return jsonify({"error": error_msg, "success": False, "run_id": run_id}), 500