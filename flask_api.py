from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
from supabase.client import Client
from io import BytesIO
import secrets
from functools import wraps
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing required environment variables: SUPABASE_URL and SUPABASE_KEY must be set")

# API configuration
API_HOST = '0.0.0.0'  # Always bind to all interfaces
API_PORT = int(os.getenv('PORT', os.getenv('API_PORT', 5000)))  # Use PORT for Render compatibility
API_BASE_URL = os.getenv('API_BASE_URL')

# Initialize Supabase client
try:
    # First try with direct initialization
    try:
        supabase = Client(SUPABASE_URL, SUPABASE_KEY)
    except TypeError:
        # If that fails, try with the create_client function
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Test the connection
    supabase.auth.get_session()  # This will fail early if the connection is not working
    logger.info("Supabase client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Supabase client: {str(e)}")
    raise

def get_user_from_subdomain(subdomain):
    """Get user ID from subdomain"""
    try:
        result = supabase.table('user_settings').select("user_id").eq('settings->>subdomain', subdomain).execute()
        if result.data:
            return result.data[0]['user_id']
        return None
    except Exception as e:
        logger.error(f"Error getting user from subdomain: {str(e)}")
        return None

@app.before_request
def handle_subdomain():
    """Handle subdomain routing"""
    try:
        host = request.headers.get('Host', '')
        if '.' in host and not host.startswith('localhost'):
            subdomain = host.split('.')[0]
            if subdomain:
                user_id = get_user_from_subdomain(subdomain)
                if user_id:
                    # Store user_id in request context
                    request.user_id = user_id
                    return None
                else:
                    return jsonify({"error": "Invalid subdomain"}), 404
    except Exception as e:
        logger.error(f"Error handling subdomain: {str(e)}")
    return None

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({"error": "No API key provided"}), 401
        
        # Get user_id from subdomain first
        user_id = getattr(request, 'user_id', None)
        
        # Verify API key in Supabase
        try:
            result = supabase.table('api_keys').select("user_id").eq('key', api_key).execute()
            if not result.data:
                return jsonify({"error": "Invalid API key"}), 401
                
            api_key_user_id = result.data[0]['user_id']
            
            # If request came through subdomain, verify user matches
            if user_id and user_id != api_key_user_id:
                return jsonify({"error": "API key does not match subdomain owner"}), 403
                
            # Add user_id to request context
            request.user_id = api_key_user_id
        except Exception as e:
            logger.error(f"Error verifying API key: {str(e)}")
            return jsonify({"error": "Error verifying API key"}), 500
            
        return f(*args, **kwargs)
    return decorated_function

@app.route('/api/generate-key', methods=['POST'])
def generate_api_key():
    """Generate new API key for a user"""
    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({"error": "No authorization token provided"}), 401
        
        # Verify JWT with Supabase
        token = auth_header.split(' ')[1]
        try:
            # Get user from token
            user = supabase.auth.get_user(token)
            user_id = user.user.id
            
            # Generate new API key
            api_key = secrets.token_urlsafe(32)
            
            # Store in Supabase
            result = supabase.table('api_keys').insert({
                'key': api_key,
                'user_id': user_id,
                'created_at': 'now()'
            }).execute()
            
            return jsonify({
                "api_key": api_key,
                "message": "Store this API key safely. It won't be shown again.",
                "user_id": user_id
            })
            
        except Exception as e:
            logger.error(f"Error verifying token: {str(e)}")
            return jsonify({"error": "Invalid authorization token"}), 401
        
    except Exception as e:
        logger.error(f"Error generating API key: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/data/summary', methods=['GET'])
@require_api_key
def get_data_summary():
    """Get basic summary of the dataset"""
    bucket_path = request.args.get('bucket_path')
    if not bucket_path:
        return jsonify({"error": "bucket_path parameter is required"}), 400
    
    # Verify user owns this dataset
    if not bucket_path.startswith(f"user_{request.user_id}/"):
        return jsonify({"error": "Unauthorized access to dataset"}), 403
    
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    return jsonify({
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": df.columns.tolist(),
        "data_types": df.dtypes.astype(str).to_dict()
    })

@app.route('/api/data/head', methods=['GET'])
@require_api_key
def get_head():
    """Get first N rows of the dataset"""
    bucket_path = request.args.get('bucket_path')
    if not bucket_path:
        return jsonify({"error": "bucket_path parameter is required"}), 400
    
    # Verify user owns this dataset
    if not bucket_path.startswith(f"user_{request.user_id}/"):
        return jsonify({"error": "Unauthorized access to dataset"}), 403
    
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    n = request.args.get('n', default=5, type=int)
    return jsonify(df.head(n).to_dict(orient='records'))

@app.route('/api/data/stats', methods=['GET'])
@require_api_key
def get_stats():
    """Get statistical summary of numeric columns"""
    bucket_path = request.args.get('bucket_path')
    if not bucket_path:
        return jsonify({"error": "bucket_path parameter is required"}), 400
    
    # Verify user owns this dataset
    if not bucket_path.startswith(f"user_{request.user_id}/"):
        return jsonify({"error": "Unauthorized access to dataset"}), 403
    
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    numeric_stats = df.describe().to_dict()
    return jsonify(numeric_stats)

@app.route('/api/docs/<user_id>', methods=['GET'])
def get_api_docs(user_id):
    """Get API documentation for a specific user"""
    try:
        # Get user's datasets
        user_path = f"user_{user_id}"
        datasets = []
        try:
            folders = supabase.storage.from_('datasets').list(user_path)
            for folder in folders:
                if folder['name'].endswith('.csv'):
                    datasets.append(f"{user_path}/{folder['name']}")
        except Exception as e:
            logger.error(f"Error listing datasets: {str(e)}")
            pass
        
        example_dataset = datasets[0] if datasets else "example_dataset_path"
        
        # Generate documentation with configurable base URL
        docs = {
            "base_url": API_BASE_URL + "/api",
            "authentication": {
                "type": "API Key",
                "header": "X-API-Key: YOUR_API_KEY_HERE",
                "note": "Replace YOUR_API_KEY_HERE with the API key generated from the API Access Management section"
            },
            "endpoints": {
                "GET /data/summary": {
                    "description": "Get dataset summary",
                    "parameters": ["bucket_path"],
                    "example": f"{API_BASE_URL}/api/data/summary?bucket_path={example_dataset}"
                },
                "GET /data/head": {
                    "description": "Get first N rows",
                    "parameters": ["bucket_path", "n"],
                    "example": f"{API_BASE_URL}/api/data/head?bucket_path={example_dataset}&n=5"
                },
                "GET /data/stats": {
                    "description": "Get statistical summary of numeric columns",
                    "parameters": ["bucket_path"],
                    "example": f"{API_BASE_URL}/api/data/stats?bucket_path={example_dataset}"
                }
            },
            "available_datasets": datasets
        }
        return jsonify(docs)
        
    except Exception as e:
        logger.error(f"Error generating API documentation: {str(e)}")
        return jsonify({"error": str(e)}), 500

def load_csv_from_supabase(bucket_path=None):
    """Helper function to load the CSV file from Supabase"""
    try:
        if bucket_path is None:
            return None
        
        # Get file from Supabase storage
        response = supabase.storage.from_('datasets').download(bucket_path)
        
        # Try to detect encoding first using chardet
        try:
            import chardet
            detected = chardet.detect(response)
            detected_encoding = detected['encoding']
            if detected_encoding:
                try:
                    df = pd.read_csv(
                        BytesIO(response), 
                        encoding=detected_encoding,
                        on_bad_lines='skip'
                    )
                    logger.info(f"Successfully loaded dataset using detected encoding: {detected_encoding}")
                    return df
                except Exception as e:
                    logger.warning(f"Detected encoding {detected_encoding} failed, trying fallback encodings...")
        except ImportError:
            pass

        # Fallback encodings to try
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'ascii']
        errors = []
        
        for encoding in encodings:
            try:
                df = pd.read_csv(
                    BytesIO(response), 
                    encoding=encoding,
                    on_bad_lines='skip'
                )
                logger.info(f"Successfully loaded dataset using encoding: {encoding}")
                return df
            except UnicodeDecodeError as e:
                errors.append(f"{encoding}: {str(e)}")
                continue
            except Exception as e:
                errors.append(f"{encoding}: Unexpected error: {str(e)}")
                continue
        
        # If we get here, all encodings failed
        logger.error("Failed to read the CSV file with any encoding")
        logger.error("Attempted encodings and their errors:")
        for error in errors:
            logger.error(f"- {error}")
        return None
            
    except Exception as e:
        logger.error(f"Error accessing Supabase storage: {str(e)}")
        return None

@app.route('/api/hello', methods=['GET'])
def hello_world():
    """Simple test endpoint that doesn't require authentication"""
    return jsonify({
        "message": "Hello World!",
        "status": "API is working",
        "timestamp": pd.Timestamp.now().isoformat()
    })

if __name__ == '__main__':
    logger.info(f"Starting API server on {API_HOST}:{API_PORT}")
    logger.info(f"API Base URL: {API_BASE_URL}")
    app.run(host=API_HOST, port=API_PORT, debug=False) 