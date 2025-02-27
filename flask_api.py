from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
from supabase import create_client
from io import BytesIO
import secrets
from functools import wraps
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Supabase configuration
SUPABASE_URL = "https://rnqhongfxhavszyfibzr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJucWhvbmdmeGhhdnN6eWZpYnpyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDA2ODE3MjYsImV4cCI6MjA1NjI1NzcyNn0.UmwYVNEQ3IT1JUW7sK68p18RwQVYL_YVdbS57ihqmrE"

# API configuration
API_HOST = os.getenv('API_HOST', '0.0.0.0')  # Listen on all interfaces
API_PORT = int(os.getenv('API_PORT', 5000))
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:5000')  # For documentation

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return jsonify({"error": "No API key provided"}), 401
        
        # Verify API key in Supabase
        try:
            result = supabase.table('api_keys').select("user_id").eq('key', api_key).execute()
            if not result.data:
                return jsonify({"error": "Invalid API key"}), 401
            # Add user_id to request context
            request.user_id = result.data[0]['user_id']
        except Exception as e:
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
        console.log(token)
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
            print(f"Error verifying token: {str(e)}")
            return jsonify({"error": "Invalid authorization token"}), 401
        
    except Exception as e:
        print(f"Error generating API key: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/data/summary', methods=['GET'])
@require_api_key
def get_data_summary():
    """Get basic summary of the dataset"""
    bucket_path = request.args.get('bucket_path')
    
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
    
    # Verify user owns this dataset
    if not bucket_path.startswith(f"user_{request.user_id}/"):
        return jsonify({"error": "Unauthorized access to dataset"}), 403
    
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    n = request.args.get('n', default=5, type=int)
    return jsonify(df.head(n).to_dict(orient='records'))

@app.route('/api/data/filter', methods=['GET'])
def filter_data():
    """Filter data by column value"""
    bucket_path = request.args.get('bucket_path')
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    column = request.args.get('column')
    value = request.args.get('value')
    
    if not column or not value:
        return jsonify({"error": "Both column and value parameters are required"}), 400
    
    if column not in df.columns:
        return jsonify({"error": f"Column '{column}' not found"}), 400
    
    filtered_df = df[df[column].astype(str).str.contains(value, case=False, na=False)]
    return jsonify(filtered_df.head(50).to_dict(orient='records'))

@app.route('/api/data/stats', methods=['GET'])
def get_stats():
    """Get statistical summary of numeric columns"""
    bucket_path = request.args.get('bucket_path')
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    numeric_stats = df.describe().to_dict()
    return jsonify(numeric_stats)

@app.route('/api/data/unique/<column>', methods=['GET'])
def get_unique_values(column):
    """Get unique values in a column"""
    bucket_path = request.args.get('bucket_path')
    df = load_csv_from_supabase(bucket_path)
    
    if df is None:
        return jsonify({"error": "No dataset found in Supabase storage"}), 404
    
    if column not in df.columns:
        return jsonify({"error": f"Column '{column}' not found"}), 400
    
    unique_values = df[column].unique().tolist()
    return jsonify({
        "column": column,
        "unique_values": unique_values,
        "count": len(unique_values)
    })

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
        except:
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
                "GET /hello": {
                    "description": "Test endpoint (no authentication required)",
                    "parameters": [],
                    "example": f"{API_BASE_URL}/api/hello",
                    "python_example": f"""
import requests

# Test API connection (no auth required)
response = requests.get('https://5306-2806-102e-22-4ea0-e1fe-1158-ac21-a25.ngrok-free.app/api/hello')
print(response.json())
"""
                },
                "GET /data/summary": {
                    "description": "Get dataset summary",
                    "parameters": ["bucket_path"],
                    "example": f"{API_BASE_URL}/api/data/summary?bucket_path={example_dataset}",
                    "python_example": f"""
import requests

# Replace with your actual API key from the API Access Management section
API_KEY = 'YOUR_API_KEY_HERE'

headers = {{
    'X-API-Key': API_KEY  # Your API key goes here
}}

# Get dataset summary
response = requests.get(
    'https://5306-2806-102e-22-4ea0-e1fe-1158-ac21-a25.ngrok-free.app/api/data/summary',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}'
    }}
)
print(response.json())
"""
                },
                "GET /data/head": {
                    "description": "Get first N rows",
                    "parameters": ["bucket_path", "n"],
                    "example": f"{API_BASE_URL}/api/data/head?bucket_path={example_dataset}&n=5",
                    "python_example": f"""
import requests

# Replace with your actual API key from the API Access Management section
API_KEY = 'YOUR_API_KEY_HERE'

headers = {{
    'X-API-Key': API_KEY  # Your API key goes here
}}

# Get first 5 rows of the dataset
response = requests.get(
    'https://5306-2806-102e-22-4ea0-e1fe-1158-ac21-a25.ngrok-free.app/api/data/head',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}',
        'n': 5
    }}
)
print(response.json())
"""
                },
                "GET /data/stats": {
                    "description": "Get statistical summary of numeric columns",
                    "parameters": ["bucket_path"],
                    "example": f"{API_BASE_URL}/api/data/stats?bucket_path={example_dataset}",
                    "python_example": f"""
import requests

# Replace with your actual API key from the API Access Management section
API_KEY = 'YOUR_API_KEY_HERE'

headers = {{
    'X-API-Key': API_KEY  # Your API key goes here
}}

# Get statistical summary
response = requests.get(
    'https://5306-2806-102e-22-4ea0-e1fe-1158-ac21-a25.ngrok-free.app/api/data/stats',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}'
    }}
)
print(response.json())
"""
                }
            },
            "available_datasets": datasets,
            "complete_example": f"""
import requests

# Replace with your actual API key from the API Access Management section
API_KEY = 'YOUR_API_KEY_HERE'

# API endpoint configuration
BASE_URL = 'https://5306-2806-102e-22-4ea0-e1fe-1158-ac21-a25.ngrok-free.app/api'
headers = {{
    'X-API-Key': API_KEY  # Your API key goes here
}}

# Test API connection (no auth required)
response = requests.get(f'{{BASE_URL}}/hello')
print('API Test:', response.json())

# Get dataset summary
response = requests.get(
    f'{{BASE_URL}}/data/summary',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}'
    }}
)
print('Dataset Summary:', response.json())

# Get first 5 rows
response = requests.get(
    f'{{BASE_URL}}/data/head',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}',
        'n': 5
    }}
)
print('First 5 Rows:', response.json())

# Get statistical summary
response = requests.get(
    f'{{BASE_URL}}/data/stats',
    headers=headers,
    params={{
        'bucket_path': '{example_dataset}'
    }}
)
print('Statistical Summary:', response.json())
"""
        }
        return jsonify(docs)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/hello', methods=['GET'])
def hello_world():
    """Simple test endpoint that doesn't require authentication"""
    return jsonify({
        "message": "Hello World!",
        "status": "API is working",
        "timestamp": pd.Timestamp.now().isoformat()
    })

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
                    df = pd.read_csv(BytesIO(response), encoding=detected_encoding, on_bad_lines='skip')
                    print(f"Successfully loaded dataset using detected encoding: {detected_encoding}")
                    return df
                except Exception as e:
                    print(f"Detected encoding {detected_encoding} failed, trying fallback encodings...")
        except ImportError:
            pass

        # Fallback encodings to try
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'ascii']
        errors = []
        
        for encoding in encodings:
            try:
                # Convert bytes to DataFrame with specific encoding and handle bad lines
                df = pd.read_csv(
                    BytesIO(response), 
                    encoding=encoding, 
                    on_bad_lines='skip',
                    engine='python',  # More flexible but slower engine
                    encoding_errors='replace'  # Replace invalid chars with ?
                )
                print(f"Successfully loaded dataset using encoding: {encoding}")
                return df
            except UnicodeDecodeError as e:
                errors.append(f"{encoding}: {str(e)}")
                continue
            except Exception as e:
                errors.append(f"{encoding}: Unexpected error: {str(e)}")
                continue
        
        # If we get here, all encodings failed
        print("Failed to read the CSV file with any encoding")
        print("Attempted encodings and their errors:")
        for error in errors:
            print(f"- {error}")
        return None
            
    except Exception as e:
        print(f"Error accessing Supabase storage: {str(e)}")
        return None

if __name__ == '__main__':
    print(f"Starting API server on {API_HOST}:{API_PORT}")
    print(f"API Base URL: {API_BASE_URL}")
    app.run(host=API_HOST, port=API_PORT) 