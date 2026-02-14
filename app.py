"""
Simplified Flask Backend for NL2SQL
Integrated with HTML/CSS/JS Frontend
No Authentication Required - Local Use Only
"""

from flask import Flask, render_template, jsonify, request, send_from_directory
import os
import logging

# Services
from services.llm_service import get_llm_service
from services.rag_service import get_rag_service
from services.validator import SQLValidator
from services.schema_service import SchemaService
from services.query_executor import QueryExecutor
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__,
            static_folder='static',
            template_folder='templates')
app.config.from_object(Config)

# Initialize services
schema_service = SchemaService()
query_executor = QueryExecutor()

# Global state to store connection and RAG index
APP_STATE = {
    'connection_params': None,
    'schema': None,
    'rag_loaded': False,
    'connection_id': 1
}


# ============================================
# Helper Functions
# ============================================

def get_connection_params():
    """Get stored database connection parameters"""
    if APP_STATE['connection_params']:
        return APP_STATE['connection_params']

    # Try to load from config/env if not set
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', 'Jp^6'),
        'database': os.getenv('DB_NAME', 'classicmodels')
    }


def ensure_schema_loaded():
    """Ensure schema is loaded and cached"""
    if APP_STATE['schema'] is None:
        connection_params = get_connection_params()
        APP_STATE['schema'] = schema_service.extract_schema(connection_params)
        logger.info(f"Schema loaded: {len(APP_STATE['schema']['tables'])} tables")
    return APP_STATE['schema']


def ensure_rag_loaded():
    """Load RAG index if not already loaded (cached)"""
    if not APP_STATE['rag_loaded']:
        rag_service = get_rag_service()

        # Try to load existing index from disk
        if rag_service.load_index(APP_STATE['connection_id']):
            logger.info("✓ RAG index loaded from cache")
            APP_STATE['rag_loaded'] = True
            return True
        else:
            logger.warning("RAG index not found in cache - needs to be built")
            return False
    return True


# ============================================
# Frontend Routes
# ============================================

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('index.html')


@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)


# ============================================
# API Routes
# ============================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'rag_loaded': APP_STATE['rag_loaded'],
        'schema_loaded': APP_STATE['schema'] is not None
    })


@app.route('/api/connect', methods=['POST'])
def connect_database():
    """Connect to database and initialize RAG"""
    try:
        data = request.get_json()

        # Store connection parameters
        APP_STATE['connection_params'] = {
            'host': data.get('host', 'localhost'),
            'port': data.get('port', 3306),
            'user': data['username'],
            'password': data['password'],
            'database': data['database']
        }

        # Test connection
        success, message = schema_service.test_connection(APP_STATE['connection_params'])
        if not success:
            return jsonify({'error': message}), 400

        # Extract schema
        APP_STATE['schema'] = schema_service.extract_schema(APP_STATE['connection_params'])

        # Build RAG index (this creates and saves it)
        from utils.rag_helper import build_rag_index_from_schema
        build_rag_index_from_schema(APP_STATE['schema'], APP_STATE['connection_id'])
        APP_STATE['rag_loaded'] = True

        logger.info(f"✓ Connected to {data['database']} with {len(APP_STATE['schema']['tables'])} tables")
        logger.info("✓ RAG index built and saved")

        return jsonify({
            'message': 'Connected successfully',
            'tables': len(APP_STATE['schema']['tables'])
        })

    except Exception as e:
        logger.error(f"Connection error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/schema', methods=['GET'])
def get_schema():
    """Get database schema"""
    try:
        schema = ensure_schema_loaded()

        # Format schema for frontend
        formatted_schema = []
        for table_name, table_info in schema['tables'].items():
            table_data = {
                'name': table_name,
                'columns': []
            }

            for col in table_info['columns']:
                table_data['columns'].append({
                    'name': col['name'],
                    'type': col['type'],
                    'is_primary': col.get('is_primary_key', False),
                    'is_foreign': col.get('is_foreign_key', False),
                    'nullable': col.get('nullable', True)
                })

            formatted_schema.append(table_data)

        return jsonify({'schema': formatted_schema})

    except Exception as e:
        logger.error(f"Schema fetch error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/schema/refresh', methods=['POST'])
def refresh_schema():
    """Refresh schema and rebuild RAG index"""
    try:
        connection_params = get_connection_params()

        # Invalidate cache and re-extract
        schema_service.invalidate_cache(connection_params)
        APP_STATE['schema'] = schema_service.extract_schema(connection_params)

        # Rebuild RAG index
        from utils.rag_helper import build_rag_index_from_schema
        build_rag_index_from_schema(APP_STATE['schema'], APP_STATE['connection_id'])
        APP_STATE['rag_loaded'] = True

        logger.info("✓ Schema refreshed and RAG index rebuilt")

        return jsonify({
            'message': 'Schema refreshed successfully',
            'tables': len(APP_STATE['schema']['tables'])
        })

    except Exception as e:
        logger.error(f"Schema refresh error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/query', methods=['POST'])
def process_query():
    """Process natural language query and generate SQL"""
    try:
        data = request.get_json()
        natural_query = data.get('query', '').strip()

        if not natural_query:
            return jsonify({'error': 'Query cannot be empty'}), 400

        # Ensure schema and RAG are loaded
        schema = ensure_schema_loaded()
        if not ensure_rag_loaded():
            return jsonify({
                'error': 'RAG index not loaded. Please refresh schema first.',
                'needs_refresh': True
            }), 400

        # Get context from RAG
        rag_service = get_rag_service()
        schema_context = rag_service.get_schema_context(natural_query)
        business_context = rag_service.get_business_context(natural_query)

        # Generate SQL
        llm_service = get_llm_service()
        generated_sql = llm_service.generate_sql(
            natural_query,
            schema_context,
            business_context
        )

        # Validate SQL
        validation_schema = schema_service.get_schema_for_validation(schema)
        validator = SQLValidator(validation_schema)
        is_valid, errors = validator.validate(generated_sql)

        if not is_valid:
            return jsonify({
                'sql': generated_sql,
                'valid': False,
                'errors': errors
            }), 400

        # Execute SQL
        connection_params = get_connection_params()
        result = query_executor.execute_query(connection_params, generated_sql)

        if result['success']:
            return jsonify({
                'sql': generated_sql,
                'results': result['data'],
                'columns': result['columns'],
                'num_results': result['row_count'],
                'execution_time': result['execution_time_ms']
            })
        else:
            return jsonify({
                'sql': generated_sql,
                'error': result['error']
            }), 400

    except Exception as e:
        logger.error(f"Query processing error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/execute-sql', methods=['POST'])
def execute_sql():
    """Execute manually edited SQL"""
    try:
        data = request.get_json()
        sql = data.get('sql', '').strip()

        if not sql:
            return jsonify({'error': 'SQL cannot be empty'}), 400

        # Validate SQL
        schema = ensure_schema_loaded()
        validation_schema = schema_service.get_schema_for_validation(schema)
        validator = SQLValidator(validation_schema)
        is_valid, errors = validator.validate(sql)

        if not is_valid:
            return jsonify({
                'error': 'SQL validation failed',
                'validation_errors': errors
            }), 400

        # Execute SQL
        connection_params = get_connection_params()
        result = query_executor.execute_query(connection_params, sql)

        if result['success']:
            return jsonify({
                'results': result['data'],
                'columns': result['columns'],
                'num_results': result['row_count'],
                'execution_time': result['execution_time_ms']
            })
        else:
            return jsonify({'error': result['error']}), 400

    except Exception as e:
        logger.error(f"SQL execution error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/examples', methods=['GET'])
def get_examples():
    """Get example queries"""
    examples = [
        {
            'query': 'Show me all customers from USA',
            'category': 'Basic'
        },
        {
            'query': 'What are the top 5 products by quantity in stock?',
            'category': 'Aggregation'
        },
        {
            'query': 'List all employees and their managers',
            'category': 'Joins'
        },
        {
            'query': 'Find customers with credit limit greater than 50000',
            'category': 'Filtering'
        },
        {
            'query': 'Show me total sales by product line',
            'category': 'Aggregation'
        },
        {
            'query': 'Which orders are still in process?',
            'category': 'Filtering'
        }
    ]

    return jsonify({'examples': examples})


# ============================================
# Initialize on Startup
# ============================================

def initialize_app():
    """Initialize application on startup"""
    logger.info("=" * 60)
    logger.info("NL2SQL Application Starting...")
    logger.info("=" * 60)

    # Check if LLM model exists
    if not os.path.exists(Config.LLM_MODEL_PATH):
        logger.error(f"❌ LLM model not found at: {Config.LLM_MODEL_PATH}")
        logger.error("Please download the model first")
        return False

    # Initialize LLM service
    try:
        llm_service = get_llm_service()
        if llm_service.validate_model_loaded():
            logger.info("✓ LLM service initialized")
        else:
            logger.error("❌ LLM service failed to initialize")
            return False
    except Exception as e:
        logger.error(f"❌ LLM initialization error: {str(e)}")
        return False

    # Try to load existing RAG index (if available)
    try:
        if ensure_rag_loaded():
            logger.info("✓ RAG index loaded from cache")
        else:
            logger.info("⚠ RAG index not found - will be built on first connection")
    except Exception as e:
        logger.warning(f"⚠ RAG loading warning: {str(e)}")

    logger.info("=" * 60)
    logger.info("✓ Application initialized successfully!")
    logger.info("=" * 60)
    logger.info(f"Open your browser to: http://localhost:5000")
    logger.info("=" * 60)

    return True


# ============================================
# Main Entry Point
# ============================================

if __name__ == '__main__':
    if initialize_app():
        app.run(
            host='127.0.0.1',
            port=5000,
            debug=True
        )
    else:
        logger.error("Failed to initialize application")