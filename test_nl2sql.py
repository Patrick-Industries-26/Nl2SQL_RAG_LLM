"""
Main Test Script for NL2SQL System
Tests natural language query generation and execution
"""

import os
import sys
from datetime import datetime

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.llm_service import get_llm_service
from services.rag_service import get_rag_service
from services.validator import SQLValidator
from services.schema_service import SchemaService
from services.query_executor import QueryExecutor
from config import Config

# Sample metadata with business context
SAMPLE_METADATA = [
    # =========================
    # OFFICES TABLE
    # =========================
    {
        'table_name': 'offices',
        'column_name': None,
        'description': 'Office locations worldwide where employees work.',
        'business_logic': 'Each office has a unique officeCode. Employees are assigned to offices via officeCode.',
        'examples': []
    },
    {
        'table_name': 'offices',
        'column_name': 'officeCode',
        'description': 'Unique identifier for each office location',
        'business_logic': 'Primary key for offices table. Referenced by employees.officeCode.',
        'examples': ['1', '2', '3']
    },
    {
        'table_name': 'offices',
        'column_name': 'city',
        'description': 'City where the office is located',
        'business_logic': '',
        'examples': ['San Francisco', 'Paris', 'Tokyo']
    },
    {
        'table_name': 'offices',
        'column_name': 'phone',
        'description': 'Main contact phone number of the office',
        'business_logic': '',
        'examples': ['+1 650 219 4782', '+33 14 723 4404']
    },
    {
        'table_name': 'offices',
        'column_name': 'addressLine1',
        'description': 'Primary street address of the office',
        'business_logic': '',
        'examples': ['100 Market Street']
    },
    {
        'table_name': 'offices',
        'column_name': 'addressLine2',
        'description': 'Secondary street address information (suite, building, etc.)',
        'business_logic': 'Optional field.',
        'examples': ['Suite 300']
    },
    {
        'table_name': 'offices',
        'column_name': 'state',
        'description': 'State or province of the office location',
        'business_logic': 'May be NULL for countries without states.',
        'examples': ['CA', 'NY']
    },
    {
        'table_name': 'offices',
        'column_name': 'country',
        'description': 'Country where the office is located',
        'business_logic': '',
        'examples': ['USA', 'France', 'Japan']
    },
    {
        'table_name': 'offices',
        'column_name': 'postalCode',
        'description': 'Postal or ZIP code of the office location',
        'business_logic': '',
        'examples': ['94080', '75001']
    },
    {
        'table_name': 'offices',
        'column_name': 'territory',
        'description': 'Sales territory assigned to the office',
        'business_logic': 'Used for regional sales segmentation (e.g., EMEA, NA, APAC).',
        'examples': ['NA', 'EMEA', 'APAC']
    },

    # =========================
    # EMPLOYEES TABLE
    # =========================
    {
        'table_name': 'employees',
        'column_name': None,
        'description': 'Stores employee information including hierarchy and office assignment.',
        'business_logic': 'Employees report to managers via reportsTo. Sales reps are assigned to customers.',
        'examples': []
    },
    {
        'table_name': 'employees',
        'column_name': 'employeeNumber',
        'description': 'Unique identifier for each employee',
        'business_logic': 'Primary key for employees table.',
        'examples': [1002, 1056, 1076]
    },
    {
        'table_name': 'employees',
        'column_name': 'lastName',
        'description': 'Employee last name',
        'business_logic': '',
        'examples': ['Murphy', 'Patterson']
    },
    {
        'table_name': 'employees',
        'column_name': 'firstName',
        'description': 'Employee first name',
        'business_logic': '',
        'examples': ['Diane', 'Mary']
    },
    {
        'table_name': 'employees',
        'column_name': 'extension',
        'description': 'Internal phone extension of the employee',
        'business_logic': '',
        'examples': ['x5800', 'x4611']
    },
    {
        'table_name': 'employees',
        'column_name': 'email',
        'description': 'Work email address of the employee',
        'business_logic': 'Must be unique for internal communication.',
        'examples': ['dmurphy@classicmodelcars.com']
    },
    {
        'table_name': 'employees',
        'column_name': 'officeCode',
        'description': 'Office where the employee works',
        'business_logic': 'Foreign key referencing offices.officeCode.',
        'examples': ['1', '2']
    },
    {
        'table_name': 'employees',
        'column_name': 'reportsTo',
        'description': 'Employee number of the manager this employee reports to',
        'business_logic': 'Self-referencing foreign key. NULL for top-level executives.',
        'examples': [1002, 1056]
    },
    {
        'table_name': 'employees',
        'column_name': 'jobTitle',
        'description': 'Job title or role of the employee',
        'business_logic': 'Common titles: Sales Rep, Sales Manager, VP Sales, President.',
        'examples': ['Sales Rep', 'Sales Manager']
    },

    # =========================
    # CUSTOMERS TABLE
    # =========================
    {
        'table_name': 'customers',
        'column_name': None,
        'description': 'Stores customer contact and account information.',
        'business_logic': 'Customers place orders and make payments. Assigned to a sales representative.',
        'examples': []
    },
    {
        'table_name': 'customers',
        'column_name': 'customerNumber',
        'description': 'Unique identifier for each customer',
        'business_logic': 'Primary key.',
        'examples': [103, 112]
    },
    {
        'table_name': 'customers',
        'column_name': 'customerName',
        'description': 'Full business name of the customer',
        'business_logic': '',
        'examples': ['Atelier graphique']
    },
    {
        'table_name': 'customers',
        'column_name': 'contactLastName',
        'description': 'Last name of the primary contact person',
        'business_logic': '',
        'examples': ['Schmitt']
    },
    {
        'table_name': 'customers',
        'column_name': 'contactFirstName',
        'description': 'First name of the primary contact person',
        'business_logic': '',
        'examples': ['Carine']
    },
    {
        'table_name': 'customers',
        'column_name': 'phone',
        'description': 'Customer contact phone number',
        'business_logic': '',
        'examples': ['40.32.2555']
    },
    {
        'table_name': 'customers',
        'column_name': 'addressLine1',
        'description': 'Primary street address of the customer',
        'business_logic': '',
        'examples': ['54, rue Royale']
    },
    {
        'table_name': 'customers',
        'column_name': 'addressLine2',
        'description': 'Secondary address details',
        'business_logic': 'Optional field.',
        'examples': []
    },
    {
        'table_name': 'customers',
        'column_name': 'city',
        'description': 'City where the customer is located',
        'business_logic': '',
        'examples': ['Nantes']
    },
    {
        'table_name': 'customers',
        'column_name': 'state',
        'description': 'State or province of the customer',
        'business_logic': 'May be NULL.',
        'examples': ['CA']
    },
    {
        'table_name': 'customers',
        'column_name': 'postalCode',
        'description': 'Postal code of the customer',
        'business_logic': '',
        'examples': ['44000']
    },
    {
        'table_name': 'customers',
        'column_name': 'country',
        'description': 'Country where the customer is located',
        'business_logic': '',
        'examples': ['France', 'USA']
    },
    {
        'table_name': 'customers',
        'column_name': 'salesRepEmployeeNumber',
        'description': 'Employee number of assigned sales representative',
        'business_logic': 'Foreign key referencing employees.employeeNumber.',
        'examples': [1056]
    },
    {
        'table_name': 'customers',
        'column_name': 'creditLimit',
        'description': 'Maximum credit allowed for the customer',
        'business_logic': 'Determines how much outstanding balance a customer can have.',
        'examples': [50000.00, 75000.00]
    },

    # =========================
    # ORDERS TABLE
    # =========================
    {
        'table_name': 'orders',
        'column_name': None,
        'description': 'Stores customer orders and their lifecycle status.',
        'business_logic': 'Each order belongs to a customer and may contain multiple orderdetails.',
        'examples': []
    },
    {
        'table_name': 'orders',
        'column_name': 'orderNumber',
        'description': 'Unique identifier for each order',
        'business_logic': 'Primary key.',
        'examples': [10100, 10101]
    },
    {
        'table_name': 'orders',
        'column_name': 'orderDate',
        'description': 'Date when the order was placed',
        'business_logic': '',
        'examples': ['2003-01-06']
    },
    {
        'table_name': 'orders',
        'column_name': 'requiredDate',
        'description': 'Date when the order is required by the customer',
        'business_logic': '',
        'examples': ['2003-01-13']
    },
    {
        'table_name': 'orders',
        'column_name': 'shippedDate',
        'description': 'Date when the order was shipped',
        'business_logic': 'NULL if not yet shipped.',
        'examples': ['2003-01-10']
    },
    {
        'table_name': 'orders',
        'column_name': 'status',
        'description': 'Current status of the order',
        'business_logic': 'Values: Shipped, Cancelled, Resolved, On Hold, In Process, Disputed.',
        'examples': ['Shipped', 'Cancelled']
    },
    {
        'table_name': 'orders',
        'column_name': 'comments',
        'description': 'Additional comments or notes about the order',
        'business_logic': 'Optional field.',
        'examples': []
    },
    {
        'table_name': 'orders',
        'column_name': 'customerNumber',
        'description': 'Customer who placed the order',
        'business_logic': 'Foreign key referencing customers.customerNumber.',
        'examples': [103]
    },

    # =========================
    # PAYMENTS TABLE
    # =========================
    {
        'table_name': 'payments',
        'column_name': None,
        'description': 'Records payments made by customers.',
        'business_logic': 'A customer can make multiple payments. Composite primary key.',
        'examples': []
    },
    {
        'table_name': 'payments',
        'column_name': 'customerNumber',
        'description': 'Customer who made the payment',
        'business_logic': 'Foreign key referencing customers.customerNumber.',
        'examples': [103]
    },
    {
        'table_name': 'payments',
        'column_name': 'checkNumber',
        'description': 'Payment reference or check number',
        'business_logic': 'Part of composite primary key.',
        'examples': ['HQ336336']
    },
    {
        'table_name': 'payments',
        'column_name': 'paymentDate',
        'description': 'Date when payment was made',
        'business_logic': '',
        'examples': ['2003-01-16']
    },
    {
        'table_name': 'payments',
        'column_name': 'amount',
        'description': 'Amount paid by the customer',
        'business_logic': 'Recorded in dollars.',
        'examples': [6066.78]
    },

    # =========================
    # PRODUCTLINES TABLE
    # =========================
    {
        'table_name': 'productlines',
        'column_name': None,
        'description': 'Groups products into logical product categories.',
        'business_logic': 'Referenced by products.productLine.',
        'examples': []
    },
    {
        'table_name': 'productlines',
        'column_name': 'productLine',
        'description': 'Name of the product category',
        'business_logic': 'Primary key.',
        'examples': ['Classic Cars', 'Motorcycles']
    },
    {
        'table_name': 'productlines',
        'column_name': 'textDescription',
        'description': 'Plain text description of the product line',
        'business_logic': '',
        'examples': []
    },
    {
        'table_name': 'productlines',
        'column_name': 'htmlDescription',
        'description': 'HTML-formatted description of the product line',
        'business_logic': '',
        'examples': []
    },
    {
        'table_name': 'productlines',
        'column_name': 'image',
        'description': 'Image representing the product line',
        'business_logic': 'Stored as binary large object.',
        'examples': []
    },

    # =========================
    # PRODUCTS TABLE
    # =========================
    {
        'table_name': 'products',
        'column_name': None,
        'description': 'Catalog of products available for sale.',
        'business_logic': 'Products belong to a product line and are referenced in orderdetails.',
        'examples': []
    },
    {
        'table_name': 'products',
        'column_name': 'productCode',
        'description': 'Unique identifier for each product',
        'business_logic': 'Primary key.',
        'examples': ['S10_1678']
    },
    {
        'table_name': 'products',
        'column_name': 'productName',
        'description': 'Name of the product',
        'business_logic': '',
        'examples': ['1969 Harley Davidson Ultimate Chopper']
    },
    {
        'table_name': 'products',
        'column_name': 'productLine',
        'description': 'Product category the product belongs to',
        'business_logic': 'Foreign key referencing productlines.productLine.',
        'examples': ['Motorcycles']
    },
    {
        'table_name': 'products',
        'column_name': 'productScale',
        'description': 'Scale of the product model',
        'business_logic': 'Used for collectible scale models (e.g., 1:10, 1:18).',
        'examples': ['1:10']
    },
    {
        'table_name': 'products',
        'column_name': 'productVendor',
        'description': 'Supplier or vendor of the product',
        'business_logic': '',
        'examples': ['Min Lin Diecast']
    },
    {
        'table_name': 'products',
        'column_name': 'productDescription',
        'description': 'Detailed description of the product',
        'business_logic': '',
        'examples': []
    },
    {
        'table_name': 'products',
        'column_name': 'quantityInStock',
        'description': 'Current inventory quantity available',
        'business_logic': 'Used for stock management.',
        'examples': [7933]
    },
    {
        'table_name': 'products',
        'column_name': 'buyPrice',
        'description': 'Wholesale purchase price paid by the company',
        'business_logic': 'Cost basis for profit calculation.',
        'examples': [48.81]
    },
    {
        'table_name': 'products',
        'column_name': 'MSRP',
        'description': 'Manufacturer Suggested Retail Price',
        'business_logic': 'Selling price benchmark.',
        'examples': [95.70]
    },

    # =========================
    # ORDERDETAILS TABLE
    # =========================
    {
        'table_name': 'orderdetails',
        'column_name': None,
        'description': 'Line items for each order showing products, quantity, and price.',
        'business_logic': 'Each record represents a product within an order.',
        'examples': []
    },
    {
        'table_name': 'orderdetails',
        'column_name': 'orderNumber',
        'description': 'Order to which this line item belongs',
        'business_logic': 'Foreign key referencing orders.orderNumber.',
        'examples': [10100]
    },
    {
        'table_name': 'orderdetails',
        'column_name': 'productCode',
        'description': 'Product included in the order',
        'business_logic': 'Foreign key referencing products.productCode.',
        'examples': ['S10_1678']
    },
    {
        'table_name': 'orderdetails',
        'column_name': 'quantityOrdered',
        'description': 'Number of units ordered',
        'business_logic': '',
        'examples': [30]
    },
    {
        'table_name': 'orderdetails',
        'column_name': 'priceEach',
        'description': 'Selling price per unit for this order line',
        'business_logic': 'May differ from MSRP due to discounts.',
        'examples': [95.70]
    },
    {
        'table_name': 'orderdetails',
        'column_name': 'orderLineNumber',
        'description': 'Line number within the order',
        'business_logic': 'Used to order items within an order.',
        'examples': [1, 2]
    }
]


def print_separator():
    print("\n" + "=" * 80 + "\n")


def build_rag_index(connection_params):
    """Build RAG index with schema and metadata"""
    print("Extracting schema from database...")

    schema_service = SchemaService()
    schema = schema_service.extract_schema(connection_params)

    print(f"✓ Schema extracted: {len(schema['tables'])} tables found")

    # Prepare schema data for RAG
    schema_data = []

    # Add table and column information
    for table_name, table_info in schema['tables'].items():
        # Table-level entry
        table_meta = next((m for m in SAMPLE_METADATA if m['table_name'] == table_name and m['column_name'] is None),
                          None)
        schema_data.append({
            'table_name': table_name,
            'column_name': None,
            'description': table_meta['description'] if table_meta else table_info.get('comment', ''),
            'business_logic': table_meta['business_logic'] if table_meta else '',
            'examples': []
        })

        # Column-level entries
        for col in table_info['columns']:
            col_meta = next(
                (m for m in SAMPLE_METADATA if m['table_name'] == table_name and m.get('column_name') == col['name']),
                None)
            schema_data.append({
                'table_name': table_name,
                'column_name': col['name'],
                'data_type': col['type'],
                'description': col_meta['description'] if col_meta else '',
                'business_logic': col_meta['business_logic'] if col_meta else '',
                'examples': col_meta.get('examples', []) if col_meta else [],
                'is_primary_key': col.get('is_primary_key', False),
                'is_foreign_key': col.get('is_foreign_key', False),
                'foreign_key_ref': col.get('foreign_key_ref')
            })

    # Build index
    print("Building RAG index...")
    rag_service = get_rag_service()
    rag_service.build_index(schema_data, connection_id=1)
    print("✓ RAG index built successfully")

    return schema


def format_schema_for_llm(schema):
    """Format schema for LLM consumption"""
    formatted = []

    for table_name, table_info in schema['tables'].items():
        formatted.append(f"\nTable: {table_name}")

        # Add table description
        table_meta = next(
            (m for m in SAMPLE_METADATA if m['table_name'] == table_name and m.get('column_name') is None), None)
        if table_meta and table_meta.get('description'):
            formatted.append(f"  Description: {table_meta['description']}")
        if table_meta and table_meta.get('business_logic'):
            formatted.append(f"  Business Logic: {table_meta['business_logic']}")

        formatted.append("  Columns:")
        for col in table_info['columns']:
            col_str = f"    - {col['name']} ({col['type']})"

            # Add column metadata
            col_meta = next(
                (m for m in SAMPLE_METADATA if m['table_name'] == table_name and m.get('column_name') == col['name']),
                None)
            if col_meta and col_meta.get('description'):
                col_str += f" - {col_meta['description']}"

            if col['is_primary_key']:
                col_str += " [PRIMARY KEY]"
            if col['is_foreign_key']:
                col_str += f" [FK -> {col.get('foreign_key_ref')}]"

            formatted.append(col_str)

    return '\n'.join(formatted)


def get_validation_schema(schema):
    """Get schema in format suitable for validator"""
    validation_schema = {}

    for table_name, table_info in schema['tables'].items():
        validation_schema[table_name] = {
            'columns': [col['name'] for col in table_info['columns']],
            'primary_key': table_info.get('primary_key'),
            'foreign_keys': {}
        }

        for col in table_info['columns']:
            if col.get('is_foreign_key'):
                validation_schema[table_name]['foreign_keys'][col['name']] = col.get('foreign_key_ref')

    return validation_schema


def test_query(query_text, schema, use_rag=True, execute=False, connection_params=None):
    """
    Test a natural language query

    Args:
        query_text: Natural language question
        schema: Database schema dictionary
        use_rag: Whether to use RAG for context retrieval
        execute: Whether to execute the query on database
        connection_params: Database connection parameters for execution
    """
    print(f"Natural Language Query: {query_text}")
    print("-" * 80)

    # Get schema context
    if use_rag:
        rag_service = get_rag_service()
        schema_context = rag_service.get_schema_context(query_text)
        business_context = rag_service.get_business_context(query_text)
        print("RAG Retrieved Context:")
        print(schema_context[:500] + "..." if len(schema_context) > 500 else schema_context)
    else:
        schema_context = format_schema_for_llm(schema)
        business_context = ""

    print("\n" + "-" * 80)

    # Generate SQL
    print("Generating SQL...")
    llm_service = get_llm_service()

    try:
        generated_sql = llm_service.generate_sql(
            query_text,
            schema_context,
            business_context
        )

        print(f"\nGenerated SQL:\n{generated_sql}")
    except Exception as e:
        print(f"❌ SQL Generation Failed: {str(e)}")
        return None

    print("\n" + "-" * 80)

    # Validate SQL
    print("Validating SQL...")
    validation_schema = get_validation_schema(schema)
    validator = SQLValidator(validation_schema)

    is_valid, errors = validator.validate(generated_sql)
    cost_estimate = validator.estimate_cost(generated_sql)

    if is_valid:
        print("✓ SQL is VALID")
        print(f"Complexity Score: {cost_estimate['estimated_complexity']}")
        print(f"Joins: {cost_estimate['join_count']}")
        print(f"Subquery Depth: {cost_estimate['subquery_depth']}")
    else:
        print("❌ SQL VALIDATION FAILED:")
        for error in errors:
            print(f"  - {error}")
        return None

    # Execute if requested
    if execute and connection_params:
        print("\n" + "-" * 80)
        print("Executing Query...")
        executor = QueryExecutor()

        try:
            result = executor.execute_query(connection_params, generated_sql)

            if result['success']:
                print(f"✓ Execution Successful")
                print(f"Rows Returned: {result['row_count']}")
                print(f"Execution Time: {result['execution_time_ms']}ms")

                if result['data']:
                    print("\nFirst 5 Results:")
                    for i, row in enumerate(result['data'][:5]):
                        print(f"{i + 1}. {row}")

                    if result['truncated']:
                        print(f"\n(Results truncated at {Config.MAX_QUERY_ROWS} rows)")
            else:
                print(f"❌ Execution Failed: {result['error']}")
        except Exception as e:
            print(f"❌ Execution Error: {str(e)}")

    return generated_sql


def main():
    """Main test function"""
    print_separator()
    print("NL2SQL SYSTEM TEST")
    print(f"Timestamp: {datetime.now()}")
    print_separator()

    # Check if LLM model exists
    if not os.path.exists(Config.LLM_MODEL_PATH):
        print("❌ ERROR: LLM model not found!")
        print(f"Expected path: {Config.LLM_MODEL_PATH}")
        print("\nPlease download the model:")
        print("wget https://huggingface.co/defog/sqlcoder-7b-2/resolve/main/sqlcoder-7b-q5_k_m.gguf")
        return

    # Get database connection parameters
    print("Database Connection Setup")
    print("-" * 80)
    host = "localhost"
    port = "3306"
    user = "root"
    password = "Jp^6"
    database = "classicmodels"

    connection_params = {
        'host': host,
        'port': int(port),
        'user': user,
        'password': password,
        'database': database
    }

    # Test connection
    print("\nTesting database connection...")
    schema_service = SchemaService()
    success, message = schema_service.test_connection(connection_params)

    if not success:
        print(f"❌ Connection failed: {message}")
        return

    print("✓ Database connection successful")
    print_separator()

    # Initialize services
    print("Initializing services...")
    try:
        llm_service = get_llm_service()
        if not llm_service.validate_model_loaded():
            print("❌ LLM model failed to load")
            return
        print("✓ LLM service initialized")
    except Exception as e:
        print(f"❌ Failed to initialize LLM: {str(e)}")
        return

    # Build RAG index and extract schema
    try:
        schema = build_rag_index(connection_params)
    except Exception as e:
        print(f"❌ Failed to build RAG index: {str(e)}")
        return

    print_separator()

    # Test queries
    test_queries = [
        # "Show all employees",
        "Show me all customers from USA",
        # "What are the top 5 products by quantity in stock?",
        # "List all employees and their managers",
        # "Find customers with credit limit greater than 50000",
        # "Show me total sales amount by product line",
        # "Which orders are still in process?",
        # "Get the names and emails of all sales representatives",
        # "Show me the most expensive product in each product line",
        # "List all offices in California",
        # "Find customers who have never placed an order"
    ]

    # Ask user if they want to execute queries
    execute_queries = False
    response = input("\nDo you want to execute queries on the database? (y/n): ").lower()
    if response == 'y':
        execute_queries = True

    # Run test queries
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'=' * 80}")
        print(f"TEST QUERY {i}/{len(test_queries)}")
        print(f"{'=' * 80}\n")

        try:
            test_query(
                query,
                schema,
                use_rag=True,
                execute=execute_queries,
                connection_params=connection_params if execute_queries else None
            )
        except Exception as e:
            print(f"❌ Test failed: {str(e)}")

        if i < len(test_queries):
            input("\nPress Enter to continue to next query...")

    print_separator()
    print("✓ All tests completed!")
    print_separator()


if __name__ == "__main__":
    main()
