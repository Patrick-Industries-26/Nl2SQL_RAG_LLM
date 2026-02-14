"""
Quick Test Script - Simple NL2SQL Testing
Test individual natural language queries quickly
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.llm_service import get_llm_service
from services.rag_service import get_rag_service
from services.validator import SQLValidator
from config import Config

# Simplified schema for validation
VALIDATION_SCHEMA = {
    'offices': {
        'columns': ['officeCode', 'city', 'phone', 'addressLine1', 'addressLine2', 'state', 'country', 'postalCode',
                    'territory'],
        'primary_key': 'officeCode',
        'foreign_keys': {}
    },
    'employees': {
        'columns': ['employeeNumber', 'lastName', 'firstName', 'extension', 'email', 'officeCode', 'reportsTo',
                    'jobTitle'],
        'primary_key': 'employeeNumber',
        'foreign_keys': {'officeCode': 'offices.officeCode', 'reportsTo': 'employees.employeeNumber'}
    },
    'customers': {
        'columns': ['customerNumber', 'customerName', 'contactLastName', 'contactFirstName', 'phone', 'addressLine1',
                    'addressLine2', 'city', 'state', 'postalCode', 'country', 'salesRepEmployeeNumber', 'creditLimit'],
        'primary_key': 'customerNumber',
        'foreign_keys': {'salesRepEmployeeNumber': 'employees.employeeNumber'}
    },
    'orders': {
        'columns': ['orderNumber', 'orderDate', 'requiredDate', 'shippedDate', 'status', 'comments', 'customerNumber'],
        'primary_key': 'orderNumber',
        'foreign_keys': {'customerNumber': 'customers.customerNumber'}
    },
    'orderdetails': {
        'columns': ['orderNumber', 'productCode', 'quantityOrdered', 'priceEach', 'orderLineNumber'],
        'primary_key': None,
        'foreign_keys': {'orderNumber': 'orders.orderNumber', 'productCode': 'products.productCode'}
    },
    'payments': {
        'columns': ['customerNumber', 'checkNumber', 'paymentDate', 'amount'],
        'primary_key': None,
        'foreign_keys': {'customerNumber': 'customers.customerNumber'}
    },
    'products': {
        'columns': ['productCode', 'productName', 'productLine', 'productScale', 'productVendor',
                    'productDescription', 'quantityInStock', 'buyPrice', 'MSRP'],
        'primary_key': 'productCode',
        'foreign_keys': {'productLine': 'productlines.productLine'}
    },
    'productlines': {
        'columns': ['productLine', 'textDescription', 'htmlDescription', 'image'],
        'primary_key': 'productLine',
        'foreign_keys': {}
    }
}

# Simple schema description
SCHEMA_DESCRIPTION = """
Table: offices
  Description: Office locations worldwide
  Columns:
    - officeCode (varchar(10)) [PRIMARY KEY]
    - city (varchar(50))
    - country (varchar(50))
    - territory (varchar(10))

Table: employees
  Description: Employee information and organizational hierarchy
  Columns:
    - employeeNumber (int) [PRIMARY KEY]
    - lastName (varchar(50))
    - firstName (varchar(50))
    - email (varchar(100))
    - officeCode (varchar(10)) [FK -> offices.officeCode]
    - reportsTo (int) [FK -> employees.employeeNumber]
    - jobTitle (varchar(50))

Table: customers
  Description: Customer information and contact details
  Business Logic: Customers are assigned to sales representatives
  Columns:
    - customerNumber (int) [PRIMARY KEY]
    - customerName (varchar(50))
    - contactLastName (varchar(50))
    - contactFirstName (varchar(50))
    - city (varchar(50))
    - country (varchar(50))
    - salesRepEmployeeNumber (int) [FK -> employees.employeeNumber]
    - creditLimit (decimal(10,2))

Table: orders
  Description: Customer orders with status tracking
  Columns:
    - orderNumber (int) [PRIMARY KEY]
    - orderDate (date)
    - requiredDate (date)
    - shippedDate (date)
    - status (varchar(15)) - Values: Shipped, Cancelled, In Process, On Hold
    - customerNumber (int) [FK -> customers.customerNumber]

Table: orderdetails
  Description: Line items for each order
  Columns:
    - orderNumber (int) [FK -> orders.orderNumber]
    - productCode (varchar(15)) [FK -> products.productCode]
    - quantityOrdered (int)
    - priceEach (decimal(10,2))
    - orderLineNumber (smallint)

Table: payments
  Description: Customer payment records
  Columns:
    - customerNumber (int) [FK -> customers.customerNumber]
    - checkNumber (varchar(50))
    - paymentDate (date)
    - amount (decimal(10,2))

Table: products
  Description: Product catalog with pricing and inventory
  Business Logic: MSRP is suggested retail price, buyPrice is wholesale cost
  Columns:
    - productCode (varchar(15)) [PRIMARY KEY]
    - productName (varchar(70))
    - productLine (varchar(50)) [FK -> productlines.productLine]
    - quantityInStock (smallint)
    - buyPrice (decimal(10,2))
    - MSRP (decimal(10,2))

Table: productlines
  Description: Product categories
  Columns:
    - productLine (varchar(50)) [PRIMARY KEY]
    - textDescription (varchar(4000))
"""


def test_single_query(natural_query):
    """Test a single natural language query"""

    print("\n" + "=" * 80)
    print(f"Natural Language: {natural_query}")
    print("=" * 80 + "\n")

    # Check if model exists
    if not os.path.exists(Config.LLM_MODEL_PATH):
        print(f"❌ Model not found at: {Config.LLM_MODEL_PATH}")
        print("\nDownload with:")
        print("wget https://huggingface.co/defog/sqlcoder-7b-2/resolve/main/sqlcoder-7b-q5_k_m.gguf")
        return

    # Initialize LLM
    print("Loading LLM model...")
    try:
        llm_service = get_llm_service()
        print("✓ Model loaded\n")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return

    # Generate SQL
    print("Generating SQL...")
    try:
        sql = llm_service.generate_sql(
            natural_query,
            SCHEMA_DESCRIPTION,
            business_context=""
        )

        print("\n" + "-" * 80)
        print("GENERATED SQL:")
        print("-" * 80)
        print(sql)
        print("-" * 80 + "\n")

    except Exception as e:
        print(f"❌ Generation failed: {e}")
        return

    # Validate
    print("Validating SQL...")
    validator = SQLValidator(VALIDATION_SCHEMA)
    is_valid, errors = validator.validate(sql)

    if is_valid:
        print("✓ SQL is VALID")

        # Show complexity
        cost = validator.estimate_cost(sql)
        print(f"\nComplexity Metrics:")
        print(f"  - Joins: {cost['join_count']}")
        print(f"  - Subquery Depth: {cost['subquery_depth']}")
        print(f"  - Complexity Score: {cost['estimated_complexity']}")

    else:
        print("❌ VALIDATION ERRORS:")
        for error in errors:
            print(f"  - {error}")

    print("\n" + "=" * 80 + "\n")
    return sql if is_valid else None


def interactive_mode():
    """Interactive testing mode"""
    print("\n" + "=" * 80)
    print("NL2SQL INTERACTIVE TEST MODE")
    print("=" * 80)
    print("\nEnter natural language queries to generate SQL")
    print("Type 'exit' or 'quit' to stop")
    print("Type 'examples' to see sample queries\n")

    while True:
        query = input("Your query: ").strip()

        if query.lower() in ['exit', 'quit']:
            print("\nGoodbye!")
            break

        if query.lower() == 'examples':
            print("\nSample Queries:")
            print("  1. Show customers from USA")
            print("  2. What are the top 5 products by quantity in stock?")
            print("  3. List all employees and their managers")
            print("  4. Find customers with credit limit greater than 50000")
            print("  5. Show me total sales by product line")
            print("  6. Which orders are still in process?")
            print("  7. Get all sales representatives")
            print("  8. Show the most expensive product in each category")
            print()
            continue

        if not query:
            continue

        test_single_query(query)


def batch_test():
    """Run predefined test queries"""

    queries = [
        "Show me all customers from USA",
        "What are the top 5 products by quantity in stock?",
        "List all employees and their managers",
        "Find customers with credit limit greater than 50000",
        "Which orders are still in process?",
    ]

    print("\n" + "=" * 80)
    print(f"RUNNING {len(queries)} TEST QUERIES")
    print("=" * 80)

    results = []
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] Testing: {query}")
        sql = test_single_query(query)
        results.append({
            'query': query,
            'sql': sql,
            'success': sql is not None
        })

        if i < len(queries):
            input("\nPress Enter for next query...")

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    successful = sum(1 for r in results if r['success'])
    print(f"Successful: {successful}/{len(queries)}")

    for i, r in enumerate(results, 1):
        status = "✓" if r['success'] else "✗"
        print(f"{status} {i}. {r['query']}")

    print("=" * 80 + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Command line query
        query = " ".join(sys.argv[1:])
        test_single_query(query)
    else:
        # Interactive mode
        print("\nSelect mode:")
        print("1. Interactive mode (enter queries one by one)")
        print("2. Batch test (run predefined queries)")

        choice = input("\nChoice (1 or 2): ").strip()

        if choice == "2":
            batch_test()
        else:
            interactive_mode()
