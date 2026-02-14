"""
Helper functions for building RAG index from schema
"""

from services.rag_service import get_rag_service
import logging

logger = logging.getLogger(__name__)

# Sample metadata - can be customized per database
DEFAULT_METADATA = [
    {
        'table_name': 'customers',
        'column_name': None,
        'description': 'Stores customer contact information and details',
        'business_logic': 'Customers are assigned to sales representatives. Credit limit determines maximum order value.',
        'examples': []
    },
    {
        'table_name': 'employees',
        'column_name': None,
        'description': 'Stores employee information and organizational hierarchy',
        'business_logic': 'Employees report to managers via reportsTo field',
        'examples': []
    },
    {
        'table_name': 'products',
        'column_name': None,
        'description': 'Product catalog with pricing and inventory',
        'business_logic': 'MSRP is suggested retail price, buyPrice is wholesale cost',
        'examples': []
    },
    {
        'table_name': 'orders',
        'column_name': None,
        'description': 'Customer orders with status tracking',
        'business_logic': 'Status values: Shipped, Cancelled, In Process, On Hold, Resolved, Disputed',
        'examples': []
    }
]


def build_rag_index_from_schema(schema, connection_id):
    """
    Build RAG index from database schema

    Args:
        schema: Schema dictionary from schema_service
        connection_id: ID for storing the index
    """
    rag_service = get_rag_service()

    # Prepare schema data for RAG
    schema_data = []

    for table_name, table_info in schema['tables'].items():
        # Find metadata for this table
        table_meta = next(
            (m for m in DEFAULT_METADATA
             if m['table_name'] == table_name and m.get('column_name') is None),
            None
        )

        # Table-level entry
        schema_data.append({
            'table_name': table_name,
            'column_name': None,
            'description': table_meta['description'] if table_meta else table_info.get('comment', ''),
            'business_logic': table_meta['business_logic'] if table_meta else '',
            'examples': []
        })

        # Column-level entries
        for col in table_info['columns']:
            schema_data.append({
                'table_name': table_name,
                'column_name': col['name'],
                'data_type': col['type'],
                'description': '',
                'business_logic': '',
                'examples': [],
                'is_primary_key': col.get('is_primary_key', False),
                'is_foreign_key': col.get('is_foreign_key', False),
                'foreign_key_ref': col.get('foreign_key_ref')
            })

    # Build and save index
    rag_service.build_index(schema_data, connection_id)
    logger.info(f"✓ RAG index built with {len(schema_data)} documents")