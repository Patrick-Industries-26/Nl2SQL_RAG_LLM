import pymysql
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from config import Config
import logging

logger = logging.getLogger(__name__)


class SchemaService:
    def __init__(self):
        self.cache_dir = Config.SCHEMA_CACHE_DIR
        self.cache_ttl = Config.SCHEMA_CACHE_TTL
        os.makedirs(self.cache_dir, exist_ok=True)

    def extract_schema(self, connection_params: Dict) -> Dict:
        """
        Extract schema from MySQL database

        Args:
            connection_params: {
                'host': str,
                'port': int,
                'user': str,
                'password': str,
                'database': str
            }

        Returns:
            Dictionary containing schema information
        """
        try:
            # Check cache first
            cache_key = self._get_cache_key(connection_params)
            cached_schema = self._load_from_cache(cache_key)
            if cached_schema:
                logger.info("Loaded schema from cache")
                return cached_schema

            # Connect to database
            connection = pymysql.connect(
                host=connection_params['host'],
                port=connection_params.get('port', 3306),
                user=connection_params['user'],
                password=connection_params['password'],
                database=connection_params['database'],
                cursorclass=pymysql.cursors.DictCursor
            )

            schema = {
                'database_name': connection_params['database'],
                'tables': {},
                'extracted_at': datetime.utcnow().isoformat()
            }

            with connection.cursor() as cursor:
                # Get all tables
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()

                table_key = f"Tables_in_{connection_params['database']}"

                for table_row in tables:
                    table_name = table_row[table_key]

                    # Get table columns
                    cursor.execute(f"DESCRIBE `{table_name}`")
                    columns = cursor.fetchall()

                    # Get table statistics
                    cursor.execute(f"""
                        SELECT TABLE_COMMENT, TABLE_ROWS, DATA_LENGTH
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    """, (connection_params['database'], table_name))
                    table_info = cursor.fetchone()

                    # Get foreign keys
                    cursor.execute(f"""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = %s 
                        AND TABLE_NAME = %s
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    """, (connection_params['database'], table_name))
                    foreign_keys = cursor.fetchall()

                    # Build column information
                    column_list = []
                    primary_key = None
                    fk_map = {fk['COLUMN_NAME']: {
                        'ref_table': fk['REFERENCED_TABLE_NAME'],
                        'ref_column': fk['REFERENCED_COLUMN_NAME']
                    } for fk in foreign_keys}

                    for col in columns:
                        is_pk = col['Key'] == 'PRI'
                        is_fk = col['Field'] in fk_map

                        column_info = {
                            'name': col['Field'],
                            'type': col['Type'],
                            'nullable': col['Null'] == 'YES',
                            'default': col['Default'],
                            'is_primary_key': is_pk,
                            'is_foreign_key': is_fk,
                            'foreign_key_ref': None
                        }

                        if is_pk:
                            primary_key = col['Field']

                        if is_fk:
                            fk_info = fk_map[col['Field']]
                            column_info['foreign_key_ref'] = (
                                f"{fk_info['ref_table']}.{fk_info['ref_column']}"
                            )

                        column_list.append(column_info)

                    # Add table to schema
                    schema['tables'][table_name] = {
                        'columns': column_list,
                        'primary_key': primary_key,
                        'row_count': table_info['TABLE_ROWS'] if table_info else 0,
                        'size_bytes': table_info['DATA_LENGTH'] if table_info else 0,
                        'comment': table_info['TABLE_COMMENT'] if table_info else ''
                    }

            connection.close()

            # Cache the schema
            self._save_to_cache(cache_key, schema)

            logger.info(f"Extracted schema with {len(schema['tables'])} tables")
            return schema

        except Exception as e:
            logger.error(f"Schema extraction failed: {str(e)}")
            raise

    def format_schema_for_llm(self, schema: Dict,
                              metadata: List[Dict] = None) -> str:
        """
        Format schema for LLM consumption

        Args:
            schema: Schema dictionary
            metadata: Optional custom metadata

        Returns:
            Formatted schema string
        """
        formatted = []

        # Create metadata lookup
        metadata_map = {}
        if metadata:
            for item in metadata:
                key = (item['table_name'], item.get('column_name'))
                metadata_map[key] = item

        for table_name, table_info in schema['tables'].items():
            formatted.append(f"\nTable: {table_name}")

            # Add table-level metadata
            table_meta = metadata_map.get((table_name, None))
            if table_meta and table_meta.get('description'):
                formatted.append(f"  Description: {table_meta['description']}")
            elif table_info.get('comment'):
                formatted.append(f"  Description: {table_info['comment']}")

            if table_meta and table_meta.get('business_logic'):
                formatted.append(f"  Business Logic: {table_meta['business_logic']}")

            # Add columns
            formatted.append("  Columns:")
            for col in table_info['columns']:
                col_name = col['name']
                col_type = col['type']

                col_str = f"    - {col_name} ({col_type})"

                # Add column metadata
                col_meta = metadata_map.get((table_name, col_name))
                if col_meta and col_meta.get('description'):
                    col_str += f" - {col_meta['description']}"

                if col['is_primary_key']:
                    col_str += " [PRIMARY KEY]"

                if col['is_foreign_key']:
                    col_str += f" [FK -> {col['foreign_key_ref']}]"

                formatted.append(col_str)

        return '\n'.join(formatted)

    def get_schema_for_validation(self, schema: Dict) -> Dict:
        """
        Get schema in format suitable for validator

        Returns:
            {
                'table_name': {
                    'columns': ['col1', 'col2', ...],
                    'primary_key': 'id',
                    'foreign_keys': {...}
                }
            }
        """
        validation_schema = {}

        for table_name, table_info in schema['tables'].items():
            validation_schema[table_name] = {
                'columns': [col['name'] for col in table_info['columns']],
                'primary_key': table_info['primary_key'],
                'foreign_keys': {}
            }

            # Add foreign key information
            for col in table_info['columns']:
                if col['is_foreign_key']:
                    validation_schema[table_name]['foreign_keys'][col['name']] = \
                        col['foreign_key_ref']

        return validation_schema

    def _get_cache_key(self, connection_params: Dict) -> str:
        """Generate cache key from connection parameters"""
        return f"{connection_params['host']}_{connection_params['database']}"

    def _load_from_cache(self, cache_key: str) -> Dict:
        """Load schema from cache if valid"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")

        if not os.path.exists(cache_file):
            return None

        try:
            # Check if cache is still valid
            file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_mtime > timedelta(seconds=self.cache_ttl):
                logger.info("Cache expired")
                return None

            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load cache: {str(e)}")
            return None

    def _save_to_cache(self, cache_key: str, schema: Dict):
        """Save schema to cache"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")

        try:
            with open(cache_file, 'w') as f:
                json.dump(schema, f, indent=2)
            logger.info(f"Schema cached to {cache_file}")
        except Exception as e:
            logger.error(f"Failed to save cache: {str(e)}")

    def invalidate_cache(self, connection_params: Dict):
        """Invalidate cache for a connection"""
        cache_key = self._get_cache_key(connection_params)
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")

        if os.path.exists(cache_file):
            os.remove(cache_file)
            logger.info(f"Cache invalidated for {cache_key}")

    def test_connection(self, connection_params: Dict) -> Tuple[bool, str]:
        """
        Test database connection

        Returns:
            (success, message)
        """
        try:
            connection = pymysql.connect(
                host=connection_params['host'],
                port=connection_params.get('port', 3306),
                user=connection_params['user'],
                password=connection_params['password'],
                database=connection_params['database'],
                connect_timeout=5
            )
            connection.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
