import pymysql
import pandas as pd
from typing import Dict, List, Any
import time
import signal
from contextlib import contextmanager
from config import Config
import logging

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


@contextmanager
def timeout(seconds):
    """Context manager for query timeout"""

    def timeout_handler(signum, frame):
        raise TimeoutException("Query execution timeout")

    # Set the signal handler
    original_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


class QueryExecutor:
    def __init__(self):
        self.max_rows = Config.MAX_QUERY_ROWS
        self.query_timeout = Config.QUERY_TIMEOUT

    def execute_query(self, connection_params: Dict, sql_query: str) -> Dict:
        """
        Execute SQL query with timeout and result limiting

        Args:
            connection_params: Database connection parameters
            sql_query: SQL query to execute

        Returns:
            {
                'success': bool,
                'data': list of dicts,
                'columns': list of column names,
                'row_count': int,
                'execution_time_ms': int,
                'truncated': bool,
                'error': str (if failed)
            }
        """
        start_time = time.time()

        try:
            # Ensure query has LIMIT clause
            limited_query = self._add_limit_clause(sql_query)

            # Connect to database
            connection = pymysql.connect(
                host=connection_params['host'],
                port=connection_params.get('port', 3306),
                user=connection_params['user'],
                password=connection_params['password'],
                database=connection_params['database'],
                cursorclass=pymysql.cursors.DictCursor
            )

            result = {
                'success': False,
                'data': [],
                'columns': [],
                'row_count': 0,
                'execution_time_ms': 0,
                'truncated': False,
                'error': None
            }

            try:
                with timeout(self.query_timeout):
                    with connection.cursor() as cursor:
                        cursor.execute(limited_query)
                        rows = cursor.fetchall()

                        if rows:
                            result['data'] = rows
                            result['columns'] = list(rows[0].keys())
                            result['row_count'] = len(rows)
                            result['truncated'] = len(rows) >= self.max_rows

                        result['success'] = True

            except TimeoutException:
                result['error'] = f"Query exceeded timeout of {self.query_timeout} seconds"
                logger.warning(f"Query timeout: {sql_query[:100]}")

            finally:
                connection.close()

            execution_time = int((time.time() - start_time) * 1000)
            result['execution_time_ms'] = execution_time

            return result

        except pymysql.Error as e:
            logger.error(f"Database error: {str(e)}")
            return {
                'success': False,
                'data': [],
                'columns': [],
                'row_count': 0,
                'execution_time_ms': int((time.time() - start_time) * 1000),
                'truncated': False,
                'error': f"Database error: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            return {
                'success': False,
                'data': [],
                'columns': [],
                'row_count': 0,
                'execution_time_ms': int((time.time() - start_time) * 1000),
                'truncated': False,
                'error': f"Execution error: {str(e)}"
            }

    def _add_limit_clause(self, sql_query: str) -> str:
        """Add LIMIT clause to query if not present"""
        sql_upper = sql_query.upper().strip()

        # Remove trailing semicolon
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1].strip()

        # Check if LIMIT already exists
        if 'LIMIT' not in sql_upper:
            sql_query += f' LIMIT {self.max_rows}'
        else:
            # Check if LIMIT exceeds max
            import re
            limit_match = re.search(r'LIMIT\s+(\d+)', sql_upper)
            if limit_match:
                limit_value = int(limit_match.group(1))
                if limit_value > self.max_rows:
                    sql_query = re.sub(
                        r'LIMIT\s+\d+',
                        f'LIMIT {self.max_rows}',
                        sql_query,
                        flags=re.IGNORECASE
                    )

        return sql_query

    def export_to_csv(self, data: List[Dict], columns: List[str]) -> str:
        """
        Export query results to CSV

        Returns:
            CSV string
        """
        try:
            df = pd.DataFrame(data, columns=columns)
            return df.to_csv(index=False)
        except Exception as e:
            logger.error(f"CSV export failed: {str(e)}")
            raise

    def export_to_json(self, data: List[Dict]) -> str:
        """
        Export query results to JSON

        Returns:
            JSON string
        """
        try:
            import json
            return json.dumps(data, indent=2, default=str)
        except Exception as e:
            logger.error(f"JSON export failed: {str(e)}")
            raise

    def prepare_chart_data(self, data: List[Dict],
                           chart_type: str) -> Dict:
        """
        Prepare data for chart visualization

        Args:
            data: Query results
            chart_type: 'bar', 'line', 'pie', 'area'

        Returns:
            Chart configuration
        """
        try:
            if not data:
                return {'data': [], 'type': chart_type}

            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data)

            if chart_type == 'pie':
                # For pie charts, use first column as label, second as value
                if len(df.columns) >= 2:
                    chart_data = []
                    for _, row in df.iterrows():
                        chart_data.append({
                            'name': str(row[df.columns[0]]),
                            'value': float(row[df.columns[1]])
                        })
                    return {'data': chart_data, 'type': 'pie'}

            else:  # bar, line, area
                # Use first column as X-axis
                x_column = df.columns[0]

                # All other numeric columns as Y-axis values
                chart_data = []
                for _, row in df.iterrows():
                    data_point = {x_column: str(row[x_column])}

                    for col in df.columns[1:]:
                        try:
                            data_point[col] = float(row[col])
                        except (ValueError, TypeError):
                            data_point[col] = str(row[col])

                    chart_data.append(data_point)

                return {
                    'data': chart_data,
                    'type': chart_type,
                    'x_key': x_column,
                    'y_keys': list(df.columns[1:])
                }

            return {'data': [], 'type': chart_type}

        except Exception as e:
            logger.error(f"Chart data preparation failed: {str(e)}")
            return {'data': [], 'type': chart_type, 'error': str(e)}
