import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function
from sqlparse.tokens import Keyword, DML
import re
from typing import Dict, List, Tuple
from config import Config
import logging

logger = logging.getLogger(__name__)


class SQLValidator:
    def __init__(self, schema_info: Dict):
        """
        Initialize validator with schema information

        Args:
            schema_info: Dictionary containing tables and columns
                {
                    'table_name': {
                        'columns': ['col1', 'col2'],
                        'primary_key': 'id',
                        'foreign_keys': {...}
                    }
                }
        """
        self.schema_info = schema_info
        self.max_joins = Config.MAX_JOINS
        self.max_subquery_depth = Config.MAX_SUBQUERY_DEPTH

    def validate(self, sql_query: str) -> Tuple[bool, List[str]]:
        """
        Validate SQL query against all constraints

        Returns:
            (is_valid, list_of_errors)
        """
        errors = []

        try:
            # Parse SQL
            parsed = sqlparse.parse(sql_query)
            if not parsed:
                return False, ["Invalid SQL syntax"]

            statement = parsed[0]

            # 1. Check if SELECT only
            if not self._is_select_only(statement):
                errors.append("Only SELECT queries are allowed")

            # 2. Check query complexity
            complexity_errors = self._check_complexity(statement)
            errors.extend(complexity_errors)

            # 3. Validate tables and columns exist in schema
            schema_errors = self._validate_schema(statement)
            errors.extend(schema_errors)

            # 4. Check for dangerous patterns
            danger_errors = self._check_dangerous_patterns(sql_query)
            errors.extend(danger_errors)

            is_valid = len(errors) == 0
            return is_valid, errors

        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return False, [f"Validation failed: {str(e)}"]

    def _is_select_only(self, statement) -> bool:
        """Check if query is SELECT only"""
        first_token = statement.token_first(skip_ws=True, skip_cm=True)
        if not first_token:
            return False

        return first_token.ttype is DML and first_token.value.upper() == 'SELECT'

    def _check_complexity(self, statement) -> List[str]:
        """Check query complexity constraints"""
        errors = []

        # Count JOINs
        join_count = self._count_joins(statement)
        if join_count > self.max_joins:
            errors.append(
                f"Too many JOINs: {join_count} (max allowed: {self.max_joins})"
            )

        # Count subquery depth
        subquery_depth = self._count_subquery_depth(statement)
        if subquery_depth > self.max_subquery_depth:
            errors.append(
                f"Subquery nesting too deep: {subquery_depth} "
                f"(max allowed: {self.max_subquery_depth})"
            )

        return errors

    def _count_joins(self, statement) -> int:
        """Count number of JOINs in query"""
        join_count = 0
        sql_str = str(statement).upper()

        # Count different types of joins
        join_patterns = [
            r'\bINNER\s+JOIN\b',
            r'\bLEFT\s+JOIN\b',
            r'\bRIGHT\s+JOIN\b',
            r'\bFULL\s+JOIN\b',
            r'\bCROSS\s+JOIN\b',
            r'\bJOIN\b'
        ]

        for pattern in join_patterns:
            join_count += len(re.findall(pattern, sql_str))

        return join_count

    def _count_subquery_depth(self, statement, current_depth=0) -> int:
        """Recursively count maximum subquery nesting depth"""
        max_depth = current_depth

        for token in statement.tokens:
            if token.ttype is None:
                # Check if it's a subquery (parenthesized statement)
                if hasattr(token, 'tokens'):
                    token_str = str(token).strip()
                    if token_str.startswith('(') and 'SELECT' in token_str.upper():
                        depth = self._count_subquery_depth(token, current_depth + 1)
                        max_depth = max(max_depth, depth)
                    else:
                        depth = self._count_subquery_depth(token, current_depth)
                        max_depth = max(max_depth, depth)

        return max_depth

    def _validate_schema(self, statement) -> List[str]:
        """Validate that all tables and columns exist in schema"""
        errors = []

        try:
            # Extract table names
            tables = self._extract_tables(statement)

            # Validate tables
            for table in tables:
                if table not in self.schema_info:
                    errors.append(f"Table '{table}' not found in schema")

            # Extract columns
            columns = self._extract_columns(statement)

            # Validate columns
            for table, column in columns:
                if table and table in self.schema_info:
                    if column not in self.schema_info[table]['columns']:
                        errors.append(
                            f"Column '{column}' not found in table '{table}'"
                        )
                elif not table:
                    # Column without table qualifier - check all tables
                    found = False
                    for tbl_name, tbl_info in self.schema_info.items():
                        if column in tbl_info['columns']:
                            found = True
                            break
                    if not found:
                        errors.append(f"Column '{column}' not found in any table")

        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}")
            errors.append(f"Schema validation failed: {str(e)}")

        return errors

    def _extract_tables(self, statement) -> List[str]:
        """Extract table names from SQL statement"""
        tables = []

        from_seen = False
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        table_name = self._get_table_name(identifier)
                        if table_name:
                            tables.append(table_name)
                elif isinstance(token, Identifier):
                    table_name = self._get_table_name(token)
                    if table_name:
                        tables.append(table_name)
                from_seen = False

            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True

            # Handle JOINs
            if token.ttype is Keyword and 'JOIN' in token.value.upper():
                from_seen = True

        return tables

    def _get_table_name(self, identifier) -> str:
        """Extract table name from identifier"""
        name = identifier.get_real_name()
        if name:
            return name.strip('`"[]')
        return None

    def _extract_columns(self, statement) -> List[Tuple[str, str]]:
        """
        Extract columns from SQL statement

        Returns:
            List of (table_name, column_name) tuples
            table_name can be None for unqualified columns
        """
        columns = []

        for token in statement.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    col = self._parse_identifier(identifier)
                    if col:
                        columns.append(col)
            elif isinstance(token, Identifier):
                col = self._parse_identifier(token)
                if col:
                    columns.append(col)

        return columns

    def _parse_identifier(self, identifier) -> Tuple[str, str]:
        """Parse identifier to extract table and column name"""
        name = str(identifier)

        # Handle table.column format
        if '.' in name:
            parts = name.split('.')
            if len(parts) == 2:
                table = parts[0].strip('`"[]')
                column = parts[1].strip('`"[]')
                return (table, column)
        else:
            # Column without table
            column = name.strip('`"[]')
            # Filter out keywords and functions
            if column.upper() not in ['SELECT', 'FROM', 'WHERE', 'AS', '*']:
                return (None, column)

        return None

    def _check_dangerous_patterns(self, sql_query: str) -> List[str]:
        """Check for potentially dangerous SQL patterns"""
        errors = []
        sql_upper = sql_query.upper()

        # Check for multiple statements
        if ';' in sql_query[:-1]:  # Allow trailing semicolon
            errors.append("Multiple SQL statements not allowed")

        # Check for SQL injection patterns
        dangerous_patterns = [
            (r'--', "SQL comments not allowed"),
            (r'/\*', "Multi-line comments not allowed"),
            (r'\bEXEC\b', "EXEC command not allowed"),
            (r'\bEXECUTE\b', "EXECUTE command not allowed"),
            (r'\bINTO\s+OUTFILE\b', "INTO OUTFILE not allowed"),
            (r'\bLOAD_FILE\b', "LOAD_FILE not allowed"),
        ]

        for pattern, message in dangerous_patterns:
            if re.search(pattern, sql_upper):
                errors.append(message)

        return errors

    def estimate_cost(self, sql_query: str) -> Dict:
        """
        Estimate query cost/complexity

        Returns:
            Dictionary with cost metrics
        """
        try:
            parsed = sqlparse.parse(sql_query)[0]

            return {
                'join_count': self._count_joins(parsed),
                'subquery_depth': self._count_subquery_depth(parsed),
                'has_aggregation': 'GROUP BY' in sql_query.upper(),
                'has_order': 'ORDER BY' in sql_query.upper(),
                'estimated_complexity': self._calculate_complexity_score(parsed)
            }
        except:
            return {'estimated_complexity': 0}

    def _calculate_complexity_score(self, statement) -> int:
        """Calculate a simple complexity score"""
        score = 1  # Base score

        score += self._count_joins(statement) * 2
        score += self._count_subquery_depth(statement) * 3

        sql_str = str(statement).upper()
        if 'GROUP BY' in sql_str:
            score += 2
        if 'ORDER BY' in sql_str:
            score += 1
        if 'DISTINCT' in sql_str:
            score += 1

        return score
