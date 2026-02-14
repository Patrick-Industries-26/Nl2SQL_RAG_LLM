from llama_cpp import Llama
import os
from config import Config
import logging

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self):
        self.model = None
        self.model_path = Config.LLM_MODEL_PATH
        self._initialize_model()

    def _initialize_model(self):
        """Initialize SQLCoder model with llama.cpp"""
        try:
            if not os.path.exists(self.model_path):
                raise FileNotFoundError(
                    f"Model not found at {self.model_path}. "
                    "Please download SQLCoder GGUF model first."
                )

            logger.info(f"Loading SQLCoder model from {self.model_path}")

            self.model = Llama(
                model_path=self.model_path,
                n_ctx=Config.LLM_CONTEXT_SIZE,
                n_gpu_layers=Config.LLM_GPU_LAYERS,  # Optimized for 128MB VRAM
                n_batch=512,
                n_threads=4,
                verbose=False
            )

            logger.info("SQLCoder model loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load SQLCoder model: {str(e)}")
            raise

    def generate_sql(self, natural_query: str, schema_context: str,
                     business_context: str = "") -> str:
        """
        Generate SQL query from natural language using SQLCoder

        Args:
            natural_query: Natural language question
            schema_context: Database schema information from RAG
            business_context: Business logic and domain knowledge

        Returns:
            Generated SQL query
        """
        try:
            # Construct prompt for SQLCoder
            prompt = self._build_prompt(natural_query, schema_context, business_context)

            print("prompt: " + prompt)

            # Generate SQL
            response = self.model(
                prompt,
                max_tokens=Config.LLM_MAX_TOKENS,
                temperature=Config.LLM_TEMPERATURE,
                top_p=0.95,
                stop=["</s>", ";", "\n\n"],
                echo=False
            )

            # Extract SQL from response
            sql_query = self._extract_sql(response['choices'][0]['text'])

            return sql_query.strip()

        except Exception as e:
            logger.error(f"SQL generation failed: {str(e)}")
            raise

    def _build_prompt(self, question: str, schema: str, business_context: str) -> str:
        """
        Build SQLCoder-specific prompt optimized for MySQL

        SQLCoder expects a specific format with schema and question
        """
        prompt = f"""### Task
Generate a MySQL query to answer the following question: `{question}`

### Database Schema
The query will run on a MySQL database with the following schema:
{schema}

"""

        if business_context:
            prompt += f"""### Business Context
{business_context}

"""

        prompt += """### Critical MySQL Syntax Rules
1. Use ONLY MySQL-compatible syntax
2. DO NOT use PostgreSQL syntax like ILIKE, NULLS LAST, NULLS FIRST
3. For case-insensitive search: Use LIKE (MySQL is case-insensitive by default)
4. For ordering with nulls: Use IS NULL conditions in ORDER BY
5. DO NOT mix columns from different tables without proper JOINs
6. Always qualify column names with table aliases when using JOINs
7. Use only columns that exist in the specified table

### MySQL Syntax Examples
-- Case-insensitive search (MySQL default):
SELECT * FROM customers WHERE country LIKE '%USA%';

-- NOT: SELECT * FROM customers WHERE country ILIKE '%USA%';

-- Ordering with nulls:
SELECT * FROM employees ORDER BY reportsTo IS NULL, reportsTo;

-- NOT: SELECT * FROM employees ORDER BY reportsTo NULLS LAST;

-- Proper JOIN with qualified columns:
SELECT e.firstName, e.lastName, o.city 
FROM employees e 
JOIN offices o ON e.officeCode = o.officeCode;

-- NOT: SELECT e.firstName, e.lastName, o.city, e.customerName 
-- (customerName is not in employees table!)

### Important Column Rules
- customers table has: customerNumber, customerName, contactLastName, contactFirstName, phone, addressLine1, addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit
- employees table has: employeeNumber, lastName, firstName, extension, email, officeCode, reportsTo, jobTitle
- products table has: productCode, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice, MSRP
- orders table has: orderNumber, orderDate, requiredDate, shippedDate, status, comments, customerNumber
- DO NOT use columns from one table in another table's query without JOIN

### Instructions
- Generate ONLY MySQL-compatible SELECT queries
- Use proper table and column names from the schema above
- Include appropriate JOINs when accessing multiple tables
- Use WHERE clauses for filtering
- Add GROUP BY for aggregations
- Use ORDER BY for sorting (without NULLS LAST/FIRST)
- Use LIMIT for row limiting
- Ensure all columns used exist in the tables specified
- When joining tables, use table aliases and qualify all column names
- Use LIKE for pattern matching (not ILIKE)

### SQL Query
```sql
"""

        return prompt

    def _extract_sql(self, text: str) -> str:
        """Extract SQL query from model output and clean up syntax"""
        # Remove markdown code blocks if present
        text = text.strip()

        if "```sql" in text:
            parts = text.split("```sql")
            if len(parts) > 1:
                text = parts[1].split("```")[0]
        elif "```" in text:
            parts = text.split("```")
            if len(parts) > 1:
                text = parts[1].split("```")[0]

        # Remove comments
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove SQL comments
            if '--' in line:
                line = line[:line.index('--')]
            if line.strip():
                cleaned_lines.append(line)

        sql = '\n'.join(cleaned_lines).strip()

        # Fix PostgreSQL syntax to MySQL syntax
        sql = self._fix_mysql_syntax(sql)

        # Ensure it ends with semicolon
        if sql and not sql.endswith(';'):
            sql += ';'

        return sql

    def _fix_mysql_syntax(self, sql: str) -> str:
        """Fix common PostgreSQL syntax to MySQL equivalents"""
        import re

        # Replace ILIKE with LIKE (MySQL LIKE is case-insensitive by default)
        sql = re.sub(r'\bILIKE\b', 'LIKE', sql, flags=re.IGNORECASE)

        # Remove NULLS LAST / NULLS FIRST from ORDER BY
        sql = re.sub(r'\s+NULLS\s+(LAST|FIRST)\b', '', sql, flags=re.IGNORECASE)

        # Replace :: type casting with CAST function
        # e.g., column::integer -> CAST(column AS SIGNED)
        sql = re.sub(r'(\w+)::INTEGER', r'CAST(\1 AS SIGNED)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'(\w+)::TEXT', r'CAST(\1 AS CHAR)', sql, flags=re.IGNORECASE)

        # Fix CONCAT operator (|| in PostgreSQL vs CONCAT() in MySQL)
        # This is complex, so we'll leave it for now as it's less common

        return sql

    def validate_model_loaded(self) -> bool:
        """Check if model is loaded successfully"""
        return self.model is not None


# Singleton instance
_llm_service = None


def get_llm_service() -> LLMService:
    """Get or create LLM service instance"""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service
