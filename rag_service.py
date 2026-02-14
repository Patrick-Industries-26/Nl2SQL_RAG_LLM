import faiss
import numpy as np
import pickle
import os
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Tuple
from config import Config
import logging

logger = logging.getLogger(__name__)


class RAGService:
    def __init__(self):
        self.embedding_model = None
        self.index = None
        self.documents = []
        self.metadata = []
        self.index_path = Config.FAISS_INDEX_PATH
        self._initialize_embedding_model()

    def _initialize_embedding_model(self):
        """Initialize sentence transformer for embeddings"""
        try:
            logger.info(f"Loading embedding model: {Config.EMBEDDING_MODEL}")
            self.embedding_model = SentenceTransformer(Config.EMBEDDING_MODEL)
            logger.info("Embedding model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {str(e)}")
            raise

    def build_index(self, schema_data: List[Dict], connection_id: int):
        """
        Build FAISS index from schema and metadata

        Args:
            schema_data: List of schema information dictionaries
            connection_id: Database connection ID
        """
        try:
            documents = []
            metadata = []

            # Process schema data into documents
            for item in schema_data:
                # Create document for each table/column with metadata
                doc_text = self._create_document_text(item)
                documents.append(doc_text)
                metadata.append({
                    'connection_id': connection_id,
                    'table_name': item.get('table_name'),
                    'column_name': item.get('column_name'),
                    'type': 'column' if item.get('column_name') else 'table'
                })

            if not documents:
                logger.warning("No documents to index")
                return

            # Generate embeddings
            logger.info(f"Generating embeddings for {len(documents)} documents")
            embeddings = self.embedding_model.encode(
                documents,
                show_progress_bar=True,
                convert_to_numpy=True
            )

            # Create FAISS index
            dimension = embeddings.shape[1]
            self.index = faiss.IndexFlatL2(dimension)
            self.index.add(embeddings.astype('float32'))

            self.documents = documents
            self.metadata = metadata

            # Save index
            self._save_index(connection_id)

            logger.info(f"FAISS index built with {len(documents)} documents")

        except Exception as e:
            logger.error(f"Failed to build FAISS index: {str(e)}")
            raise

    def _create_document_text(self, item: Dict) -> str:
        """Create searchable document text from schema item"""
        parts = []

        # Table information
        table_name = item.get('table_name', '')
        if table_name:
            parts.append(f"TABLE: {table_name}")

        # Column information
        if item.get('column_name'):
            column_name = item['column_name']
            parts.append(f"COLUMN: {table_name}.{column_name}")

            if item.get('data_type'):
                parts.append(f"Data Type: {item['data_type']}")

            # Emphasize table ownership
            parts.append(f"This column belongs to the {table_name} table")
        else:
            # Table-level entry
            parts.append(f"This is the {table_name} table")

        # Description
        if item.get('description'):
            parts.append(f"Description: {item['description']}")

        # Business logic
        if item.get('business_logic'):
            parts.append(f"Business Logic: {item['business_logic']}")

        # Examples
        if item.get('examples') and len(item['examples']) > 0:
            examples_str = ', '.join(str(e) for e in item['examples'][:5])
            parts.append(f"Example values: {examples_str}")

        # Key information
        if item.get('is_primary_key'):
            parts.append(f"Primary Key of {table_name}")
        if item.get('is_foreign_key') and item.get('foreign_key_ref'):
            parts.append(f"Foreign Key: References {item['foreign_key_ref']}")

        return '. '.join(parts)

    def search(self, query: str, top_k: int = None) -> List[Tuple[str, Dict, float]]:
        """
        Search for relevant schema information

        Args:
            query: Natural language query
            top_k: Number of results to return

        Returns:
            List of (document_text, metadata, distance) tuples
        """
        if top_k is None:
            top_k = Config.RAG_TOP_K

        try:
            if self.index is None or len(self.documents) == 0:
                logger.warning("Index not initialized")
                return []

            # Generate query embedding
            query_embedding = self.embedding_model.encode(
                [query],
                convert_to_numpy=True
            )

            # Search index
            distances, indices = self.index.search(
                query_embedding.astype('float32'),
                min(top_k, len(self.documents))
            )

            # Prepare results
            results = []
            for dist, idx in zip(distances[0], indices[0]):
                if idx < len(self.documents):
                    results.append((
                        self.documents[idx],
                        self.metadata[idx],
                        float(dist)
                    ))

            return results

        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return []

    def get_schema_context(self, query: str) -> str:
        """
        Get formatted schema context for LLM

        Args:
            query: Natural language query

        Returns:
            Formatted schema string with clear table separation
        """
        results = self.search(query)

        if not results:
            return "No schema information available."

        # Group by tables and track which tables are most relevant
        tables = {}
        table_relevance = {}

        for doc, meta, dist in results:
            table_name = meta.get('table_name')
            if table_name not in tables:
                tables[table_name] = {
                    'columns': [],
                    'description': '',
                    'table_doc': ''
                }
                table_relevance[table_name] = []

            # Track relevance (lower distance = more relevant)
            table_relevance[table_name].append(dist)

            if meta.get('type') == 'column':
                # Add column with full context
                column_info = doc.replace(f'TABLE: {table_name}. ', '')
                tables[table_name]['columns'].append(column_info)
            else:
                # Table-level info
                tables[table_name]['table_doc'] = doc

        # Sort tables by relevance (average distance)
        sorted_tables = sorted(
            tables.keys(),
            key=lambda t: sum(table_relevance[t]) / len(table_relevance[t])
        )

        # Format schema with clear separation
        schema_parts = ["=== RELEVANT TABLES ===\n"]

        for table_name in sorted_tables:
            info = tables[table_name]
            schema_parts.append(f"\n--- TABLE: {table_name} ---")

            if info['table_doc']:
                # Extract description from table doc
                if 'Description:' in info['table_doc']:
                    desc_part = info['table_doc'].split('Description:')[1].split('.')[0]
                    schema_parts.append(f"Description: {desc_part}")
                if 'Business Logic:' in info['table_doc']:
                    logic_parts = info['table_doc'].split('Business Logic:')[1].split('.')
                    if logic_parts[0].strip():
                        schema_parts.append(f"Business Logic: {logic_parts[0].strip()}")

            if info['columns']:
                schema_parts.append(f"Columns in {table_name}:")
                for col in info['columns']:
                    schema_parts.append(f"  * {col}")

        schema_parts.append("\n=== END OF SCHEMA ===")

        return '\n'.join(schema_parts)

    def get_business_context(self, query: str) -> str:
        """
        Get business logic context for LLM

        Args:
            query: Natural language query

        Returns:
            Business logic string
        """
        results = self.search(query)

        business_logic = []
        for doc, meta, dist in results:
            # Extract business logic from document
            if 'Business Logic:' in doc:
                parts = doc.split('Business Logic:')
                if len(parts) > 1:
                    logic = parts[1].split('.')[0].strip()
                    if logic and logic not in business_logic:
                        business_logic.append(logic)

        return '\n- '.join([''] + business_logic) if business_logic else ''

    def load_index(self, connection_id: int) -> bool:
        """Load existing FAISS index"""
        try:
            index_file = os.path.join(
                self.index_path,
                f'index_{connection_id}.faiss'
            )
            metadata_file = os.path.join(
                self.index_path,
                f'metadata_{connection_id}.pkl'
            )
            docs_file = os.path.join(
                self.index_path,
                f'documents_{connection_id}.pkl'
            )

            if not all(os.path.exists(f) for f in [index_file, metadata_file, docs_file]):
                return False

            self.index = faiss.read_index(index_file)

            with open(metadata_file, 'rb') as f:
                self.metadata = pickle.load(f)

            with open(docs_file, 'rb') as f:
                self.documents = pickle.load(f)

            logger.info(f"Loaded FAISS index for connection {connection_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to load index: {str(e)}")
            return False

    def _save_index(self, connection_id: int):
        """Save FAISS index to disk"""
        try:
            os.makedirs(self.index_path, exist_ok=True)

            index_file = os.path.join(
                self.index_path,
                f'index_{connection_id}.faiss'
            )
            metadata_file = os.path.join(
                self.index_path,
                f'metadata_{connection_id}.pkl'
            )
            docs_file = os.path.join(
                self.index_path,
                f'documents_{connection_id}.pkl'
            )

            faiss.write_index(self.index, index_file)

            with open(metadata_file, 'wb') as f:
                pickle.dump(self.metadata, f)

            with open(docs_file, 'wb') as f:
                pickle.dump(self.documents, f)

            logger.info(f"Saved FAISS index for connection {connection_id}")

        except Exception as e:
            logger.error(f"Failed to save index: {str(e)}")
            raise


# Singleton instance
_rag_service = None


def get_rag_service() -> RAGService:
    """Get or create RAG service instance"""
    global _rag_service
    if _rag_service is None:
        _rag_service = RAGService()
    return _rag_service
