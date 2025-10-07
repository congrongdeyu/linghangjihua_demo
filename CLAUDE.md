# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Chinese government policy document processing system that builds knowledge graphs and vector databases from AI-related policy documents. The system implements a complete pipeline from raw document ingestion to structured knowledge representations.

## Environment Setup

**Required Environment Variables in `.env`:**
```
ZHIPUAI_API_KEY="your_zhipuai_api_key"
```

**Key Dependencies:**
```bash
pip install langchain-chroma langchain-community langchain-experimental langchain-kuzu
pip install kuzu python-dotenv requests tqdm
pip install langchain-text-splitters langchain_core
```

## Core Pipeline Commands

Execute in sequence for full document processing:

```bash
# 1. Document metadata creation
python 00_create_metadata_for_raw_files.py

# 2. File acquisition (requires API access)
python 02_download_mineru_files.py
python 03_unzip_mineru_files_and_rename_md_file.py

# 3. Document processing and enhancement
python 01_use_mineru_process_raw_files.py
python 04_use_llm_structure_markdown_files.py

# 4. Content structuring
python 05_chunk_md_files_and_store_chunks.py

# 5. Database creation
python 06_create_vector_database_from_chunks.py
python 07_create_knowledge_graph_from_chunks.py

# 6. Testing
python test_graph_creation.py
```

## Architecture

**Data Flow:**
1. `knowledge_base/01_raw_files/` - Original PDFs and MD files
2. `knowledge_base/02_raw_md_files/` - Processed Markdown files
3. `knowledge_base/03_structure_md_files/` - Structured content
4. `knowledge_base/04_database/01_langchain_split_documents_files/` - Chunked documents (.pkl)
5. `knowledge_base/04_database/02_vector_chroma_db/` - Chroma vector database
6. `knowledge_base/04_database/03_kg_kuzu_db/` - Kuzu knowledge graph

**Key Technologies:**
- **LangChain**: Document processing with MarkdownHeaderTextSplitter
- **ZhipuAI**: Chinese LLM for embeddings and content enhancement
- **Chroma**: Vector database for semantic search
- **KuzuDB**: Graph database for knowledge relationships
- **LLMGraphTransformer**: Converts documents to knowledge graphs

## Important Files

- `knowledge_base/metadata.json` - Document UUID mapping and provenance
- `create_knowledge_graph.py:68-92` - Graph schema generation logic
- `chunk_and_store.py:25-45` - Document chunking configuration
- `create_vector_database.py:30-50` - Vector database setup

## Development Notes

- Each script can be run independently for incremental processing
- System uses batch processing to handle API rate limits
- All intermediate results are persisted in knowledge_base directory
- Error handling includes comprehensive logging with progress indicators
- Graph database schema auto-generates from document content
- Vector database supports semantic search over processed documents

## Testing

The system uses integration testing rather than unit tests. Each script can be run individually to verify functionality:
- Run scripts with their `__name__ == "__main__"` blocks to test individual components
- Check console output for progress indicators and error messages
- Verify database creation in `knowledge_base/04_database/` directory