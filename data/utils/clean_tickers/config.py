"""Configuration settings for the ticker cleaning utility."""
from pathlib import Path
from typing import Dict, Any
import os

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output'

# Ensure output directory exists
OUTPUT_DIR.mkdir(exist_ok=True)

# Yahoo Finance settings
YF_SEARCH_CONFIG: Dict[str, Any] = {
    'max_results': 5,
    'timeout': 30,
    'enable_fuzzy_query': True
}

# OpenAI settings
GPT_CONFIG: Dict[str, Any] = {
    'model': 'gpt-4',
    'temperature': 0.0,  # We want deterministic responses
    'max_retries': 3
}

# Database settings
DB_CONFIG: Dict[str, Any] = {
    'batch_size': 100,
    'timeout': 30
}

# Logging settings
LOG_CONFIG: Dict[str, Any] = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

# File paths
CATEGORY_BOOK_PATH = DATA_DIR / 'utils' / 'category_book.csv'

# Environment variable names
ENV_VARS = {
    'OPENAI_API_KEY': 'OPENAI_API_KEY',
    'DB_CONNECTION': 'DB_CONNECTION'
}