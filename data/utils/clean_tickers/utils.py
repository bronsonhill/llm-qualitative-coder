"""Utility functions for the ticker cleaning process."""
from typing import Any, Dict, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path
import logging

from .config import OUTPUT_DIR, LOG_CONFIG
from .exceptions import ConfigurationError

def setup_logging(name: str) -> logging.Logger:
    """
    Set up logging configuration for a module.
    
    Args:
        name: Name of the logger
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_CONFIG['level'])
    
    formatter = logging.Formatter(
        LOG_CONFIG['format'],
        datefmt=LOG_CONFIG['date_format']
    )
    
    # Add console handler if none exists
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

def generate_output_filename(prefix: str, ext: str = 'csv') -> Path:
    """
    Generate a timestamped output filename.
    
    Args:
        prefix: Prefix for the filename
        ext: File extension (default: csv)
        
    Returns:
        Path object for the output file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return OUTPUT_DIR / f"{prefix}_{timestamp}.{ext}"

def save_dataframe(df: pd.DataFrame, prefix: str) -> Path:
    """
    Save a DataFrame to a CSV file with timestamp.
    
    Args:
        df: DataFrame to save
        prefix: Prefix for the output filename
        
    Returns:
        Path where the file was saved
    """
    output_path = generate_output_filename(prefix)
    df.to_csv(output_path, index=False)
    return output_path

def validate_env_vars(required_vars: Dict[str, str]) -> None:
    """
    Validate that required environment variables are set.
    
    Args:
        required_vars: Dictionary of required variables and their descriptions
        
    Raises:
        ConfigurationError: If any required variable is missing
    """
    import os
    missing = [var for var, desc in required_vars.items() if not os.getenv(var)]
    if missing:
        raise ConfigurationError(
            "environment_variables",
            f"Missing required environment variables: {', '.join(missing)}"
        )

def safe_get_attr(obj: Any, attr: str, default: Any = None) -> Any:
    """
    Safely get an attribute from an object.
    
    Args:
        obj: Object to get attribute from
        attr: Name of the attribute
        default: Default value if attribute doesn't exist
        
    Returns:
        Attribute value or default
    """
    try:
        return getattr(obj, attr, default)
    except (AttributeError, TypeError):
        return default

def parse_date_safe(date_str: str) -> Optional[datetime]:
    """
    Safely parse a date string.
    
    Args:
        date_str: Date string to parse
        
    Returns:
        Parsed datetime or None if parsing fails
    """
    try:
        return pd.to_datetime(date_str)
    except (ValueError, TypeError):
        return None