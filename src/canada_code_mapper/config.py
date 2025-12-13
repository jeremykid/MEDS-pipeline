"""
Configuration loader for code mappers.

Handles loading mapper configurations from YAML files.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML config file
        
    Returns:
        Configuration dictionary
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Loaded configuration from {config_path}")
    return config


def get_mapper_config(
    config: Dict[str, Any],
    mapper_name: str
) -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific mapper.
    
    Args:
        config: Full configuration dictionary
        mapper_name: Name of the mapper
        
    Returns:
        Mapper configuration or None if not found
    """
    mappers = config.get("mappers", {})
    return mappers.get(mapper_name)


# Default configuration template
DEFAULT_CONFIG = """
# Code Mapper Configuration
# 
# This file defines the mapping files for different medical coding systems

mappers:
  icd10ca:
    file_path: "data/icd10ca_descriptions.txt"
    code_column: "code"
    description_column: "description"
    delimiter: "|"
    encoding: "utf-8"
    code_type: "diagnosis"
    
  cci:
    file_path: "data/cci_descriptions.txt"
    code_column: "code"
    description_column: "description"
    delimiter: ","
    encoding: "utf-8"
    code_type: "procedure"

# Base directory for relative paths
base_dir: "."
"""


def create_default_config(output_path: str):
    """
    Create a default configuration file.
    
    Args:
        output_path: Path where to save the config file
    """
    output_path = Path(output_path)
    
    if output_path.exists():
        logger.warning(f"Config file already exists: {output_path}")
        return
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(DEFAULT_CONFIG)
    
    logger.info(f"Created default config at {output_path}")
