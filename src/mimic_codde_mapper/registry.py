"""
Registry for managing multiple MIMIC code mappers.

Allows centralized management of ICD-9/ICD-10 diagnosis and procedure mappers.
Supports automatic routing for composite code formats based on ICD version.
"""

from typing import Dict, Optional, List
from pathlib import Path
import logging
import pandas as pd

from .mapper import MIMICCodeMapper
from .composite import parse_composite_code, get_mapper_key

logger = logging.getLogger(__name__)


class MIMICMapperRegistry:
    """
    Centralized registry for managing multiple MIMICCodeMappers.
    
    Supports automatic routing based on composite code format (ICD version).
    
    Usage:
        # Initialize registry
        registry = MIMICMapperRegistry()
        
        # Register mappers
        registry.register_from_mimic_file(
            "diagnosis_9",
            "d_icd_diagnoses.csv.gz",
            code_type="diagnosis",
            icd_version=9
        )
        registry.register_from_mimic_file(
            "diagnosis_10",
            "d_icd_diagnoses.csv.gz",
            code_type="diagnosis",
            icd_version=10
        )
        
        # Auto-routing: composite codes are routed to correct mapper
        desc = registry.get_description("DIAGNOSIS//ICD//10//R531")
    """
    
    def __init__(self):
        """Initialize empty registry."""
        self._mappers: Dict[str, MIMICCodeMapper] = {}
        logger.info("Initialized MIMICMapperRegistry")
    
    def register(
        self,
        name: str,
        mapper: MIMICCodeMapper,
        overwrite: bool = False
    ):
        """
        Register a MIMICCodeMapper instance.
        
        Args:
            name: Unique name for this mapper
            mapper: MIMICCodeMapper instance
            overwrite: Whether to overwrite existing mapper with same name
        """
        if name in self._mappers and not overwrite:
            raise ValueError(
                f"Mapper '{name}' already registered. "
                f"Use overwrite=True to replace."
            )
        
        self._mappers[name] = mapper
        logger.info(f"Registered mapper: {name}")
    
    def register_from_mimic_file(
        self,
        name: str,
        file_path: str,
        code_type: str = "diagnosis",
        icd_version: Optional[int] = None,
        overwrite: bool = False
    ):
        """
        Create and register a mapper from a MIMIC file.
        
        Args:
            name: Unique name for this mapper
            file_path: Path to MIMIC mapping file
            code_type: Type of codes ("diagnosis" or "procedure")
            icd_version: ICD version to filter (9 or 10)
            overwrite: Whether to overwrite existing mapper
        """
        mapper = MIMICCodeMapper.from_mimic_file(
            file_path=file_path,
            code_type=code_type,
            icd_version=icd_version,
            name=name
        )
        
        self.register(name, mapper, overwrite=overwrite)
    
    def get_mapper(self, name: str) -> MIMICCodeMapper:
        """
        Get a registered mapper by name.
        
        Args:
            name: Name of the mapper
            
        Returns:
            MIMICCodeMapper instance
        """
        if name not in self._mappers:
            raise KeyError(
                f"Mapper '{name}' not found. "
                f"Available mappers: {self.list_mappers()}"
            )
        
        return self._mappers[name]
    
    def get_description(
        self,
        mapper_name: str,
        code: str,
        default: str = "Unknown",
        auto_route: bool = True
    ) -> str:
        """
        Get description for a code using a specific mapper or auto-routing.
        
        Supports both plain and composite code formats.
        
        For composite codes (e.g., "DIAGNOSIS//ICD//10//R531"):
        - If auto_route=True, automatically routes to correct mapper based on version
        - If auto_route=False, uses the specified mapper_name
        
        Args:
            mapper_name: Name of the mapper to use (may be overridden by auto_route)
            code: Medical code to lookup (plain or composite format)
            default: Default value if code not found
            auto_route: If True, automatically route composite codes
            
        Returns:
            Description string
        """
        code_str = str(code).strip()
        
        # Try to auto-route based on composite code format
        if auto_route:
            mapper_key = get_mapper_key(code_str)
            if mapper_key and mapper_key in self._mappers:
                logger.debug(f"Auto-routing composite code to {mapper_key} mapper")
                return self._mappers[mapper_key].get_description(code_str, default=default)
            elif mapper_key:
                logger.warning(
                    f"No mapper found for key '{mapper_key}', "
                    f"falling back to specified mapper '{mapper_name}'"
                )
        
        # Fall back to specified mapper
        mapper = self.get_mapper(mapper_name)
        return mapper.get_description(code_str, default=default)
    
    def get_descriptions(
        self,
        mapper_name: str,
        codes: List[str],
        default: str = "Unknown"
    ) -> List[str]:
        """
        Batch lookup descriptions using a specific mapper.
        
        Args:
            mapper_name: Name of the mapper to use
            codes: List of medical codes
            default: Default value for codes not found
            
        Returns:
            List of descriptions
        """
        return [
            self.get_description(mapper_name, code, default=default)
            for code in codes
        ]
    
    def list_mappers(self) -> List[str]:
        """Get list of all registered mapper names."""
        return list(self._mappers.keys())
    
    def has_mapper(self, name: str) -> bool:
        """Check if a mapper is registered."""
        return name in self._mappers
    
    def remove_mapper(self, name: str):
        """Remove a registered mapper."""
        if name not in self._mappers:
            logger.warning(f"Mapper '{name}' not found, nothing to remove")
            return
        
        del self._mappers[name]
        logger.info(f"Removed mapper: {name}")
    
    def get_all_stats(self) -> Dict:
        """Get statistics for all registered mappers."""
        return {
            name: mapper.get_stats()
            for name, mapper in self._mappers.items()
        }
    
    def __len__(self) -> int:
        """Return number of registered mappers."""
        return len(self._mappers)
    
    def __repr__(self) -> str:
        mapper_info = ", ".join(
            f"{name}({len(mapper)} codes)"
            for name, mapper in self._mappers.items()
        )
        return f"MIMICMapperRegistry({mapper_info})"


# Global registry instance (optional convenience)
_global_registry: Optional[MIMICMapperRegistry] = None


def get_global_registry() -> MIMICMapperRegistry:
    """Get or create the global registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = MIMICMapperRegistry()
    return _global_registry


def init_mimic_mappers(
    diagnosis_path: Optional[str] = None,
    procedure_path: Optional[str] = None,
    registry: Optional[MIMICMapperRegistry] = None
) -> MIMICMapperRegistry:
    """
    Convenience function to initialize MIMIC medical code mappers.
    
    Registers separate mappers for ICD-9 and ICD-10 versions of both
    diagnosis and procedure codes.
    
    Args:
        diagnosis_path: Path to d_icd_diagnoses.csv.gz
        procedure_path: Path to d_icd_procedures.csv.gz
        registry: MIMICMapperRegistry to use (creates new if None)
        
    Returns:
        MIMICMapperRegistry with MIMIC mappers registered
        
    Example:
        >>> registry = init_mimic_mappers(
        ...     diagnosis_path="/path/to/d_icd_diagnoses.csv.gz",
        ...     procedure_path="/path/to/d_icd_procedures.csv.gz"
        ... )
        >>> registry.get_description("diagnosis_10", "DIAGNOSIS//ICD//10//R531")
        'Fatigue, unspecified'
    """
    if registry is None:
        registry = MIMICMapperRegistry()
    
    if diagnosis_path:
        # Register ICD-9 diagnosis codes
        registry.register_from_mimic_file(
            name="diagnosis_9",
            file_path=diagnosis_path,
            code_type="diagnosis",
            icd_version=9
        )
        logger.info("Registered ICD-9 diagnosis mapper")
        
        # Register ICD-10 diagnosis codes
        registry.register_from_mimic_file(
            name="diagnosis_10",
            file_path=diagnosis_path,
            code_type="diagnosis",
            icd_version=10
        )
        logger.info("Registered ICD-10 diagnosis mapper")
    
    if procedure_path:
        # Register ICD-9 procedure codes
        registry.register_from_mimic_file(
            name="procedure_9",
            file_path=procedure_path,
            code_type="procedure",
            icd_version=9
        )
        logger.info("Registered ICD-9 procedure mapper")
        
        # Register ICD-10 procedure codes
        registry.register_from_mimic_file(
            name="procedure_10",
            file_path=procedure_path,
            code_type="procedure",
            icd_version=10
        )
        logger.info("Registered ICD-10 procedure mapper")
    
    return registry
