"""
Registry for managing multiple code mappers.

Allows centralized management of ICD-10-CA, CCI, and other medical code mappers.
Supports automatic routing for composite code formats.
"""

from typing import Dict, Optional, List
from pathlib import Path
import logging

from .mapper import CodeMapper
from .composite import parse_composite_code

logger = logging.getLogger(__name__)


class MapperRegistry:
    """
    Centralized registry for managing multiple CodeMappers.
    
    Usage:
        # Initialize registry
        registry = MapperRegistry()
        
        # Register mappers
        registry.register_from_file(
            "icd10ca",
            "data/icd10ca_descriptions.txt",
            code_column="code",
            description_column="description",
            delimiter="|"
        )
        
        registry.register_from_file(
            "cci",
            "data/cci_descriptions.txt",
            code_column="code",
            description_column="description"
        )
        
        # Use registered mappers
        desc = registry.get_description("icd10ca", "A00.0")
        desc = registry.get_description("cci", "1.AA.50")
        
        # Or get the mapper directly
        icd_mapper = registry.get_mapper("icd10ca")
    """
    
    def __init__(self):
        """Initialize empty registry."""
        self._mappers: Dict[str, CodeMapper] = {}
        logger.info("Initialized MapperRegistry")
    
    def register(
        self,
        name: str,
        mapper: CodeMapper,
        overwrite: bool = False
    ):
        """
        Register a CodeMapper instance.
        
        Args:
            name: Unique name for this mapper
            mapper: CodeMapper instance
            overwrite: Whether to overwrite existing mapper with same name
        """
        if name in self._mappers and not overwrite:
            raise ValueError(
                f"Mapper '{name}' already registered. "
                f"Use overwrite=True to replace."
            )
        
        self._mappers[name] = mapper
        logger.info(f"Registered mapper: {name}")
    
    def register_from_file(
        self,
        name: str,
        file_path: str,
        code_column: str = "code",
        description_column: str = "description",
        delimiter: str = ",",
        encoding: str = "utf-8",
        code_type: str = "generic",
        overwrite: bool = False,
        **read_csv_kwargs
    ):
        """
        Create and register a mapper from a file.
        
        Args:
            name: Unique name for this mapper
            file_path: Path to mapping file
            code_column: Code column name
            description_column: Description column name
            delimiter: File delimiter
            encoding: File encoding
            code_type: Type of codes (e.g., "diagnosis", "procedure")
            overwrite: Whether to overwrite existing mapper
            **read_csv_kwargs: Additional arguments for pd.read_csv
        """
        mapper = CodeMapper.from_file(
            file_path=file_path,
            code_column=code_column,
            description_column=description_column,
            delimiter=delimiter,
            encoding=encoding,
            name=name,
            code_type=code_type,
            **read_csv_kwargs
        )
        
        self.register(name, mapper, overwrite=overwrite)
    
    def get_mapper(self, name: str) -> CodeMapper:
        """
        Get a registered mapper by name.
        
        Args:
            name: Name of the mapper
            
        Returns:
            CodeMapper instance
        """
        if name not in self._mappers:
            raise KeyError(
                f"Mapper '{name}' not found. "
                f"Available mappers: {self.list_mappers()}"
            )
        
        return self._mappers[name]
    
    def get_mapper_by_system(self, system: str) -> Optional[CodeMapper]:
        """
        Get a mapper by system name (e.g., 'icd10ca', 'cci').
        
        This is useful for routing composite codes to the correct mapper
        based on their system component.
        
        Args:
            system: System name (normalized, lowercase)
            
        Returns:
            CodeMapper instance if found, None otherwise
        """
        # Try exact match first
        if system in self._mappers:
            return self._mappers[system]
        
        # Try common variations
        system_lower = system.lower()
        for mapper_name in self._mappers:
            if mapper_name.lower() == system_lower:
                return self._mappers[mapper_name]
        
        logger.debug(f"No mapper found for system: {system}")
        return None
    
    def get_description(
        self,
        mapper_name: str,
        code: str,
        default: str = "Unknown",
        auto_route: bool = True
    ) -> str:
        """
        Get description for a code using a specific mapper.
        
        Supports both plain and composite code formats.
        
        For composite codes (e.g., "DIAGNOSIS//ICD10CA//M1000"):
        - If auto_route=True, automatically routes to correct mapper based on system
        - If auto_route=False, uses the specified mapper_name
        
        Args:
            mapper_name: Name of the mapper to use (ignored if auto_route and composite)
            code: Medical code to lookup (plain or composite format)
            default: Default value if code not found
            auto_route: If True, automatically route composite codes to correct mapper
            
        Returns:
            Description string
        """
        # Try to parse composite format for auto-routing
        if auto_route:
            parsed = parse_composite_code(code)
            if parsed:
                # Try to find mapper by system name
                system_mapper = self.get_mapper_by_system(parsed['system'])
                if system_mapper:
                    logger.debug(f"Auto-routing composite code to {parsed['system']} mapper")
                    return system_mapper.get_description(code, default=default)
                else:
                    logger.warning(
                        f"No mapper found for system '{parsed['system']}', "
                        f"falling back to specified mapper '{mapper_name}'"
                    )
        
        # Fall back to specified mapper
        mapper = self.get_mapper(mapper_name)
        return mapper.get_description(code, default=default)
    
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
        mapper = self.get_mapper(mapper_name)
        return mapper.get_descriptions(codes, default=default)
    
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
        return f"MapperRegistry({mapper_info})"


# Global registry instance (optional convenience)
_global_registry: Optional[MapperRegistry] = None


def get_global_registry() -> MapperRegistry:
    """Get or create the global registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = MapperRegistry()
    return _global_registry


def init_canadian_mappers(
    icd10ca_path: Optional[str] = None,
    cci_path: Optional[str] = None,
    registry: Optional[MapperRegistry] = None
) -> MapperRegistry:
    """
    Convenience function to initialize Canadian medical code mappers.
    
    Args:
        icd10ca_path: Path to ICD-10-CA mapping file
        cci_path: Path to CCI mapping file
        registry: MapperRegistry to use (creates new if None)
        
    Returns:
        MapperRegistry with Canadian mappers registered
    """
    if registry is None:
        registry = MapperRegistry()
    
    if icd10ca_path:
        registry.register_from_file(
            name="icd10ca",
            file_path=icd10ca_path,
            code_type="diagnosis",
            code_column="code",
            description_column="description"
        )
        logger.info("Registered ICD-10-CA mapper")
    
    if cci_path:
        registry.register_from_file(
            name="cci",
            file_path=cci_path,
            code_type="procedure",
            code_column="code",
            description_column="description"
        )
        logger.info("Registered CCI mapper")
    
    return registry
