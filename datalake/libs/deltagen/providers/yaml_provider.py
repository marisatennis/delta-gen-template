"""YAML configuration provider for Delta-Gen."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Type

import yaml
from pydantic import ValidationError

from .base import ConfigProvider, ConfigT
from .macros import expand_macros, load_defaults, merge_defaults


class YamlConfigProvider(ConfigProvider[ConfigT]):
    """Generic provider for loading configurations from YAML files.

    Supports:
    - Any Pydantic model (TableConfig, EnvironmentConfig, etc.)
    - Macro expansion with ${...} syntax
    - Smart column template matching (auto-merges common_columns)
    - Defaults loading from defaults.yaml
    - Clear validation errors with context
    - Deep merging of defaults and config

    Type Parameters:
        ConfigT: The type of configuration to load (e.g., TableConfig, EnvironmentConfig)

    Examples:
        >>> # Load table configurations
        >>> from deltagen.model import TableConfig
        >>> table_provider = YamlConfigProvider(TableConfig)
        >>> table = table_provider.load("configs/customer_dim.yaml")
        >>> table.name
        'customer_dim'

        >>> # Load environment configurations
        >>> from deltagen.model import EnvironmentConfig
        >>> env_provider = YamlConfigProvider(EnvironmentConfig)
        >>> env = env_provider.load("configs/production.yaml")

        >>> # With explicit defaults
        >>> provider = YamlConfigProvider(TableConfig, defaults_path="configs/defaults.yaml")
        >>> table = provider.load("configs/product_dim.yaml")
    """

    def __init__(
        self,
        config_class: Type[ConfigT],
        defaults_path: str | Path | None = None,
        auto_discover_defaults: bool = True,
    ):
        """Initialize the YAML provider.

        Args:
            config_class: The Pydantic model class to instantiate (TableConfig, etc.)
            defaults_path: Explicit path to defaults.yaml. If None and auto_discover_defaults
                          is True, will look for defaults.yaml in same directory as config.
            auto_discover_defaults: If True, automatically look for defaults.yaml in the
                                   same directory as the config file being loaded.
        """
        self._config_class = config_class
        self._explicit_defaults_path = Path(defaults_path) if defaults_path else None
        self._auto_discover_defaults = auto_discover_defaults
        self._defaults_cache: dict[Path, dict[str, Any]] = {}

    def load(self, config_path: str | Path) -> ConfigT:
        """Load and validate a configuration from YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            Validated configuration instance of type ConfigT

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If configuration is invalid with detailed context
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        # Load raw YAML
        try:
            with open(path, "r") as f:
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML file '{path}': {e}") from e

        if config_dict is None:
            raise ValueError(f"Configuration file is empty: {path}")

        # Load defaults
        defaults = self._load_defaults_for_config(path)

        # Process config with defaults and macros
        try:
            processed_config = self._process_config(config_dict, defaults, path)
        except ValueError as e:
            # Re-raise with file context
            raise ValueError(f"Error processing '{path}': {e}") from e

        # Validate with Pydantic (skip processing since already done)
        return self.load_dict(processed_config, source_file=str(path), skip_processing=True)

    def load_dict(
        self,
        config_dict: dict[str, Any],
        source_file: str | None = None,
        skip_processing: bool = False,
    ) -> ConfigT:
        """Load configuration from a dictionary.

        Args:
            config_dict: Configuration as a dictionary
            source_file: Optional source file path for error messages
            skip_processing: If True, skip macro expansion and defaults merging
                           (used when called from load() which already processed)

        Returns:
            Validated configuration instance of type ConfigT

        Raises:
            ValueError: If configuration is invalid or macros can't be resolved
        """
        # Process macros if not already processed
        if not skip_processing:
            defaults = self._load_defaults_for_config(None)  # Load from explicit path only
            try:
                config_dict = self._process_config(config_dict, defaults, Path("."))
            except ValueError as e:
                # Re-raise with context
                context = f" in '{source_file}'" if source_file else ""
                raise ValueError(f"Error processing config{context}: {e}") from e

        try:
            return self._config_class(**config_dict)
        except ValidationError as e:
            # Format Pydantic errors with better context
            error_msg = self._format_validation_error(e, source_file)
            raise ValueError(error_msg) from e

    def _load_defaults_for_config(self, config_path: Path | None) -> dict[str, Any]:
        """Load defaults for a specific config file.

        Args:
            config_path: Path to the config file (None for explicit path only)

        Returns:
            Defaults dictionary
        """
        # Use explicit defaults if provided
        if self._explicit_defaults_path:
            if self._explicit_defaults_path not in self._defaults_cache:
                self._defaults_cache[self._explicit_defaults_path] = load_defaults(
                    self._explicit_defaults_path
                )
            return self._defaults_cache[self._explicit_defaults_path]

        # Auto-discover defaults.yaml in same directory
        if self._auto_discover_defaults and config_path:
            defaults_path = config_path.parent / "defaults.yaml"
            if defaults_path.exists():
                if defaults_path not in self._defaults_cache:
                    self._defaults_cache[defaults_path] = load_defaults(defaults_path)
                return self._defaults_cache[defaults_path]

        return {}

    def _process_config(
        self, config_dict: dict[str, Any], defaults: dict[str, Any], config_path: Path
    ) -> dict[str, Any]:
        """Process config with defaults and macro expansion.

        Args:
            config_dict: Raw configuration dictionary
            defaults: Defaults dictionary
            config_path: Path to config file (for error context)

        Returns:
            Processed configuration ready for validation
        """
        # Step 1: Merge defaults (config takes precedence)
        merged = merge_defaults(config_dict, defaults)

        # Step 2: Expand macros
        expanded = expand_macros(merged, defaults)

        return expanded

    def _format_validation_error(
        self, error: ValidationError, source_file: str | None
    ) -> str:
        """Format Pydantic validation errors with better context.

        Args:
            error: Pydantic ValidationError
            source_file: Optional source file path

        Returns:
            Formatted error message
        """
        file_context = f" in '{source_file}'" if source_file else ""
        error_lines = [f"Configuration validation failed{file_context}:"]

        for err in error.errors():
            loc = " -> ".join(str(x) for x in err["loc"])
            msg = err["msg"]
            error_lines.append(f"  • {loc}: {msg}")

        return "\n".join(error_lines)
