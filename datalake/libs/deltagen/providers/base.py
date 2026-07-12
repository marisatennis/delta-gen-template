"""Base protocol for configuration providers."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

from deltagen.model.base import StrictBaseModel

# Generic type variable for any Pydantic model
ConfigT = TypeVar("ConfigT", bound=StrictBaseModel)


class ConfigProvider(ABC, Generic[ConfigT]):
    """Generic protocol for loading and validating configurations.

    Providers are responsible for:
    - Loading configuration from various sources (YAML, XML, etc.)
    - Expanding macros and merging defaults
    - Validating configuration and surfacing clear errors
    - Returning validated config instances (TableConfig, EnvironmentConfig, etc.)

    Type Parameters:
        ConfigT: The type of configuration this provider loads (must be a StrictBaseModel)

    Examples:
        >>> # TableConfig provider
        >>> table_provider = YamlConfigProvider(TableConfig, defaults_path="defaults.yaml")
        >>> table = table_provider.load("customer_dim.yaml")  # Returns TableConfig
        >>>
        >>> # EnvironmentConfig provider
        >>> env_provider = YamlConfigProvider(EnvironmentConfig)
        >>> env = env_provider.load("production.yaml")  # Returns EnvironmentConfig
    """

    @abstractmethod
    def load(self, config_path: str | Path) -> ConfigT:
        """Load and validate a configuration.

        Args:
            config_path: Path to the configuration file

        Returns:
            Validated configuration instance of type ConfigT

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If configuration is invalid (with line numbers if available)
        """
        ...

    @abstractmethod
    def load_dict(self, config_dict: dict[str, Any]) -> ConfigT:
        """Load configuration from a dictionary.

        Useful for testing and programmatic config generation.

        Args:
            config_dict: Configuration as a dictionary

        Returns:
            Validated configuration instance of type ConfigT

        Raises:
            ValueError: If configuration is invalid
        """
        ...
