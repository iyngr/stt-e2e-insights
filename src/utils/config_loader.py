"""Configuration loader utility for STT E2E Insights."""

import os
import yaml
import subprocess
from typing import Dict, Any, Optional
from pathlib import Path


def get_gcp_project_id() -> Optional[str]:
    """Get the current GCP project ID implicitly.
    
    Returns:
        Project ID string if available, None otherwise.
    """
    try:
        # Try to get project ID from gcloud CLI
        result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'project'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            project_id = result.stdout.strip()
            if project_id != "(unset)":
                return project_id
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    
    # Try to get from environment variable
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get('GCP_PROJECT')
    if project_id:
        return project_id
    
    # Try to get from metadata service (when running on GCP)
    try:
        import requests
        response = requests.get(
            'http://metadata.google.internal/computeMetadata/v1/project/project-id',
            headers={'Metadata-Flavor': 'Google'},
            timeout=2
        )
        if response.status_code == 200:
            return response.text
    except:
        pass
    
    return None


class ConfigLoader:
    """Handles loading and validation of configuration from YAML files."""
    
    def __init__(self, config_path: str = None):
        """Initialize the config loader.
        
        Args:
            config_path: Path to the configuration file. If None, uses default path.
        """
        if config_path is None:
            # Default to config/config.yaml relative to project root
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            Dictionary containing configuration data.
            
        Raises:
            FileNotFoundError: If config file doesn't exist.
            yaml.YAMLError: If config file is malformed.
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
            
            # Auto-detect project ID if not provided
            if 'gcp' in self._config and 'project_id' not in self._config['gcp']:
                project_id = get_gcp_project_id()
                if project_id:
                    self._config['gcp']['project_id'] = project_id
                else:
                    raise ValueError("Could not detect GCP project ID. Please set GOOGLE_CLOUD_PROJECT environment variable or configure gcloud CLI.")
            
            return self._config
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file: {e}")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the loaded configuration.
        
        Returns:
            Dictionary containing configuration data.
            
        Raises:
            RuntimeError: If configuration hasn't been loaded yet.
        """
        if self._config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return self._config
    
    def get_section(self, section_name: str) -> Dict[str, Any]:
        """Get a specific configuration section.
        
        Args:
            section_name: Name of the configuration section.
            
        Returns:
            Dictionary containing the section data.
            
        Raises:
            KeyError: If section doesn't exist.
        """
        config = self.get_config()
        if section_name not in config:
            raise KeyError(f"Configuration section '{section_name}' not found.")
        return config[section_name]
    
    def validate_required_sections(self, required_sections: list) -> None:
        """Validate that all required configuration sections exist.
        
        Args:
            required_sections: List of required section names.
            
        Raises:
            ValueError: If any required section is missing.
        """
        config = self.get_config()
        missing_sections = [section for section in required_sections if section not in config]
        
        if missing_sections:
            raise ValueError(f"Missing required configuration sections: {missing_sections}")
    
    def substitute_env_vars(self) -> None:
        """Substitute environment variables in configuration values."""
        if self._config is None:
            return
        
        self._config = self._substitute_env_vars_recursive(self._config)
    
    def _substitute_env_vars_recursive(self, obj: Any) -> Any:
        """Recursively substitute environment variables in configuration."""
        if isinstance(obj, dict):
            return {k: self._substitute_env_vars_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._substitute_env_vars_recursive(item) for item in obj]
        elif isinstance(obj, str):
            return os.path.expandvars(obj)
        else:
            return obj


# Global config instance
_config_loader = None


def get_config_loader(config_path: str = None) -> ConfigLoader:
    """Get the global configuration loader instance.
    
    Args:
        config_path: Path to configuration file. Only used on first call.
        
    Returns:
        ConfigLoader instance.
    """
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader(config_path)
        _config_loader.load_config()
        _config_loader.substitute_env_vars()
        
        # Validate required sections
        required_sections = ['gcp', 'gcs', 'dlp', 'ccai', 'processing']
        _config_loader.validate_required_sections(required_sections)
    
    return _config_loader


def get_config() -> Dict[str, Any]:
    """Get the loaded configuration.
    
    Returns:
        Dictionary containing configuration data.
    """
    return get_config_loader().get_config()


def get_config_section(section_name: str) -> Dict[str, Any]:
    """Get a specific configuration section.
    
    Args:
        section_name: Name of the configuration section.
        
    Returns:
        Dictionary containing the section data.
    """
    return get_config_loader().get_section(section_name)