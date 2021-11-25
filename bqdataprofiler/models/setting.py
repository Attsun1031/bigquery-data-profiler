from pathlib import Path
from typing import Optional, Pattern

import yaml
from pydantic import BaseModel


def load_setting_from_yaml(yaml_path: Path):
    """
    Load setting from yaml
    """
    with yaml_path.open() as f:
        contents = yaml.safe_load(f)
    return ProfileSettingRoot.parse_obj(contents)


class ProjectSetting(BaseModel):
    """
    Setting per project
    """
    project_name: str
    include_schema_patterns: list[Pattern] = []
    include_table_patterns: list[Pattern] = []
    include_null_count: bool = True
    include_max_value: bool = True
    include_min_value: bool = True
    include_empty_or_zero_count: bool = True


class ProfileSettingRoot(BaseModel):
    """
    Root of setting
    """
    settings: list[ProjectSetting]
    max_workers: int = 4
    maximum_byte_billed: Optional[int] = None
