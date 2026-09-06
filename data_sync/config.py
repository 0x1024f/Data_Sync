"""Strict configuration. Secrets are environment references, never literal values."""
import os
import re
import ipaddress
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def safe_key(value: str) -> str:
    if not value or value.startswith("/") or "\\" in value or any(
        p in ("", ".", "..") for p in value.split("/")
    ) or any(ord(c) < 32 for c in value):
        raise ValueError("invalid object key or relative path")
    return value


def secret(ref: str) -> str:
    name = ref.removeprefix("env:")
    value = os.environ.get(name)
    if not value:
        raise ValueError("required credential environment variable is missing")
    return value


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AgentConfig(Strict):
    source_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    work_dir: Path
    scan_seconds: float = Field(default=10, gt=0)
    min_free_bytes: int = Field(default=1073741824, ge=0)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class FileSource(Strict):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    system: str
    root: Path
    include: list[str] = ["**/*"]
    exclude: list[str] = ["**/*.tmp", "**/*.part", "**/*.done"]
    recursive: bool = True
    initial_scan: Literal["new_only", "existing_and_new"] = "new_only"
    stable_seconds: float = Field(default=60, ge=0)
    quiet_seconds: float = Field(default=120, ge=0)
    filename_regex: str
    obs_time_format: str = "%Y%m%d_%H%M"
    timezone_offset: str = "+08:00"
    prefix: str = "rs-raw"

    @field_validator("prefix")
    @classmethod
    def key(cls, v):
        return safe_key(v)

    @field_validator("filename_regex")
    @classmethod
    def regex(cls, v):
        if "batch_no" not in re.compile(v).groupindex:
            raise ValueError("filename_regex requires named group batch_no")
        return v

    @field_validator("timezone_offset")
    @classmethod
    def offset(cls, v):
        datetime.fromisoformat("2000-01-01T00:00:00" + v)
        if not re.fullmatch(r"[+-]\d{2}:\d{2}", v):
            raise ValueError("expected timezone offset such as +08:00")
        return v


class Target(Strict):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    enabled: bool = True
    enabled_at: Optional[datetime] = None
    host: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9.-]+$")
    port: int = Field(default=443, ge=1, le=65535)
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    prefix: str = "transfer"
    region: str = "us-east-1"
    access_key: str = Field(pattern=r"^env:[A-Za-z_][A-Za-z0-9_]*$")
    secret_key: str = Field(pattern=r"^env:[A-Za-z_][A-Za-z0-9_]*$")
    ca_bundle: Optional[Path] = None
    concurrency: int = Field(default=2, ge=1, le=32)
    timeout_seconds: int = Field(default=60, ge=1, le=600)
    retry_initial_seconds: float = Field(default=2, gt=0)
    retry_max_seconds: float = Field(default=300, gt=0)
    part_size: int = Field(default=16777216, ge=5242880, le=536870912)
    multipart_threshold: int = Field(default=16777216, ge=5242880, le=536870912)

    @field_validator("host")
    @classmethod
    def sni_domain(cls, v):
        try:
            ipaddress.ip_address(v)
        except ValueError:
            if "." not in v:
                raise ValueError("use a fully qualified SNI domain")
            return v
        raise ValueError("SNI routing requires a domain, not an IP address")

    @field_validator("prefix")
    @classmethod
    def key(cls, v):
        return safe_key(v)

    @field_validator("enabled_at")
    @classmethod
    def aware(cls, v):
        if v and v.utcoffset() is None:
            raise ValueError("enabled_at must include timezone")
        return v

    @model_validator(mode="after")
    def bounds(self):
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError("retry maximum must be >= initial")
        return self


class MySQLSource(Strict):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    host: str
    port: int = Field(default=3306, ge=1, le=65535)
    user: str = Field(pattern=r"^env:[A-Za-z_][A-Za-z0-9_]*$")
    password: str = Field(pattern=r"^env:[A-Za-z_][A-Za-z0-9_]*$")
    database: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    table: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    primary_key: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    fields: list[str] = Field(min_length=1)
    batch_size: int = Field(default=1000, ge=1, le=100000)
    poll_seconds: float = Field(default=10, gt=0)
    initial_scan: Literal["new_only", "existing_and_new"] = "new_only"
    prefix: str = "db-increment"
    ca_bundle: Optional[Path] = None
    # AUTO_INCREMENT allocation order is not commit order. Opt-in is required.
    commit_order_guaranteed: Literal[True]

    @field_validator("prefix")
    @classmethod
    def key(cls, v):
        return safe_key(v)

    @model_validator(mode="after")
    def identifiers(self):
        if self.primary_key not in self.fields or len(set(self.fields)) != len(self.fields):
            raise ValueError("fields must be unique and contain primary_key")
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", f) for f in self.fields):
            raise ValueError("invalid SQL identifier")
        return self


class Config(Strict):
    agent: AgentConfig
    files: list[FileSource] = []
    mysql: list[MySQLSource] = []
    targets: list[Target] = Field(min_length=1)

    @model_validator(mode="after")
    def consistency(self):
        ids = [s.id for s in self.files + self.mysql]
        if len(ids) != len(set(ids)):
            raise ValueError("source ids must be unique")
        if len({t.id for t in self.targets}) != len(self.targets):
            raise ValueError("target ids must be unique")
        if len({t.prefix for t in self.targets}) != 1:
            raise ValueError("all targets must use identical prefixes for shared manifests")
        if len({t.port for t in self.targets}) != 1:
            raise ValueError("all targets must use the same relay port")
        if not self.files and not self.mysql:
            raise ValueError("configure at least one source")
        return self


def load_config(path: Path) -> Config:
    config = Config.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    base = path.resolve().parent
    config.agent.work_dir = (base / config.agent.work_dir).resolve()
    for item in config.files:
        item.root = (base / item.root).resolve()
        if config.agent.work_dir == item.root or item.root in config.agent.work_dir.parents:
            raise ValueError("work_dir must be outside watched directories")
    for item in config.targets + config.mysql:
        if item.ca_bundle:
            item.ca_bundle = (base / item.ca_bundle).resolve()
    return config
