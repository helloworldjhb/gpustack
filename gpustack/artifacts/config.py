"""S3 profile settings, independent of any artifact's business domain."""

from typing import Literal, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class ArtifactStorageProfile(BaseModel):
    model_config = ConfigDict(extra='forbid', hide_input_in_errors=True)

    endpoint: Optional[str] = None
    bucket: str
    prefix: str = 'gpustack'
    region: str = 'us-east-1'
    access_key_id: Optional[SecretStr] = Field(default=None, repr=False)
    secret_access_key: Optional[SecretStr] = Field(default=None, repr=False)
    session_token: Optional[SecretStr] = Field(default=None, repr=False)
    ca_file: Optional[str] = None
    tls_verify: bool = True
    addressing_style: Literal['auto', 'path', 'virtual'] = 'auto'
    conditional_writes: bool = True
    connect_timeout: float = Field(default=5, gt=0, le=120)
    read_timeout: float = Field(default=30, gt=0, le=300)
    max_connections: int = Field(default=8, ge=1, le=128)
    max_concurrency: int = Field(default=4, ge=1, le=64)
    max_attempts: int = Field(default=3, ge=1, le=10)
    max_object_bytes: int = Field(default=16 * 1024 * 1024, ge=1, le=256 * 1024 * 1024)

    @model_validator(mode='after')
    def validate_profile(self):
        if not self.bucket or '/' in self.bucket or '\\' in self.bucket:
            raise ValueError('artifact storage requires a bucket name')
        if bool(self.access_key_id) != bool(self.secret_access_key):
            raise ValueError('artifact storage credentials require both key fields')
        if self.endpoint:
            parsed = urlparse(self.endpoint)
            if (
                parsed.scheme not in ('http', 'https')
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(
                    'artifact storage endpoint must be an HTTP(S) URL without credentials'
                )
        self.prefix = self.prefix.strip('/')
        if '\\' in self.prefix or any(ord(char) < 32 for char in self.prefix):
            raise ValueError('artifact storage prefix must be a relative namespace')
        if any(p in ('.', '..', '') for p in self.prefix.split('/')) and self.prefix:
            raise ValueError('artifact storage prefix must be a relative namespace')
        return self
