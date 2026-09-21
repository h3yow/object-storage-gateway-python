"""On-disk layout and safe key/path encoding.

data_dir/
  buckets/<encoded-bucket>.json          bucket record (owner, policy, stats)
  objects/<encoded-bucket>/<encoded-key>/<version>.part
  uploads/<encoded-bucket>/<encoded-key>/<upload-id>/
      meta.json
      parts/<part-number>.part

Both bucket names and object keys are percent-encoded before touching the
filesystem, so crafted keys ("../", absolute paths, unicode) can never escape
their own directory.
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote, unquote

from app.config import Settings
from app.errors import ErrorCode, error

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{0,61}[a-z0-9]$|^[a-z0-9]$")
_SAFE_BUCKET_CHARS = ".-"   # extra chars left unencoded by quote()
_KEY_SAFE = ""              # encode everything except alnum and '_.-~'


class Layout:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.root = settings.data_dir
        self.buckets_dir = self.root / "buckets"
        self.objects_dir = self.root / "objects"
        self.uploads_dir = self.root / "uploads"

    def init_dirs(self) -> None:
        for d in (self.buckets_dir, self.objects_dir, self.uploads_dir):
            d.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def validate_bucket_name(name: str) -> None:
        if not isinstance(name, str) or not _BUCKET_RE.match(name):
            raise error(
                ErrorCode.INVALID_ARGUMENT,
                f"invalid bucket name {name!r}: 3-63 chars, lowercase "
                "letters/digits/'.'/'-', must start and end alphanumerically",
            )
        if ".." in name:
            raise error(ErrorCode.INVALID_ARGUMENT, f"invalid bucket name {name!r}")

    @staticmethod
    def validate_key(key: str) -> None:
        if not isinstance(key, str) or key == "":
            raise error(ErrorCode.INVALID_ARGUMENT, "object key must be a non-empty string")
        if len(key) > 1024:
            raise error(ErrorCode.INVALID_ARGUMENT, "object key too long (max 1024)")

    @staticmethod
    def encode_name(name: str, safe: str = _KEY_SAFE) -> str:
        """Percent-encode so the result is a single safe path component."""
        return quote(name, safe=safe)

    @staticmethod
    def decode_name(encoded: str) -> str:
        return unquote(encoded)

    # ---- buckets ----
    def bucket_file(self, bucket: str) -> Path:
        return self.buckets_dir / f"{self.encode_name(bucket, safe=_SAFE_BUCKET_CHARS)}.json"

    def all_bucket_files(self) -> list[Path]:
        if not self.buckets_dir.exists():
            return []
        return sorted(p for p in self.buckets_dir.glob("*.json") if p.is_file())

    # ---- objects ----
    def bucket_object_dir(self, bucket: str) -> Path:
        return self.objects_dir / self.encode_name(bucket, safe=_SAFE_BUCKET_CHARS)

    def object_dir(self, bucket: str, key: str) -> Path:
        return self.bucket_object_dir(bucket) / self.encode_name(key)

    def version_blob(self, bucket: str, key: str, version_id: str) -> Path:
        return self.object_dir(bucket, key) / f"{version_id}.part"

    def iter_object_dirs(self, bucket: str):
        base = self.bucket_object_dir(bucket)
        if not base.exists():
            return
        for child in sorted(base.iterdir()):
            if child.is_dir():
                yield child, self.decode_name(child.name)

    # ---- uploads ----
    def bucket_upload_dir(self, bucket: str) -> Path:
        return self.uploads_dir / self.encode_name(bucket, safe=_SAFE_BUCKET_CHARS)

    def upload_dir(self, bucket: str, key: str, upload_id: str) -> Path:
        return (self.bucket_upload_dir(bucket) / self.encode_name(key) / upload_id)

    def upload_meta_file(self, bucket: str, key: str, upload_id: str) -> Path:
        return self.upload_dir(bucket, key, upload_id) / "meta.json"

    def upload_parts_dir(self, bucket: str, key: str, upload_id: str) -> Path:
        return self.upload_dir(bucket, key, upload_id) / "parts"

    def part_file(self, bucket: str, key: str, upload_id: str, part_number: int) -> Path:
        return self.upload_parts_dir(bucket, key, upload_id) / f"{part_number:05d}.part"
