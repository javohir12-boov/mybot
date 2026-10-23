"""Validation and image normalization for the app's own editable profiles."""

import re
from pathlib import Path

import fitz


class ProfileError(ValueError):
    pass


def validate_profile(data):
    name, username = data.get("display_name"), data.get("username", "")
    if not isinstance(name, str) or not isinstance(username, str):
        raise ProfileError("invalid_profile")
    name = " ".join(name.split())
    username = username.strip().removeprefix("@").lower()
    if not 1 <= len(name) <= 80 or any(ord(c) < 32 for c in name):
        raise ProfileError("invalid_profile")
    if username and not re.fullmatch(r"[a-z][a-z0-9_]{2,31}", username):
        raise ProfileError("invalid_username")
    return name, username


def normalize_avatar(raw):
    """Decode the image and store only a small PNG, without original metadata."""
    # The browser converts uploads (including WebP) to PNG before submitting.
    allowed = raw.startswith(b"\x89PNG\r\n\x1a\n") or raw.startswith(b"\xff\xd8\xff")
    if not allowed:
        raise ProfileError("invalid_avatar")
    try:
        with fitz.open(stream=raw) as image:
            page = image[0]
            info = page.get_image_info()
            if len(info) != 1 or not 0 < info[0]["width"] * info[0]["height"] <= 16_000_000:
                raise ProfileError("invalid_avatar")
            scale = min(1, 384 / max(page.rect.width, page.rect.height))
            return page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=True).tobytes("png")
    except Exception as exc:
        raise ProfileError("invalid_avatar") from exc


def remove_avatar(path, root):
    if not path:
        return
    resolved = Path(path).resolve()
    if resolved.is_relative_to(root.resolve()):
        resolved.unlink(missing_ok=True)
