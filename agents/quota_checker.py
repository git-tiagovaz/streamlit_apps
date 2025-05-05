def exceeds_quota(bytes_processed, limit_bytes=2 * 1024 ** 3):
    """Returns True if query exceeds quota (default 2GB)."""
    return bytes_processed > limit_bytes
