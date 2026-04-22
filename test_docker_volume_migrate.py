"""Basic unit tests for docker_volume_migrate helpers."""

from docker_volume_migrate import (
    suggest_volume_name,
    suggest_target_path,
    validate_volume_name,
    _resolve_bind_source,
)


def test_suggest_volume_name_basic():
    name = suggest_volume_name("mycontainer", "/data")
    assert name == "mycontainer_data"


def test_suggest_volume_name_with_prefix():
    name = suggest_volume_name("mycontainer", "/data", prefix="prod_")
    assert name == "prod_mycontainer_data"


def test_suggest_volume_name_max_length():
    long_name = "a" * 40
    name = suggest_volume_name(long_name, "/some/nested/path")
    assert len(name) <= 63


def test_suggest_volume_name_starts_with_alnum():
    name = suggest_volume_name("mycontainer", "/data")
    assert name[0].isalnum()


def test_validate_volume_name_valid():
    assert validate_volume_name("my-volume_1.0") is True


def test_validate_volume_name_empty():
    assert validate_volume_name("") is False


def test_validate_volume_name_starts_with_special():
    assert validate_volume_name("-badname") is False


def test_validate_volume_name_too_long():
    assert validate_volume_name("a" * 64) is False


def test_suggest_target_path():
    path = suggest_target_path("/mnt/nfs", "mycontainer", "/data")
    assert path == "/mnt/nfs/mycontainer/data"


def test_suggest_target_path_with_prefix():
    path = suggest_target_path("/mnt/nfs", "mycontainer", "/data", prefix="prod_")
    assert path == "/mnt/nfs/prod_mycontainer/data"


def test_resolve_bind_source_absolute():
    result = _resolve_bind_source("/absolute/path", None)
    assert result == "/absolute/path"


def test_resolve_bind_source_relative():
    result = _resolve_bind_source("relative/path", "/base")
    assert result == "/base/relative/path"
