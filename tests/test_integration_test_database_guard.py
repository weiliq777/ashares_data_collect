from __future__ import annotations

import pytest

from data_collect.data_access.schema import _assert_integration_test_config


@pytest.fixture(autouse=True)
def clear_integration_profile(monkeypatch):
    monkeypatch.delenv("ASHARES_ENV", raising=False)
    monkeypatch.delenv("ASHARES_TEST_ENVIRONMENT_ID", raising=False)
    monkeypatch.delenv("ASHARES_TEST_DATABASE_PORT", raising=False)


def test_non_integration_profile_does_not_change_connection_configuration():
    assert _assert_integration_test_config({"database": "ashares"}) is None


def test_integration_profile_requires_the_exact_test_endpoint(monkeypatch):
    monkeypatch.setenv("ASHARES_ENV", "integration_test")
    monkeypatch.setenv("ASHARES_TEST_ENVIRONMENT_ID", "fixture-id")

    with pytest.raises(RuntimeError, match="integration-test database target rejected"):
        _assert_integration_test_config({
            "host": "127.0.0.1",
            "port": 15432,
            "user": "ashares",
            "database": "ashares",
        })


def test_integration_profile_requires_persisted_environment_identity(monkeypatch):
    monkeypatch.setenv("ASHARES_ENV", "integration_test")
    monkeypatch.delenv("ASHARES_TEST_ENVIRONMENT_ID", raising=False)

    with pytest.raises(RuntimeError, match="ASHARES_TEST_ENVIRONMENT_ID"):
        _assert_integration_test_config({
            "host": "127.0.0.1",
            "port": 15433,
            "user": "ashares_app",
            "database": "ashares_test",
        })


def test_integration_profile_returns_expected_identity(monkeypatch):
    monkeypatch.setenv("ASHARES_ENV", "integration_test")
    monkeypatch.setenv("ASHARES_TEST_ENVIRONMENT_ID", "fixture-id")

    assert _assert_integration_test_config({
        "host": "127.0.0.1",
        "port": 15433,
        "user": "ashares_app",
        "database": "ashares_test",
    }) == "fixture-id"


def test_isolated_instance_requires_its_explicit_test_port(monkeypatch):
    monkeypatch.setenv("ASHARES_ENV", "integration_test")
    monkeypatch.setenv("ASHARES_TEST_ENVIRONMENT_ID", "fixture-id")
    monkeypatch.setenv("ASHARES_TEST_DATABASE_PORT", "15434")

    assert _assert_integration_test_config({
        "host": "127.0.0.1",
        "port": 15434,
        "user": "ashares_app",
        "database": "ashares_test",
    }) == "fixture-id"
    with pytest.raises(RuntimeError, match="integration-test database target rejected"):
        _assert_integration_test_config({
            "host": "127.0.0.1",
            "port": 15433,
            "user": "ashares_app",
            "database": "ashares_test",
        })
