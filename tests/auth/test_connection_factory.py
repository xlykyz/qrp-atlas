from contextlib import contextmanager

import pytest

from qrp_atlas.auth.dependencies import make_connection_factory
from qrp_atlas.auth.exceptions import AuthBackendUnavailableError


def test_connection_factory_translates_psycopg_errors(monkeypatch):
    class DummyError(Exception):
        pass

    class DummyPsycopg:
        Error = DummyError

        @staticmethod
        def connect(*args, **kwargs):
            raise DummyError("connection refused")

    class DummyRows:
        dict_row = object()

    import sys
    import types

    monkeypatch.setitem(sys.modules, "psycopg", DummyPsycopg)
    monkeypatch.setitem(sys.modules, "psycopg.rows", DummyRows)

    factory = make_connection_factory("postgresql://example")
    with pytest.raises(AuthBackendUnavailableError):
        with factory():
            pass
