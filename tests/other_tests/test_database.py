#!/usr/bin/env python3

import os
import unittest
from typing import Any, ClassVar, cast

import psycopg2.extensions

from cdedb.backend.common import (
    DatabaseLock,
    Silencer,
    _affirm_atomized_context,  # noqa: PLC2701
)
from cdedb.common import RequestState
from cdedb.config import Config, SecretsConfig
from cdedb.database import DATABASE_ROLES, DBRole
from cdedb.database.connection import (
    Atomizer,
    ConnectionContainer,
    IrradiatedConnection,
    connection_pool_factory,
)


class TestDatabase(unittest.TestCase):
    config: ClassVar[Config]
    secrets: ClassVar[SecretsConfig]

    @classmethod
    def setUpClass(cls, *args: Any, **kwargs: Any) -> None:
        super().setUpClass(*args, **kwargs)
        cls.config = Config()
        cls.secrets = SecretsConfig()

    def test_instant_connection(self) -> None:
        factory = connection_pool_factory(
            self.config["CDB_DATABASE_NAME"],
            DATABASE_ROLES,
            self.secrets["CDB_DATABASE_ROLES"],
            self.config["DB_HOST"],
            self.config["DB_PORT"],
        )
        with factory(DBRole.persona) as conn:
            self.assertIsInstance(conn, psycopg2.extensions.connection)
            self.assertIsInstance(conn, IrradiatedConnection)

    def test_pooled_connection(self) -> None:
        factory = connection_pool_factory(
            self.config["CDB_DATABASE_NAME"],
            (DBRole.anonymous, DBRole.persona),
            self.secrets["CDB_DATABASE_ROLES"],
            self.config["DB_HOST"],
            self.config["DB_PORT"],
            max_idle=1,
        )
        conn = factory(DBRole.persona)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 1234")
                cur.execute("SELECT pg_backend_pid() AS pid")
                pid = cur.fetchone()["pid"]
        conn.close()
        self.assertFalse(conn.closed)

        # Connections are reused per role ...
        reused = factory(DBRole.persona)
        self.assertIs(conn, reused)
        other = factory(DBRole.anonymous)
        self.assertIsNot(conn, other)
        # ... but without any leftover session state.
        with reused:
            with reused.cursor() as cur:
                cur.execute("SELECT pg_backend_pid() AS pid")
                self.assertEqual(pid, cur.fetchone()["pid"])
                cur.execute("SHOW statement_timeout")
                self.assertEqual("0", cur.fetchone()["statement_timeout"])
                cur.execute("SHOW client_encoding")
                self.assertEqual("UTF8", cur.fetchone()["client_encoding"])

        # Connections exceeding max_idle are closed for real.
        surplus = factory(DBRole.persona)
        reused.close()
        surplus.close()
        self.assertFalse(reused.closed)
        self.assertTrue(surplus.closed)
        self.assertIs(reused, factory(DBRole.persona))
        reused.close()

        # Idle connections are not shared with forked processes.
        if (pid := os.fork()) == 0:
            os._exit(0)  # pragma: no cover
        os.waitpid(pid, 0)
        self.assertTrue(reused.closed)
        self.assertIsNot(reused, factory(DBRole.persona))

    def test_less_users(self) -> None:
        factory = connection_pool_factory(
            self.config["CDB_DATABASE_NAME"],
            (DBRole.anonymous, DBRole.admin),
            self.secrets["CDB_DATABASE_ROLES"],
            self.config["DB_HOST"],
            self.config["DB_PORT"],
        )
        with self.assertRaises(ValueError):
            factory(DBRole.persona)

    def test_atomizer(self) -> None:
        factory = connection_pool_factory(
            self.config["CDB_DATABASE_NAME"],
            (DBRole.persona,),
            self.secrets["CDB_DATABASE_ROLES"],
            self.config["DB_HOST"],
            self.config["DB_PORT"],
        )
        conn = factory(DBRole.persona)

        rs = ConnectionContainer()
        rs.conn = rs._conn = conn
        with Atomizer(rs) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM core.personas")
            with rs.conn as nested_conn:
                with nested_conn.cursor() as nested_cur:
                    nested_cur.execute("SELECT * FROM core.sessions")
            self.assertNotEqual(psycopg2.extensions.STATUS_READY, nested_conn.status)
        self.assertEqual(psycopg2.extensions.STATUS_READY, nested_conn.status)

        rs = cast(RequestState, rs)

        with self.assertRaises(RuntimeError):
            _affirm_atomized_context(rs)
        with Atomizer(rs):
            _affirm_atomized_context(rs)

        # The following context managers raise an error during __enter__.
        #  The pass is required syntactically, but never run.
        with Atomizer(rs):
            with self.assertRaises(RuntimeError):
                with DatabaseLock(rs):
                    pass  # pragma: no cover

        # Add missing attribute of actual RequestState.
        rs.is_quiet = False

        with self.assertRaises(RuntimeError):
            with Silencer(rs):
                pass  # pragma: no cover

        with Atomizer(rs):
            with Silencer(rs):
                with self.assertRaises(RuntimeError):
                    with Silencer(rs):
                        pass  # pragma: no cover

    def test_suppressed_exception(self) -> None:
        factory = connection_pool_factory(
            self.config["CDB_DATABASE_NAME"],
            (DBRole.admin,),
            self.secrets["CDB_DATABASE_ROLES"],
            self.config["DB_HOST"],
            self.config["DB_PORT"],
        )
        conn = factory(DBRole.admin)

        rs = ConnectionContainer()
        rs.conn = rs._conn = conn
        with self.assertRaises(RuntimeError):
            with Atomizer(rs) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM core.personas")
                # don't do this at home
                # this is an anti-pattern
                try:
                    with rs.conn as nested_conn:
                        with nested_conn.cursor() as nested_cur:
                            nested_cur.execute(
                                "UPDATE core.personas SET given_names"
                                " = 'ABBA (random name)'"
                            )
                            raise ValueError("test error")
                except ValueError:
                    pass
        with rs.conn as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT given_names FROM core.personas")
                result = cur.fetchall()
                self.assertFalse(
                    any(x['given_names'] == "ABBA (random name)" for x in result)
                )
