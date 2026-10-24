#!/usr/bin/env python3

"""This module provides our python interface to the database.

Note the class :py:class:`Atomizer` which takes care of creating atomic
contexts for database transactions across arbitrary backend logic.

This should be the only module which makes subsistantial use of psycopg.
"""

import enum
import functools
import logging
import os
import threading
import time
from collections.abc import Callable, Collection, Mapping
from types import TracebackType
from typing import Any

import psycopg2
import psycopg2.extensions
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE as SERIALIZABLE
from psycopg2.extras import RealDictCursor


class ConnectionContainer:
    # Visible version of the database connection
    # noinspection PyTypeChecker
    conn: "IrradiatedConnection"
    # Private version of the database connection, only visible in the
    # backends (mediated by the make_proxy)
    # noinspection PyTypeChecker
    _conn: "IrradiatedConnection"


class DBRole(enum.Enum):
    """
    Privilege levels available in the SQL database.

    (Where we have less differentiation for the sake of simplicity.)

    The appropriate db role can be determined from a users 'Roles' via 'get_db_role()'.

    >>> from cdedb.common.roles import Roles
    >>> Roles.cde_admin.get_db_role()
    <DBRole.admin: 'cdb_admin'>
    >>> Roles.cde.get_db_role()
    <DBRole.member: 'cdb_member'>
    >>> Roles.assembly.get_db_role()
    <DBRole.member: 'cdb_member'>
    >>> Roles.persona.get_db_role()
    <DBRole.persona: 'cdb_persona'>
    >>> Roles.anonymous.get_db_role()
    <DBRole.anonymous: 'cdb_anonymous'>

    'get_db_role()' assumes to be called on a fully populated roles object.

    >>> Roles.event.get_db_role()
    <DBRole.anonymous: 'cdb_anonymous'>
    >>> Roles.ml.get_db_role()
    <DBRole.anonymous: 'cdb_anonymous'>
    >>> Roles.member.get_db_role()
    <DBRole.anonymous: 'cdb_anonymous'>
    >>> Roles.searchable.get_db_role()
    <DBRole.anonymous: 'cdb_anonymous'>
    """

    admin = "cdb_admin"
    member = "cdb_member"
    persona = "cdb_persona"
    anonymous = "cdb_anonymous"


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, None)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, None)

_LOGGER = logging.getLogger(__name__)


def _create_connection(
    dbname: str,
    dbuser: DBRole,
    password: str,
    host: str,
    port: int,
    isolation_level: int | None = SERIALIZABLE,
) -> "IrradiatedConnection":
    """This creates a wrapper around :py:class:`psycopg2.extensions.connection`
    and correctly initializes the database connection.

    :param isolation_level: Isolation level of database connection, a
        constant coming from :py:mod:`psycopg2.extensions`. This should be used
        very sparingly!
    :returns: open database connection
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=dbuser.value,
        password=password,
        host=host,
        port=port,
        # Send this on startup, so it survives the DISCARD ALL of _ConnectionPool.
        client_encoding="UTF8",
        connection_factory=IrradiatedConnection,
        cursor_factory=RealDictCursor,
    )
    conn.set_client_encoding("UTF8")
    conn.set_session(isolation_level)
    _LOGGER.debug(f"Created connection to {dbname} as {dbuser}")
    return conn


class _ConnectionPool:
    """Idle connections of one process for one database role.

    Connections are handed out by :py:func:`connection_pool_factory` and come
    back here when :py:meth:`IrradiatedConnection.close` is called on them.
    Connections which are never closed explicitly are simply garbage collected.
    """

    # Check that idle connections are still alive before handing them out.
    PING_AFTER = 30.0  # seconds

    def __init__(self, connect: Callable[[], "IrradiatedConnection"], max_idle: int):
        self._connect = connect
        self._max_idle = max_idle
        self._idle: list[tuple[IrradiatedConnection, float]] = []
        self._lock = threading.Lock()

    def close_idle(self) -> None:
        with self._lock:
            idle, self._idle = self._idle, []
        for conn, _ in idle:
            conn.close()

    def get(self) -> "IrradiatedConnection":
        while True:
            with self._lock:
                if not self._idle:
                    break
                conn, since = self._idle.pop()
            if time.monotonic() - since < self.PING_AFTER or _ping(conn):
                conn._pool = self
                return conn
            _LOGGER.info("Dropping dead pooled connection.")
            conn.close()
        conn = self._connect()
        conn._pool = self
        return conn

    def put(self, conn: "IrradiatedConnection") -> None:
        """Reset the connection and keep it, if there is room."""
        try:
            conn.rollback()
            conn.autocommit = True
            with conn.cursor() as cur:
                # Clear all session state (prepared statements, locks, ...).
                cur.execute("DISCARD ALL")
            conn.autocommit = False
        except psycopg2.Error:
            _LOGGER.info("Dropping broken pooled connection.", exc_info=True)
            conn.close()
            return
        conn.reset_state()
        with self._lock:
            if len(self._idle) < self._max_idle:
                self._idle.append((conn, time.monotonic()))
                return
        conn.close()


def _ping(conn: "IrradiatedConnection") -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.rollback()
    except psycopg2.Error:
        return False
    return True


_POOLS: dict[tuple[str, DBRole, str, int, int | None, int], _ConnectionPool] = {}
_POOLS_LOCK = threading.Lock()


def _close_idle_connections() -> None:
    """Close idle connections before forking.

    Otherwise parent and child would share the same server sessions.
    The child cannot even close them, as that would terminate them for the parent.
    """
    with _POOLS_LOCK:
        pools = list(_POOLS.values())
    for pool in pools:
        pool.close_idle()


os.register_at_fork(before=_close_idle_connections)


def connection_pool_factory(
    dbname: str,
    roles: Collection[DBRole],
    db_passwords: Mapping[str, str],
    host: str,
    port: int,
    isolation_level: int | None = SERIALIZABLE,
    max_idle: int = 0,
) -> Callable[[DBRole], "IrradiatedConnection"]:
    """This returns callable which takes a database role and returns a
    connection using that role.

    Database connections are a costly good (in memory terms), so it is
    wise to create them only when necessary. Since this is costly in
    itself (in time terms) it is advisable to use connection pooling,
    either via an external pooler (e.g. pgbouncer) or by setting ``max_idle``.
    Additionally this approach offers thread-safety
    since connetions created at runtime are not shared between threads.

    :param roles: roles for which database connections shall be available.
    :param db_passwords: container for db passwords.
    :param isolation_level: Isolation level of database connection, a
        constant coming from :py:mod:`psycopg2.extensions`.
        This should be used very sparingly!
    :param max_idle: If positive, closed connections are reset and kept for reuse,
        up to this many per role. The pools are shared by all factories of
        this process with the same parameters.
    """
    roles = frozenset(roles)
    passwords: Mapping[str, str] = dict(db_passwords)

    def connect(role: DBRole) -> "IrradiatedConnection":
        if role not in roles:
            raise ValueError(f"Role {role!r} not available.")
        if max_idle <= 0:
            return _create_connection(
                dbname, role, passwords[role.value], host, port, isolation_level
            )
        key = (dbname, role, host, port, isolation_level, max_idle)
        with _POOLS_LOCK:
            if key not in _POOLS:
                _POOLS[key] = _ConnectionPool(
                    functools.partial(
                        _create_connection,
                        dbname,
                        role,
                        passwords[role.value],
                        host,
                        port,
                        isolation_level,
                    ),
                    max_idle,
                )
            pool = _POOLS[key]
        return pool.get()

    _LOGGER.debug(f"Initialised connection factory for roles {roles}")
    return connect


# noinspection PyProtectedMember
class Atomizer:
    """Helper to create atomic transactions.

    The backend stores the database connection in the
    :py:attr:`cdedb.common.RequestState.conn` attribute. This
    connection is then used for all queries in this request, utilizing
    ``with`` contexts to control transactions. However if several of
    these contexts are nested for the same connection, the (regular)
    termination of the innermost context will commit the
    transaction. This is surprising for the outer contexts which then do
    not work as intended.

    To mitigate this problem this class can be used. The pattern is as
    follows::

        with Atomizer(rs) as conn:
            with conn.cursor as cur():
                ## do stuff with nested contexts

    Every database connection is of type
    :py:class:`IrradiatedConnection` (a child of
    :py:class:`psycopg2.extensions.connection`) which keeps track of the
    nested contexts and only commits the transaction once the outermost
    context is left. This does not pose a problem for the inner contexts
    since their actions are still atomic.

    For the error case we must be careful not to catch exceptions in
    between nested contexts, otherwise we may miss to trigger a
    transaction rollback. This is in line with failing loudly if
    something goes wrong. We detect suppressed exceptions and complain
    about them, however they are still a bad idea.
    """

    def __init__(self, rs: ConnectionContainer):
        self.rs = rs

    def __enter__(self) -> "IrradiatedConnection":
        self.rs._conn.contaminate()
        return self.rs._conn.__enter__()

    def __exit__(
        self,
        atype: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.rs._conn.decontaminate()
        return self.rs._conn.__exit__(atype, value, tb)


class IrradiatedConnection(psycopg2.extensions.connection):
    """Minimally modified version of :py:class:`psycopg2.extensions.connection`
    to facilitate :py:class:`Atomizer`.

    This is a context object which can be used with the ``with``
    statement as it is derived from
    :py:class:`psycopg2.extensions.connection`.

    See :py:class:`Atomizer` for the documentation.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # set while handed out by a _ConnectionPool
        self._pool: _ConnectionPool | None = None
        self.reset_state()

    def reset_state(self) -> None:
        self._radiation_level = 0
        # keep a copy of any exception we encounter.
        self._saved_etype: type[BaseException] | None = None
        self._saved_evalue: BaseException | None = None
        self._saved_tb: TracebackType | None = None

    def close(self) -> None:
        """Return the connection to its pool, if any, otherwise close it."""
        pool, self._pool = self._pool, None
        if pool is not None and not self.closed:
            pool.put(self)
        else:
            super().close()

    def __enter__(self) -> "IrradiatedConnection":
        if self._radiation_level:
            return self
        else:
            if self.status != psycopg2.extensions.STATUS_READY:
                raise RuntimeError("Connection in use!")  # pragma: no cover
            # clear saved exception
            self._saved_etype = None
            self._saved_evalue = None
            self._saved_tb = None
            return super().__enter__()

    def __exit__(
        self,
        etype: type[BaseException] | None,
        evalue: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._radiation_level:
            # grab any exception
            self._saved_etype = etype or self._saved_etype
            self._saved_evalue = evalue or self._saved_evalue
            self._saved_tb = tb or self._saved_tb
            return None
        else:
            if not etype and self._saved_etype:
                # we encountered an exception but it was suppressed
                # somewhere -- this is bad, because it breaks the
                # with-contexts.

                # first we rollback the transaction
                super().__exit__(self._saved_etype, self._saved_evalue, self._saved_tb)
                # second we raise an exception to complain
                raise RuntimeError("Suppressed exception detected")
            return super().__exit__(etype, evalue, tb)

    # Override this to annotate, that we always use a RealDictCursor.
    def cursor(self, *args: Any, **kwargs: Any) -> RealDictCursor:  # type: ignore[override]
        return super().cursor(*args, **kwargs)

    def contaminate(self) -> None:
        """Increase recursion by one."""
        self._radiation_level += 1

    def decontaminate(self) -> None:
        """Reduce recursion by one."""
        if self._radiation_level <= 0:
            raise RuntimeError("No contamination!")  # pragma: no cover
        self._radiation_level -= 1

    @property
    def is_contaminated(self) -> bool:
        """Test for usage af an Atomizer higher up the in the stack."""
        return bool(self._radiation_level)
