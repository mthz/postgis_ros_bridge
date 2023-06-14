"""Query class for executing SQL queries on the database."""
from contextlib import AbstractContextManager

from sqlalchemy import Result, text
from sqlalchemy.orm import sessionmaker

from postgis_ros_bridge.postgresql_connection import PostgreSQLConnection


class Query(AbstractContextManager):
    """Query class for executing SQL queries on the database."""

    def __init__(self, postgresql_conn: PostgreSQLConnection, query: str):
        session_ = sessionmaker(bind=postgresql_conn.engine)
        self._session = session_()
        self._query = text(query)

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self._session.close()

    def get_results(self) -> Result:
        """Execute the query and return the results."""
        return self._session.execute(self._query)

    def __repr__(self) -> str:
        return super().__repr__() + f"({self._query})"
