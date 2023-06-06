from sqlalchemy import text, Result
from sqlalchemy.orm import sessionmaker

from contextlib import AbstractContextManager

from postgresql_connection import PostgreSQLConnection


class Query(AbstractContextManager):
    def __init__(self, postgresql_conn: PostgreSQLConnection, query: str):
        Session = sessionmaker(bind=postgresql_conn.engine)
        self._session = Session()
        self._query = text(query)

    def __enter__(self):
        return self

    def __exit__(self):
        self._session.close()

    def get_results(self) -> Result:
        return self._session.execute(self._query)

    def __repr__(self) -> str:
        return super().__repr__() + f"({self._query})"

