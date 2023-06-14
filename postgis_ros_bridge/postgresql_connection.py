"""PostgreSQL Connection Abstraction class."""
import os
from contextlib import AbstractContextManager

from rclpy.node import Node
from sqlalchemy import create_engine


class PostgreSQLConnection(AbstractContextManager):
    """PostgreSQL Connection Abstraction class."""

    def __init__(self, node: Node):
        namespace = "postgresql"
        node.declare_parameters(
            namespace="",
            parameters=[
                (f"{namespace}.user", "postgres"),
                (f"{namespace}.pass", ""),
                (f"{namespace}.pass_env", ""),
                (f"{namespace}.host", "localhost"),
                (f"{namespace}.port", 5432),
                (f"{namespace}.schema", "public"),
            ],
        )

        user = node.get_parameter(f"{namespace}.user").value
        passwd = node.get_parameter(f"{namespace}.pass").value
        if not passwd:
            passwd_env = node.get_parameter(f"{namespace}.pass_env").value
            try:
                passwd = os.environ[passwd_env]
            except KeyError as ex:
                raise ValueError(
                    f"Environment variable '{passwd_env}' is not set.") from ex

        host = node.get_parameter(f"{namespace}.host").value
        port = node.get_parameter(f"{namespace}.port").value
        schema = node.get_parameter(f"{namespace}.schema").value

        connection_uri = f"postgresql://{user}:{passwd}@{host}:{port}/{schema}"
        self.engine = create_engine(connection_uri, execution_options={
                                    'postgresql_readonly': True})

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.engine.dispose()

    def __repr__(self) -> str:
        return f"{str(self.engine.url)}"
