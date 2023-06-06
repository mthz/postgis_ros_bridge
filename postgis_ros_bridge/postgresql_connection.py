from sqlalchemy import create_engine
from rclpy.node import Node
from contextlib import AbstractContextManager


class PostgreSQLConnection(AbstractContextManager):
    def __init__(self, node: Node):
        ns = "postgresql"
        node.declare_parameters(
            namespace="",
            parameters=[
                (f"{ns}.user", "postgres"),
                (f"{ns}.pass", "postgres"),
                (f"{ns}.host", "localhost"),
                (f"{ns}.port", 5432),
                (f"{ns}.schema", "public"),
            ],
        )

        user = node.get_parameter(f"{ns}.user").value
        passwd = node.get_parameter(f"{ns}.pass").value
        host = node.get_parameter(f"{ns}.host").value
        port = node.get_parameter(f"{ns}.port").value
        schema = node.get_parameter(f"{ns}.schema").value

        connection_uri = f"postgresql://{user}:{passwd}@{host}:{port}/{schema}"
        self.engine = create_engine(connection_uri, execution_options={
                                    'postgresql_readonly': True})

    def __enter__(self):
        return self

    def __exit__(self):
        self.engine.dispose()

    def __repr__(self) -> str:
        return f"{str(self.engine.url)}"

