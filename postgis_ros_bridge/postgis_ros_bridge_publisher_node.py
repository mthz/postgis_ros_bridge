import rclpy
from rclpy.node import Node
from rclpy.publisher import Publisher
from typing import Dict
from functools import partial

from postgresql_connection import PostgreSQLConnection
from query_result_parser import QueryResultParser, PointResultParser, PC2ResultParser
from query import Query


# TODO: Maybe extension points
query_converter: Dict[str, QueryResultParser] = {
    q.TYPE: q for q in [PointResultParser, PC2ResultParser]}


class PostGisPublisher(Node):
    def __init__(self):
        super().__init__("postgis_ros_publisher")

        # automatically_declare_parameters_from_overrides=True
        self.declare_parameters(
            namespace="",
            parameters=[
                ("publish", rclpy.Parameter.Type.STRING_ARRAY),
            ],
        )

        configurations = self.get_parameter("publish").value
        print(f"Publishing: {configurations}")
        self.converter_pubs: Dict[str, Publisher] = dict()
        self.postgresql_connection = PostgreSQLConnection(self)

        for config in configurations:
            self.declare_parameters(
                namespace="",
                parameters=[
                    (f"{config}.query", rclpy.Parameter.Type.STRING),
                    (f"{config}.rate", rclpy.Parameter.Type.DOUBLE),
                    (f"{config}.type", rclpy.Parameter.Type.STRING)
                ],
            )
            query_type = self.get_parameter(f"{config}.type").value
            sql_query = self.get_parameter(f"{config}.query").value
            rate = self.get_parameter(f"{config}.rate").value

            if not query_type in query_converter.keys():
                raise ValueError(
                    f"Type: '{query_type}' is not supported. Supported: {query_converter.keys()}"
                )

            converter = query_converter[query_type]()
            # TODO: namespace bug in rclyp Node in declape params name initialized after value query
            self.declare_parameters(
                namespace="", parameters=list(map(lambda x: (f"{config}.{x[0]}", x[1], x[2]), converter.declare_params())))
            topics_msgs = converter.set_params(
                self.get_parameters_by_prefix(config))
            query = Query(self.postgresql_connection, sql_query)

            # TODO: probably sensor data qos or later configurable
            pubs = [(t, self.create_publisher(m, t, 10))
                    for (t, m) in topics_msgs]
            self.converter_pubs.update(pubs)

            self.create_timer(
                1.0/rate, partial(self.timer_callback, query, converter))

            self.get_logger().info(str(converter))

    def timer_callback(self, query: Query, converter: QueryResultParser):
        for t, m in converter.parse_result(query.get_results(), self.get_clock().now().to_msg()):
            self.converter_pubs[t].publish(m)


def main(args=None):
    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
