from functools import partial
from typing import Dict

import rclpy
from postgresql_connection import PostgreSQLConnection
from query import Query
from query_result_parser import (BasicStampedArrayParserFactory,
                                 MarkerResultParser, PC2ResultParser,
                                 PointResultParser, PoseResultParser,
                                 PoseStampedResultParser, QueryResultParser)
from rclpy.node import Node
from rclpy.publisher import Publisher
from visualization_msgs.msg import MarkerArray
from geometry_msgs.msg import PoseArray

# TODO: Maybe extension points
query_parser: Dict[str, QueryResultParser] = {
    q.TYPE: q for q in [PointResultParser, PoseResultParser, PoseStampedResultParser, PC2ResultParser, MarkerResultParser]}

query_parser.update({
    "MarkerArray": BasicStampedArrayParserFactory.create_array_parser(MarkerResultParser, MarkerArray, "markers"),
    "PoseArray": BasicStampedArrayParserFactory.create_array_parser(PoseResultParser, PoseArray, "poses"),
})


class PostGisPublisher(Node):
    def __init__(self):
        super().__init__(node_name="postgis_ros_publisher")
        self.get_logger().info(f"Starting {self.get_name()}...")

        # automatically_declare_parameters_from_overrides=True
        self.declare_parameters(
            namespace="",
            parameters=[
                ("publish", rclpy.Parameter.Type.STRING_ARRAY),
            ],
        )

        configurations = self.get_parameter("publish").value
        self.get_logger().info(f"Parsing sections: {configurations}")
        self.converter_pubs: Dict[str, Publisher] = dict()
        self.postgresql_connection = PostgreSQLConnection(self)
        self.get_logger().info(
            f"Connected to database via {self.postgresql_connection}")

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

            if not query_type in query_parser.keys():
                raise ValueError(
                    f"Type: '{query_type}' is not supported. Supported: {query_parser.keys()}"
                )

            parser = query_parser[query_type]()
            # TODO: namespace bug in rclyp Node in declare params name initialized after value query
            self.declare_parameters(
                namespace="", parameters=list(map(lambda x: (f"{config}.{x[0]}", x[1], x[2]), parser.declare_params())))
            topics_msgs = parser.set_params(
                self.get_parameters_by_prefix(config))
            query = Query(self.postgresql_connection, sql_query)

            # TODO: probably sensor data qos or later configurable
            pubs = [(t, self.create_publisher(m, t, 10))
                    for (t, m) in topics_msgs]
            self.converter_pubs.update(pubs)

            self.create_timer(
                1.0/rate, partial(self.timer_callback, query, parser))

            self.get_logger().info(f"Register parser: {str(parser)}")

    # TODO: prefetching? Async Sessions, check performance issues (pc2 sloooooow)
    def timer_callback(self, query: Query, converter: QueryResultParser):
        with query.get_results() as results:
            for t, m in converter.parse_result(results, self.get_clock().now().to_msg()):
                self.converter_pubs[t].publish(m)


def main(args=None):
    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
