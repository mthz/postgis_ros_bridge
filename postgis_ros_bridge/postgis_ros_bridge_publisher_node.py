"""PostGIS ROS Bridge Publisher Node."""
from functools import partial
from typing import Dict

import rclpy
from geometry_msgs.msg import PoseArray
from rclpy.node import Node
from rclpy.publisher import Publisher
from visualization_msgs.msg import MarkerArray

from postgis_ros_bridge.postgresql_connection import PostgreSQLConnection
from postgis_ros_bridge.query import Query
from postgis_ros_bridge.query_result_parser import (
    BasicStampedArrayParserFactory, MarkerResultParser, PC2ResultParser,
    PointResultParser, PolygonResultParser, PolygonStampedResultParser,
    PoseResultParser, PoseStampedResultParser, QueryResultDefaultParameters,
    QueryResultParser)


query_parser: Dict[str, QueryResultParser] = {
    q.TYPE: q for q in [
        PointResultParser,
        PoseResultParser,
        PoseStampedResultParser,
        PC2ResultParser,
        MarkerResultParser,
        PolygonResultParser,
        PolygonStampedResultParser
    ]}

query_parser.update({
    "MarkerArray":
        BasicStampedArrayParserFactory.create_array_parser(
            MarkerResultParser, MarkerArray, "markers"),
    "PoseArray":
        BasicStampedArrayParserFactory.create_array_parser(
            PoseResultParser, PoseArray, "poses"),
})


class PostGisPublisher(Node):
    """PostGIS ROS Bridge Publisher Node."""

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

        # get common default settings
        default_section_name = "query_defaults"
        default_parameters = QueryResultDefaultParameters()
        self.declare_parameters(
            namespace="",
            parameters=list(map(lambda x: (f"{default_section_name}.{x[0]}", x[1], x[2]),
                                default_parameters.declare_params())))
        default_parameters.set_params(
            self.get_parameters_by_prefix(default_section_name))

        for config in configurations:
            self.declare_parameters(
                namespace="",
                parameters=[
                    (f"{config}.query", rclpy.Parameter.Type.STRING),
                    (f"{config}.rate", default_parameters.rate),
                    (f"{config}.type", rclpy.Parameter.Type.STRING),
                ],
            )
            query_type = self.get_parameter(f"{config}.type").value
            sql_query = self.get_parameter(f"{config}.query").value
            rate = self.get_parameter(f"{config}.rate").value

            if query_type not in query_parser.keys():
                raise ValueError(
                    f"Type: '{query_type}' is not supported. Supported: {query_parser.keys()}"
                )

            parser = query_parser[query_type]()
            # TODO: namespace bug in rclpy Node declare
            # params name initialized after value query [fixme]
            self.declare_parameters(
                namespace="", parameters=list(
                    map(lambda x: (f"{config}.{x[0]}", x[1], x[2]),
                        parser.declare_params(default_parameters))))
            topics_msgs = parser.set_params(
                self.get_parameters_by_prefix(config))
            query = Query(self.postgresql_connection, sql_query)

            # TODO: probably sensor data qos or later configurable [fixme]
            pubs = [(t, self.create_publisher(m, t, 10))
                    for (t, m) in topics_msgs]
            self.converter_pubs.update(pubs)

            self.create_timer(
                1.0/rate, partial(self.timer_callback, query, parser))

            self.get_logger().info(f"Register parser: {str(parser)}")

    def timer_callback(self, query: Query, converter: QueryResultParser):
        """Timer callback for all queries."""
        with query.get_results() as results:
            for topic, message in converter.parse_result(results, self.get_clock().now().to_msg()):
                self.converter_pubs[topic].publish(message)


def main(args=None):
    """ROS main function."""
    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
