from sqlalchemy import create_engine, text, Result
from sqlalchemy.orm import sessionmaker

import rclpy
from builtin_interfaces.msg import Time
from rcl_interfaces.msg import ParameterDescriptor, ParameterType
from rclpy.node import Node
from rclpy.publisher import Publisher
from rclpy.parameter import Parameter
from std_msgs.msg import Header
from geometry_msgs.msg import Point, PointStamped
from sensor_msgs.msg import PointCloud2, PointField
from sensor_msgs_py import point_cloud2
from shapely import wkb
from contextlib import AbstractContextManager
from abc import ABC, abstractmethod
from typing import Dict, Any, Iterable, Tuple
from functools import partial


class PostGisConverter:
    @staticmethod
    def to_point(geometry):
        point = wkb.loads(geometry, hex=True)
        return Point(x=point.x, y=point.y, z=point.z)

    def to_point_tuple(geometry):
        point = wkb.loads(geometry, hex=True)
        return (point.x, point.y, point.z)


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
        return super().__repr__() + f"({self.engine})"


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


class QueryResultParser(ABC):
    TYPE = "AbstractParser"

    @abstractmethod
    def declare_params(self) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return []

    @abstractmethod
    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return []

    @abstractmethod
    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        return []

    def __repr__(self) -> str:
        return self.TYPE


class PointResultParser(QueryResultParser):
    TYPE = "Point3D"

    def __init__(self):
        pass

    def declare_params(self) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return [
            ('frame_id', Parameter.Type.STRING, ParameterDescriptor()),
            ('topic', Parameter.Type.STRING, ParameterDescriptor())
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        self.frame_id = params['frame_id'].value
        self.topic = params['topic'].value
        return [(self.topic, PointStamped)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        def get_frame_id(elem):
            return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'
        return ((self.topic, 
                 PointStamped(header=Header(frame_id=get_frame_id(element), stamp=time), 
                              point=PostGisConverter.to_point(element.geometry))) for element in result)

    def __repr__(self) -> str:
        return super().__repr__() + f"({self.frame_id}, {self.topic})"


class PC2ResultParser(QueryResultParser):
    TYPE = "PointCloud2"

    def __init__(self):
        pass

    def declare_params(self) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return [
            ('frame_id', Parameter.Type.STRING, ParameterDescriptor()),
            ('topic', Parameter.Type.STRING, ParameterDescriptor())
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        self.frame_id = params['frame_id'].value
        self.topic = params['topic'].value
        return [(self.topic, PointCloud2)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        def get_frame_id():
            return self.frame_id if self.frame_id else 'map'
        pointcloud_msg = PointCloud2()
        header = Header(frame_id=get_frame_id(), stamp=time)
        points = [
            PostGisConverter.to_point_tuple(element.geometry) for element in result
        ]
        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)
        return [(self.topic, pointcloud_msg)]

    def __repr__(self) -> str:
        return super().__repr__() + f"({self.frame_id}, {self.topic})"


class PostGisConverter:
    @staticmethod
    def to_point(geometry):
        point = wkb.loads(geometry, hex=True)
        return Point(x=point.x, y=point.y, z=point.z)

    def to_point_tuple(geometry):
        point = wkb.loads(geometry, hex=True)
        return (point.x, point.y, point.z)


# class Point3DQuery:
#     type = "Point3D"

#     def __init__(self, node: Node, config: str):
#         node.declare_parameters(
#             namespace="",
#             parameters=[
#                 (f"{config}.query", ""),
#                 (f"{config}.topic", "points"),
#                 (f"{config}.frame_id", ""),
#                 (f"{config}.rate", 10.0),
#             ],
#         )

#         self.query = node.get_parameter(f"{config}.query").value
#         self.topic = node.get_parameter(f"{config}.topic").value
#         self.frame_id = node.get_parameter(f"{config}.frame_id").value
#         self.rate = node.get_parameter(f"{config}.rate").value

#         self.publisher_ = node.create_publisher(PointStamped, self.topic, 10)

#     def publish(self, result, timestamp):
#         for element in result:
#             frame_id = (
#                 element.frame_id if hasattr(element, "frame_id") else self.frame_id
#             )
#             point_stamped = PointStamped(
#                 header=Header(frame_id=frame_id, stamp=timestamp.to_msg()),
#                 point=PostGisConverter.to_point(element.geometry),
#             )

#             self.publisher_.publish(point_stamped)

#     def __repr__(self):
#         return f'{self.__class__.__name__}: query: "{self.query}" topic: {self.topic}, frame_id: {self.frame_id}'


# class PointCloud2Query:
#     type = "PointCloud2"

#     def __init__(self, node: Node, config: str):
#         node.declare_parameters(
#             namespace="",
#             parameters=[
#                 (f"{config}.query", ""),
#                 (f"{config}.topic", "points"),
#                 (f"{config}.frame_id", ""),
#                 (f"{config}.rate", 10.0),
#             ],
#         )

#         self.query = node.get_parameter(f"{config}.query").value
#         self.topic = node.get_parameter(f"{config}.topic").value
#         self.frame_id = node.get_parameter(f"{config}.frame_id").value
#         self.rate = node.get_parameter(f"{config}.rate").value

#         self.publisher_ = node.create_publisher(PointCloud2, self.topic, 10)

#     def publish(self, result, timestamp):
#         pointcloud_msg = PointCloud2()

#         header = Header(frame_id=self.frame_id, stamp=timestamp.to_msg())
#         points = [
#             PostGisConverter.to_point_tuple(element.geometry) for element in result
#         ]
#         pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)

#         self.publisher_.publish(pointcloud_msg)

#     def __repr__(self):
#         return f'{self.__class__.__name__}: query: "{self.query}" topic: {self.topic}, frame_id: {self.frame_id}'

#     def __str__(self):
#         return f'{self.__class__.__name__}: \n query: "{self.query}" \b topic: {self.topic}, \n frame_id: {self.frame_id}'

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
