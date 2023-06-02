from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import rclpy
from rclpy.node import Node
from std_msgs.msg import Header
from geometry_msgs.msg import Point, PointStamped
from sensor_msgs.msg import PointCloud2, PointField
from sensor_msgs_py import point_cloud2
from shapely import wkb


class PostgreSQLConnection:
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
        engine = create_engine(connection_uri)
        try:
            engine.connect()
        except:
            print(f"Could not connect to uri: {connection_uri}")
            print(f"Check the postgresql section of the YAML file.")
            exit()

        Session = sessionmaker(bind=engine)
        self.session = Session()
        print("Connected...")


class PostGisConverter:
    @staticmethod
    def to_point(geometry):
        point = wkb.loads(geometry, hex=True)
        return Point(x=point.x, y=point.y, z=point.z)

    def to_point_tuple(geometry):
        point = wkb.loads(geometry, hex=True)
        return (point.x, point.y, point.z)


class Point3DQuery:
    type = "Point3D"

    def __init__(self, node: Node, config: str):
        node.declare_parameters(
            namespace="",
            parameters=[
                (f"{config}.query", ""),
                (f"{config}.topic", "points"),
                (f"{config}.frame_id", ""),
                (f"{config}.rate", 10.0),
            ],
        )

        self.query = node.get_parameter(f"{config}.query").value
        self.topic = node.get_parameter(f"{config}.topic").value
        self.frame_id = node.get_parameter(f"{config}.frame_id").value
        self.rate = node.get_parameter(f"{config}.rate").value

        self.publisher_ = node.create_publisher(PointStamped, self.topic, 10)

    def publish(self, result, timestamp):
        for element in result:
            frame_id = (
                element.frame_id if hasattr(element, "frame_id") else self.frame_id
            )
            point_stamped = PointStamped(
                header=Header(frame_id=frame_id, stamp=timestamp.to_msg()),
                point=PostGisConverter.to_point(element.geometry),
            )

            self.publisher_.publish(point_stamped)

    def __repr__(self):
        return f'{self.__class__.__name__}: query: "{self.query}" topic: {self.topic}, frame_id: {self.frame_id}'


class PointCloud2Query:
    type = "PointCloud2"

    def __init__(self, node: Node, config: str):
        node.declare_parameters(
            namespace="",
            parameters=[
                (f"{config}.query", ""),
                (f"{config}.topic", "points"),
                (f"{config}.frame_id", ""),
                (f"{config}.rate", 10.0),
            ],
        )

        self.query = node.get_parameter(f"{config}.query").value
        self.topic = node.get_parameter(f"{config}.topic").value
        self.frame_id = node.get_parameter(f"{config}.frame_id").value
        self.rate = node.get_parameter(f"{config}.rate").value

        self.publisher_ = node.create_publisher(PointCloud2, self.topic, 10)

    def publish(self, result, timestamp):
        pointcloud_msg = PointCloud2()

        header = Header(frame_id=self.frame_id, stamp=timestamp.to_msg())
        points = [
            PostGisConverter.to_point_tuple(element.geometry) for element in result
        ]
        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)

        self.publisher_.publish(pointcloud_msg)

    def __repr__(self):
        return f'{self.__class__.__name__}: query: "{self.query}" topic: {self.topic}, frame_id: {self.frame_id}'

    def __str__(self):
        return f'{self.__class__.__name__}: \n query: "{self.query}" \b topic: {self.topic}, \n frame_id: {self.frame_id}'


query_converter = {q.type: q for q in [Point3DQuery, PointCloud2Query]}


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
        queries = []

        for config in configurations:
            self.declare_parameter(f"{config}.type", rclpy.Parameter.Type.STRING)
            query_type = self.get_parameter(f"{config}.type").value
            if not query_type in query_converter.keys():
                raise ValueError(
                    f"Type: '{query_type}' is not supported. Supported: {query_converter.keys()}"
                )
            query_node = query_converter[query_type](self, config)
            queries.append(query_node)
            self.get_logger().info(str(query_node))

        self.queries = queries

        self.postgresql_connection = PostgreSQLConnection(self)

        # TODO: Timer per topic? Different update times?
        timer_period = 1.0  # seconds
        self.timer_ = self.create_timer(timer_period, self.timer_callback)

    def timer_callback(self):
        for query in self.queries:
            elements = self.postgresql_connection.session.execute(text(query.query))
            # if not all(column in elements.keys() for column in non_optional_columns):
            #     print('Missing one ore more non-optional column.')
            #     return
            elements = elements.all()
            if not elements:
                print("Query returned no data.")
                return
            now = self.get_clock().now()

            query.publish(elements, now)


def main(args=None):
    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
