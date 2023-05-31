from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import rclpy
from rclpy.node import Node
from std_msgs.msg import Header
from geometry_msgs.msg import Point, PointStamped

from shapely import wkb


def to_point(geometry, frame_id, timestamp):
    # Convert WKB to Shapely geometry
    point = wkb.loads(geometry, hex=True)

    # Create ROS2 PointStamped message
    point_stamped = PointStamped(
        header=Header(frame_id=frame_id, stamp = timestamp.to_msg()),
        point=Point(x=point.x, y=point.y, z=point.z)
    )
    return point_stamped


def to_line_segment(geometry, frame_id, timestamp):
    # TODO: convert to ROS2 polygon
    return 0


# TODO: Load from YAML / config
default_connection_uri = 'postgresql://postgres:postgres@localhost:5432/postgres_alchemy_ait'

# TODO: Idea: defined columns 'geometry', 'type', and 'frame_id' need to be present
query = "SELECT position AS geometry, ST_GeometryType(landmark.position) AS type, 'test_frame_id' AS frame_id FROM landmark;"

non_optional_columns = ['geometry', 'type', 'frame_id']

# TODO: If timestamp is given -> use it, otherwise -> use get_clock().now()
optional_columns = ['timestamp']
type_converter = {'ST_Point': to_point, 'ST_LineSegment': to_line_segment}


class PostGisPublisher(Node):
    def __init__(self):
        super().__init__('postgis_ros_publisher')
        engine = create_engine(default_connection_uri)
        try:
            engine.connect()
        except:
            print(f'Could not connect to schema: {default_connection_uri}')
            return

        Session = sessionmaker(bind=engine)
        self.session_ = Session()
        print('Connected...')
        self.publisher_ = self.create_publisher(PointStamped, 'points', 10)

        timer_period = 10.0  # seconds
        self.timer_ = self.create_timer(timer_period, self.timer_callback)

    def timer_callback(self):
        elements = self.session_.execute(text(query))
        if not all(column in elements.keys() for column in non_optional_columns):
            print('Missing one ore more non-optional column.')
            return
        elements = elements.all()
        if not elements:
            print('Query returned no data.')
            return
        now = self.get_clock().now()

        for element in elements:
            type = element.type
            if not type in type_converter.keys():
                print(
                    f"Type: '{type}' is not supported. Supported: {type_converter.keys()}")
                continue
            ros_msg = type_converter[type](element.geometry, element.frame_id, now)
            self.publisher_.publish(ros_msg)
            #print(ros_msg)


def main(args=None):

    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
