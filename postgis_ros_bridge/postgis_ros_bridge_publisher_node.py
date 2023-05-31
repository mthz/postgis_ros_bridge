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
    # TODO: Handle different type -> other publisher
    return PointStamped()


# TODO: Load from YAML / config
default_connection_uri = 'postgresql://postgres:postgres@localhost:5432/postgres_alchemy_ait'

# TODO: Idea: defined columns 'geometry', 'type', and 'frame_id' need to be present
query = "SELECT position AS geometry, ST_GeometryType(landmark.position) AS type, 'test_frame_id' AS frame_id FROM landmark;"
# query0 = "SELECT ST_MakePoint(75.15, 29.53, 1.0) AS geometry, ST_GeometryType(ST_MakePoint(75.15, 29.53, 1.0)) AS type, 'test' AS frame_id;"
# query1 = "SELECT ST_MakePoint(10.15, 13.53, 0.0) AS geometry, ST_GeometryType(ST_MakePoint(10.15, 13.53, 0.0)) AS type, 'test' AS frame_id;"
# query2 = "SELECT ST_MakePolygon( 'LINESTRING(75.15 29.53 1,77 29 1,77.6 29.5 1, 75.15 29.53 1)') AS geometry, ST_GeometryType(ST_MakePolygon( 'LINESTRING(75.15 29.53 1,77 29 1,77.6 29.5 1, 75.15 29.53 1)')) AS type, 'test' AS frame_id;"
# queries = [query0, query1, query2]
queries = [query]

non_optional_columns = ['geometry', 'type', 'frame_id']

# TODO: If timestamp is given -> use it, otherwise -> use get_clock().now()
optional_columns = ['timestamp']
type_converter = {'ST_Point': to_point, 'ST_Polygon': to_line_segment}


class Point3DQuery:
    def __init__(self, node, config):
        node.declare_parameter(f"{config}.query", rclpy.Parameter.Type.STRING)
        node.declare_parameter(f"{config}.topic", rclpy.Parameter.Type.STRING)
        node.declare_parameter(f"{config}.frame_id", rclpy.Parameter.Type.STRING)
        
        self.query = node.get_parameter(f"{config}.query").value
        self.topic = node.get_parameter(f"{config}.topic").value
        self.frame_id = node.get_parameter(f"{config}.frame_id").value

    def __repr__(self):
        return f"Point3DQuery: query: \"{self.query}\" topic: {self.topic}, frame_id: {self.frame_id}"
    


class PostGisPublisher(Node):
    def __init__(self):
        super().__init__('postgis_ros_publisher')
        
        self.declare_parameters(
            namespace='',
            parameters=[
                ('publish',  rclpy.Parameter.Type.STRING_ARRAY)
            ])
        
        configurations = self.get_parameter('publish').value
        print(f"Publishing: {configurations}")
        queries = []

        for config in configurations:
            self.declare_parameter(f"{config}.type", rclpy.Parameter.Type.STRING)
            query_type = self.get_parameter(f"{config}.type").value
            if query_type == "Point3D":
                queries.append(Point3DQuery(self, config))

        
        print(queries)
        self.queries = queries
      
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

        timer_period = 1.0  # seconds
        self.timer_ = self.create_timer(timer_period, self.timer_callback)





    def timer_callback(self):

        for query in self.queries:
            elements = self.session_.execute(text(query.query))
            # if not all(column in elements.keys() for column in non_optional_columns):
            #     print('Missing one ore more non-optional column.')
            #     return
            elements = elements.all()
            if not elements:
                print('Query returned no data.')
                return
            now = self.get_clock().now()

            for element in elements:
                # type = element.type
                # if not type in type_converter.keys():
                #     print(
                #         f"Type: '{type}' is not supported. Supported: {type_converter.keys()}")
                #     continue
                type = 'ST_Point'
                ros_msg = type_converter[type](element.geometry, element.frame_id, now)
                self.publisher_.publish(ros_msg)
                #print(ros_msg)


        # for query in queries:
        #     elements = self.session_.execute(text(query))
        #     if not all(column in elements.keys() for column in non_optional_columns):
        #         print('Missing one ore more non-optional column.')
        #         return
        #     elements = elements.all()
        #     if not elements:
        #         print('Query returned no data.')
        #         return
        #     now = self.get_clock().now()

        #     for element in elements:
        #         type = element.type
        #         if not type in type_converter.keys():
        #             print(
        #                 f"Type: '{type}' is not supported. Supported: {type_converter.keys()}")
        #             continue
        #         ros_msg = type_converter[type](element.geometry, element.frame_id, now)
        #         self.publisher_.publish(ros_msg)
        #         #print(ros_msg)


def main(args=None):

    rclpy.init(args=args)
    postgis_publisher = PostGisPublisher()
    rclpy.spin(postgis_publisher)

    postgis_publisher.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
