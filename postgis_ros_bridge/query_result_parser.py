from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Tuple

from builtin_interfaces.msg import Duration, Time
from geometry_msgs.msg import PointStamped, Vector3, Pose, PoseStamped, Polygon, PolygonStamped
from postgis_ros_bridge.postgis_converter import PostGisConverter
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.parameter import Parameter
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
from sqlalchemy import Result, Row
from std_msgs.msg import ColorRGBA, Header
from visualization_msgs.msg import Marker




class QueryResultDefaultParameters:
    def __init__(self):
        pass

    def declare_params(self) -> Iterable[Tuple[str, Any]]:
        return [
                    (f"rate", 7.0, ParameterDescriptor()),
                    (f"frame_id", "map", ParameterDescriptor()),
                    (f"utm_transform", False, ParameterDescriptor()),
                    (f"utm_offset.lat", Parameter.Type.STRING, ParameterDescriptor()),
                    (f"utm_offset.lon", Parameter.Type.STRING, ParameterDescriptor())
                ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        self._rate = params['rate'].value
        self._frame_id = params['frame_id'].value
        self._utm_transform = params['utm_transform'].value
        self._utm_offset_lat = params['utm_offset.lat'].value
        self._utm_offset_lon = params['utm_offset.lon'].value

    @property
    def rate(self):
        return self._rate

    @property
    def frame_id(self):
        return self._frame_id

    @property 
    def utm_transform(self):
        return self._utm_transform
    
    @property 
    def utm_offset_lat(self):
        return self._utm_offset_lat
    
    @property 
    def utm_offset_lon(self):
        return self._utm_offset_lon


class QueryResultParser(ABC):
    TYPE = "AbstractParser"

    @abstractmethod
    def declare_params(self, defaults: QueryResultDefaultParameters) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return []

    @abstractmethod
    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return []

    @abstractmethod
    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        return []

    def __repr__(self) -> str:
        return self.TYPE


class UTMTransformer:
    """
    transform from lat/lon to utm and optionally apply offset
    """

    def __init__(self, offset_lat=None, offset_lon=None) -> None:
        self.utm_offset_lat = offset_lat
        self.utm_offset_lon = offset_lon
        self.utm_offset = True if offset_lat and offset_lon else False

        from pyproj import Transformer
        self.transformer = Transformer.from_crs(
            {"proj": 'latlong', "ellps": 'WGS84', "datum": 'WGS84'},
            # {"proj":'geocent', "ellps":'WGS84', "datum":'WGS84'},
            {"proj": 'utm', "ellps": 'WGS84', "datum": 'WGS84', "zone": 33},
        )

        self.utm_offset_x, self.utm_offset_y = self.transformer.transform(
            self.utm_offset_lat, self.utm_offset_lon, radians=False)

    def transform(self, point):
        point_utm = self.transformer.transform(point.x, point.y, radians=False)
        point.x = point_utm[0]
        point.y = point_utm[1]
        if self.utm_offset:
            point.x -= self.utm_offset_x
            point.y -= self.utm_offset_y

        return point


class StampedTopicParser(QueryResultParser):
    """"Base class for parsers which produce a single stamped message topic"""
    TYPE = None

    def __init__(self) -> None:
        super().__init__()

    def declare_params(self, defaults: QueryResultDefaultParameters) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return [
            ('frame_id', defaults.frame_id, ParameterDescriptor()),
            ('topic', Parameter.Type.STRING, ParameterDescriptor()),
            ('utm_transform', defaults.utm_transform, ParameterDescriptor()),
            ('utm_offset.lat', defaults.utm_offset_lat, ParameterDescriptor()),
            ('utm_offset.lon',  defaults.utm_offset_lon, ParameterDescriptor()),
        ]
    
    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:

        self.frame_id = params['frame_id'].value
        self.topic = params['topic'].value
        self.utm_transform = params['utm_transform'].value 
        self.utm_offset_lat = params['utm_offset.lat'].value
        self.utm_offset_lon = params['utm_offset.lon'].value
        if self.utm_transform:
            self.utm_transformer = UTMTransformer(
                params['utm_offset.lat'].value, params['utm_offset.lon'].value)

        return []

    def get_frame_id_from_config(self) -> str:
        return self.frame_id if self.frame_id else 'map'

    def get_frame_id_from_sql(elem: Row) -> str:
        return elem.frame_id if hasattr(elem, 'frame_id') else 'map'

    def get_frame_id(self, elem: Row) -> str:
        return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'


class SingleElementParser(StampedTopicParser):
    TYPE = None

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return None

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        return (self.parse_single_element(element, time) for element in result)


class PointResultParser(SingleElementParser):
    TYPE = "PointStamped"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, PointStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        msg = PointStamped(header=Header(frame_id=self.get_frame_id(element), stamp=time),
                           point=PostGisConverter.to_point(element.geometry))
        if self.utm_transform:
            msg.point = self.utm_transformer.transform(msg.point)

        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PoseResultParser(SingleElementParser):
    TYPE = "Pose"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, Pose)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return (self.topic, PostGisConverter.to_pose(element.geometry, element.rotation))

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class PoseStampedResultParser(SingleElementParser):
    TYPE = "PoseStamped"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, PoseStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return (self.topic, PostGisConverter.to_pose_stamped(geometry=element.geometry,
                                                             orientation=element.rotation,
                                                             header=Header(frame_id=self.get_frame_id(element), stamp=time)))

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PC2ResultParser(StampedTopicParser):
    TYPE = "PointCloud2"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic,  PointCloud2)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        pointcloud_msg = PointCloud2()
        header = Header(frame_id=self.get_frame_id_from_config(), stamp=time)
        points = [
            PostGisConverter.to_point_tuple(element.geometry) for element in result.all()
        ]
        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)
        return [(self.topic, pointcloud_msg)]

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PolygonResultParser(SingleElementParser):
    TYPE = "Polygon"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, Polygon)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return (self.topic, PostGisConverter.to_polygon(element.geometry))

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class PolygonStampedResultParser(SingleElementParser):
    TYPE = "PolygonStamped"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, PolygonStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return (self.topic, PostGisConverter.to_polygon_stamped(geometry=element.geometry,
                                                                header=Header(frame_id=self.get_frame_id(element), stamp=time)))

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class MarkerResultParser(SingleElementParser):
    TYPE = "Marker"

    def __init__(self) -> None:
        super().__init__()

    def declare_params(self, defaults: QueryResultDefaultParameters) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return super().declare_params(defaults) + [
            ('marker_type', Parameter.Type.STRING, ParameterDescriptor()),
            ('marker_ns', Parameter.Type.STRING, ParameterDescriptor()),
            ('marker_color', Parameter.Type.DOUBLE_ARRAY, ParameterDescriptor()),
            ('marker_scale', Parameter.Type.DOUBLE_ARRAY, ParameterDescriptor()),
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        topics = super().set_params(params)
        self.marker_type = params['marker_type'].value
        self.marker_ns = params['marker_ns'].value if params['marker_ns'].value else ''
        self.marker_color = params['marker_color'].value if params['marker_color'].value else [
            1.0, 0.0, 0.0, 1.0]
        self.marker_scale = params['marker_scale'].value if params['marker_scale'].value else [
            0.1, 0.1, 0.1]
        return topics + [(self.topic, Marker)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:

        def get_id(elem):
            return elem.id if hasattr(elem, 'id') else 0

        marker_color = ColorRGBA(r=self.marker_color[0], g=self.marker_color[1], b=self.marker_color[2], a=self.marker_color[3])
        marker_types = {
            "visualization_msgs::Marker::ARROW": Marker.ARROW,
            "visualization_msgs::Marker::CUBE": Marker.CUBE,
            "visualization_msgs::Marker::SPHERE": Marker.SPHERE,
            "visualization_msgs::Marker::CYLINDER": Marker.CYLINDER,
        }
        marker_type = marker_types.get(self.marker_type, Marker.ARROW)
        marker_scale = Vector3(x=self.marker_scale[0], y=self.marker_scale[1], z=self.marker_scale[2])

        msg = PostGisConverter.to_marker(header=Header(frame_id=self.get_frame_id(element), stamp=time),
                                           geometry=element.geometry,
                                           orientation=None,
                                           action=Marker.MODIFY,
                                           id=get_id(element),
                                           ns=self.marker_ns,
                                           type=marker_type,
                                           scale=marker_scale,
                                           color=marker_color,
                                           lifetime=Duration(sec=3))
        if self.utm_transform:
                msg.points = [self.utm_transformer.transform(
                    point) for point in msg.points]
                # todo handle orientation
                if msg.type not in [Marker.LINE_STRIP, Marker.LINE_LIST, Marker.POINTS]:
                    msg.pose.position = self.utm_transformer.transform(msg.pose.position)
                
        return (self.topic,                msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class BasicStampedArrayParserFactory:
    @staticmethod
    def create_array_parser(cls: SingleElementParser, msg: Any, field: str):
        class ArrayParserMessage(cls):
            def __init__(self) -> None:
                super().__init__()
                self._has_header = hasattr(msg, 'header')  # and msg.get_fields_and_types()[
                # 'header'] == 'std_msgs/Header'

            def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
                super().set_params(params)
                return [(self.topic, msg)]

            def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
                # TODO: toplevel header (frame_id, timestamp can be different from internal elements)
                #       Add addition toplevel param and if not defined use header of first element
                args = dict()

                if self._has_header:
                    args['header'] = Header(
                        frame_id=self.get_frame_id(None), stamp=time)

                args.update({field: [self.parse_single_element(
                    element, time)[1] for element in result]})

                m = msg(**args)
                return [(self.topic, m)]

            def __repr__(self) -> str:
                return f"{super().TYPE}Array (using frame_id: {self.frame_id} and topic: {self.topic})"

        return ArrayParserMessage
