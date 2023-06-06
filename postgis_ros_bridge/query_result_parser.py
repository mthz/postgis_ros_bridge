from sqlalchemy import Result

from builtin_interfaces.msg import Time, Duration
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.parameter import Parameter
from std_msgs.msg import Header, ColorRGBA
from geometry_msgs.msg import PointStamped, Vector3
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
from visualization_msgs.msg import Marker
from abc import ABC, abstractmethod
from typing import Dict, Any, Iterable, Tuple

from postgis_converter import PostGisConverter


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


class TopicStampedParser(QueryResultParser):
    TYPE = None

    def __init__(self) -> None:
        super().__init__()


    def declare_params(self) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return [
            ('frame_id', Parameter.Type.STRING, ParameterDescriptor()),
            ('topic', Parameter.Type.STRING, ParameterDescriptor())
        ]


    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        self.frame_id = params['frame_id'].value
        self.topic = params['topic'].value
        return []


class PointResultParser(TopicStampedParser):
    TYPE = "Point3D"


    def __init__(self) -> None:
        super().__init__()


    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, PointStamped)]


    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        def get_frame_id(elem):
            return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'
        return ((self.topic,
                 PointStamped(header=Header(frame_id=get_frame_id(element), stamp=time),
                              point=PostGisConverter.to_point(element.geometry))) for element in result)


    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PC2ResultParser(TopicStampedParser):
    TYPE = "PointCloud2"

    def __init__(self) -> None:
        super().__init__()


    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic,  PointCloud2)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        def get_frame_id():
            return self.frame_id if self.frame_id else 'map'
        pointcloud_msg = PointCloud2()
        header = Header(frame_id=get_frame_id(), stamp=time)
        points = [
            PostGisConverter.to_point_tuple(element.geometry) for element in result.all()
        ]
        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)
        return [(self.topic, pointcloud_msg)]

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class MarkerResultParser(TopicStampedParser):
    TYPE = "Marker"

    def __init__(self) -> None:
        super().__init__()

    def declare_params(self) -> Iterable[Tuple[str, Any, ParameterDescriptor]]:
        return super().declare_params() + [
            ('marker_type', Parameter.Type.STRING, ParameterDescriptor()),
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        topics = super().set_params(params)
        self.marker_type = params['marker_type'].value
        return topics + [(self.topic, Marker)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        def get_frame_id(elem):
            return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'

        return ((self.topic,
                PostGisConverter.to_marker(header=Header(frame_id=get_frame_id(element), stamp=time),
                                           geometry=element.geometry,
                                           orientation=element.rotation,
                                           action=Marker.MODIFY, id=id, 
                                           type=Marker.ARROW, 
                                           scale=Vector3(x=0.1, y=0.1, z=0.1),
                                           color=ColorRGBA(r=1.0, g=0.0, b=0.0, a=1.0),
                                           lifetime=Duration(sec=3))) for id, element in enumerate(result))
