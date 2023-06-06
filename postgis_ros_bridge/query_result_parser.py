from sqlalchemy import Result

from builtin_interfaces.msg import Time
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.parameter import Parameter
from std_msgs.msg import Header
from geometry_msgs.msg import PointStamped
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
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
                              point=PostGisConverter.to_point(element.geometry))) for element in result.all())

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


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
            PostGisConverter.to_point_tuple(element.geometry) for element in result.all()
        ]
        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)
        return [(self.topic, pointcloud_msg)]

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"
