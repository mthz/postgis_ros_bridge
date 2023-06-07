from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Tuple

from builtin_interfaces.msg import Duration, Time
from geometry_msgs.msg import PointStamped, Vector3
from postgis_converter import PostGisConverter
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.parameter import Parameter
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
from sqlalchemy import Result, Row
from std_msgs.msg import ColorRGBA, Header
from visualization_msgs.msg import Marker


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
    """"Base class for parsers which produce a single stamped message topic"""
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


class SingleElementParser(TopicStampedParser):
    TYPE = None

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        return None

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        return (self.parse_single_element(element, time) for element in result)


class PointResultParser(SingleElementParser):
    TYPE = "Point3D"

    def __init__(self) -> None:
        super().__init__()

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        return super().set_params(params) + [(self.topic, PointStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        def get_frame_id(elem):
            return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'

        return (self.topic,
                PointStamped(header=Header(frame_id=get_frame_id(element), stamp=time),
                             point=PostGisConverter.to_point(element.geometry)))

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


class MarkerResultParser(SingleElementParser):
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

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        def get_frame_id(elem):
            return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'

        def get_id(elem):
            return elem.id if hasattr(elem, 'id') else 0

        return (self.topic,
                PostGisConverter.to_marker(header=Header(frame_id=get_frame_id(element), stamp=time),
                                           geometry=element.geometry,
                                           orientation=element.rotation,
                                           action=Marker.MODIFY,
                                           id=get_id(element),
                                           type=Marker.ARROW,
                                           scale=Vector3(x=0.1, y=0.1, z=0.1),
                                           color=ColorRGBA(
                                               r=1.0, g=0.0, b=0.0, a=1.0),
                                           lifetime=Duration(sec=3)))
    
    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class BasicArrayStampedParserFactory:
    @staticmethod
    def create_array_parser(cls: SingleElementParser, msg: Any, field: str):
        class ArrayParserMessage(cls):
            def __init__(self) -> None:
                super().__init__()
                self._has_header = hasattr(msg, 'header') and msg.get_fields_and_types()[
                    'header'] == 'std_msgs/Header'

            def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
                super().set_params(params)
                return [(self.topic, msg)]

            def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
                def get_frame_id(elem):
                    return self.frame_id if self.frame_id else elem.frame_id if hasattr(elem, 'frame_id') else 'map'

                # TODO: toplevel header (frame_id, timestamp can be different from internal elements)
                #       Add addition toplevel param and if not defined use header of first element
                args = dict()

                if self._has_header:
                    args['header'] = Header(
                        frame_id=get_frame_id(None), stamp=time)

                args.update({field: [self.parse_single_element(
                    element, time)[1] for element in result]})

                m = msg(**args)
                return [(self.topic, m)]
            
            def __repr__(self) -> str:
                return f"{super().TYPE}[] (using frame_id: {self.frame_id} and topic: {self.topic})"
            
        return ArrayParserMessage
