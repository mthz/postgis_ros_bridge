"""Parser classes for converting query results to ROS messages."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, SupportsFloat, Tuple

from builtin_interfaces.msg import Duration, Time
from geometry_msgs.msg import (Point, PointStamped, Polygon, PolygonStamped,
                               Pose, PoseStamped, Vector3, Point32)
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.parameter import Parameter
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
from sqlalchemy import Result, Row
from std_msgs.msg import ColorRGBA, Header
from visualization_msgs.msg import Marker
from postgis_ros_bridge.postgis_converter import PostGisConverter

from pyproj import Transformer
from pyproj.aoi import AreaOfInterest
from pyproj.crs import CRS
from pyproj.database import query_utm_crs_info


class QueryResultDefaultParameters:
    """Default parameters for query result parsers."""

    def __init__(self):
        self._rate = None
        self._frame_id = None
        self._utm_transform = None
        self._utm_zone = None
        self._utm_band = None
        self._utm_lat = None
        self._utm_lon = None
        self._utm_lonlat_origin = None

    def declare_params(self) -> Iterable[Tuple[str, Any]]:
        """Declare default parameters for query result parsers."""
        return [
            ('rate', 1.0, ParameterDescriptor()),
            ('frame_id', 'map', ParameterDescriptor()),
            ('utm_transform', False, ParameterDescriptor()),
            ('utm.zone', -1, ParameterDescriptor()),
            ('utm.band', "", ParameterDescriptor()),
            ('utm.lat', Parameter.Type.DOUBLE, ParameterDescriptor()),
            ('utm.lon', Parameter.Type.DOUBLE, ParameterDescriptor()),
            ('utm.lonlat_origin', False, ParameterDescriptor())
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Set default parameters for query result parsers."""
        self._rate = params['rate'].value
        self._frame_id = params['frame_id'].value
        self._utm_transform = params['utm_transform'].value
        self._utm_zone = params['utm.zone'].value
        self._utm_band = params['utm.band'].value
        self._utm_lat = params['utm.lat'].value
        self._utm_lon = params['utm.lon'].value
        self._utm_lonlat_origin = params['utm.lonlat_origin'].value

    @property
    def rate(self):
        """Get rate of query result parser."""
        return self._rate

    @property
    def frame_id(self):
        """Get frame id of query result parser."""
        return self._frame_id

    @property
    def utm_transform(self):
        """Get utm transform enabled / disabled of query result parser."""
        return self._utm_transform

    @property
    def utm_lat(self):
        """Get utm offset latitude of query result parser."""
        return self._utm_lat

    @property
    def utm_lon(self):
        """Get utm offset longitude of query result parser."""
        return self._utm_lon

    @property
    def utm_zone(self):
        """Get utm zone of query result parser."""
        return self._utm_zone

    @property
    def utm_band(self):
        """Get utm band of query result parser."""
        return self._utm_band

    @property
    def utm_lonlat_origin(self):
        """Get utm offset longitude of query result parser."""
        return self._utm_lonlat_origin


class QueryResultParser(ABC):
    """Abstract Query Result Parser."""

    TYPE = "AbstractParser"

    @abstractmethod
    def declare_params(self,
                       defaults: QueryResultDefaultParameters) -> Iterable[
                           Tuple[str, Any, ParameterDescriptor]]:
        """Define API of declare_params."""
        return []

    @abstractmethod
    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Define API of set_params."""
        return []

    @abstractmethod
    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        """Define API of parse_result."""
        return []

    def __repr__(self) -> str:
        return self.TYPE


class GeodesicTransform:
    """Helper class to transform geodesic coordiantes into local cartesian frame."""

    def __init__(self, crs_to) -> None:
        """Initialize GeodesicTransform."""
        self.map_origin_easting = None
        self.map_origin_northing = None
        self.map_transform = False

        crs_from = {"proj": 'latlong', "ellps": 'WGS84', "datum": 'WGS84'}
        self.transformer = Transformer.from_crs(
            crs_from,
            crs_to
        )

    @staticmethod
    def to_utm(zone: int, band: str) -> "GeodesicTransform":
        """Construct transfromer to UTM from fixed zone and band."""
        return GeodesicTransform({"proj": 'utm', "ellps": 'WGS84', "datum": 'WGS84', "zone": zone,
                                  "band": band})

    @staticmethod
    def to_geocent() -> "GeodesicTransform":
        """Construct transfromer to geocentric model."""
        return GeodesicTransform({"proj": 'geocent', "ellps": 'WGS84', "datum": 'WGS84'})

    @staticmethod
    def to_utm_lonlat(lon: float, lat: float) -> "GeodesicTransform":
        """Construct transfromer to UTM for given lat/lon."""
        utm_crs_list = query_utm_crs_info(
            datum_name="WGS84",
            area_of_interest=AreaOfInterest(
                west_lon_degree=lon,
                south_lat_degree=lat,
                east_lon_degree=lon,
                north_lat_degree=lat,
                ),
        )
        utm_crs = CRS.from_epsg(utm_crs_list[0].code)
        return GeodesicTransform(utm_crs)

    def set_map_origin(self, lon: float, lat: float) -> None:
        """Set map origin for local cartesian frame."""
        # pylint: disable=unpacking-non-sequence
        easing, northing = self.transformer.transform(lon, lat)
        self.map_origin_easting = easing
        self.map_origin_northing = northing
        self.map_transform = True

    def transform_lonlat(self, lon: float, lat: float):
        """Transform a point from lat/lon to local map and optionally apply offset."""
        # pylint: disable=unpacking-non-sequence
        easting, northing = self.transformer.transform(lon, lat)
        if self.map_transform:
            easting -= self.map_origin_easting
            northing -= self.map_origin_northing
        return easting, northing

    def transform_point(self, point: Point) -> Point:
        """Transform a geodetic point to local cartesian frame."""
        easting, northing = self.transform_lonlat(lon=point.x, lat=point.y)

        return Point(x=easting, y=northing, z=point.z)

    def transform_point32(self, point: Point32) -> Point32:
        """Transform a geodetic point to local cartesian frame."""
        easting, northing = self.transform_lonlat(lon=point.x, lat=point.y)

        return Point32(x=easting, y=northing, z=point.z)

    def transform(
            self, point: Tuple[SupportsFloat,
                               SupportsFloat,
                               SupportsFloat]) -> Tuple[SupportsFloat,
                                                        SupportsFloat,
                                                        SupportsFloat]:
        """Transform a geodetic point to local cartesian frame."""
        # TODO: handle z # pylint: disable=fixme

        easting, northing = self.transform_lonlat(lon=point[0], lat=point[1])

        return (easting, northing, point[2])


class StampedTopicParser(QueryResultParser):
    """Base class for parsers which produce a single stamped message topic."""

    TYPE = None

    def __init__(self) -> None:
        super().__init__()
        self.frame_id = None
        self.topic = None
        self.utm_transform = None
        self.utm_zone = None
        self.utm_band = None
        self.utm_lat = None
        self.utm_lon = None
        self.utm_lonlat_origin = None
        self.utm_transformer = None

    def declare_params(
            self,
            defaults: QueryResultDefaultParameters) -> Iterable[Tuple[str,
                                                                      Any,
                                                                      ParameterDescriptor]]:
        """Implement API of declare_params."""
        return [
            ('frame_id', defaults.frame_id, ParameterDescriptor()),
            ('topic', Parameter.Type.STRING, ParameterDescriptor()),
            ('utm_transform', defaults.utm_transform, ParameterDescriptor()),
            ('utm_zone', defaults.utm_zone, ParameterDescriptor()),
            ('utm_band', defaults.utm_band, ParameterDescriptor()),
            ('utm.lat', defaults.utm_lat, ParameterDescriptor()),
            ('utm.lon',  defaults.utm_lon, ParameterDescriptor()),
            ('utm.lonlat_origin',  defaults.utm_lonlat_origin, ParameterDescriptor()),
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params."""
        self.frame_id = params['frame_id'].value
        self.topic = params['topic'].value
        self.utm_transform = params['utm_transform'].value
        self.utm_zone = params['utm_zone'].value
        self.utm_band = params['utm_band'].value
        self.utm_lat = params['utm.lat'].value
        self.utm_lon = params['utm.lon'].value
        self.utm_lonlat_origin = params['utm.lonlat_origin'].value

        if self.utm_transform:
            if self.utm_zone >= 0 and self.utm_band != "":
                self.utm_transformer = GeodesicTransform.to_utm(self.utm_zone, self.utm_band)
            else:
                if self.utm_lat is None or self.utm_lon is None:
                    raise ValueError("need lat/lon or zone/band to setup utm transform")
                self.utm_transformer = GeodesicTransform.to_utm_lonlat(self.utm_lon, self.utm_lat)
            if self.utm_lonlat_origin:
                if self.utm_lat is None or self.utm_lon is None:
                    raise ValueError("utm.lat and utm.lon must be set if utm.lonlat_origin is set")
                self.utm_transformer.set_map_origin(self.utm_lon, self.utm_lat)
        return []

    def get_frame_id(self, elem: Row) -> str:
        """Get frame id of from query."""
        return elem.frame_id if hasattr(elem, 'frame_id') else self.frame_id


class SingleElementParser(StampedTopicParser):
    """Base class for parsers which produce a single stamped message topic."""

    TYPE = None

    @abstractmethod
    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element."""
        return None

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        """Implement API of parse_result for single element parsers."""
        return (self.parse_single_element(element, time) for element in result)


class PointResultParser(SingleElementParser):
    """Parser for point results."""

    TYPE = "PointStamped"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for point results."""
        return super().set_params(params) + [(self.topic, PointStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for point results."""
        msg = PointStamped(header=Header(frame_id=self.get_frame_id(element), stamp=time),
                           point=PostGisConverter.to_point(element.geometry))
        if self.utm_transform:
            msg.point = self.utm_transformer.transform_point(msg.point)

        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PoseResultParser(SingleElementParser):
    """Parser for pose results."""

    TYPE = "Pose"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for pose results."""
        return super().set_params(params) + [(self.topic, Pose)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for pose results."""
        msg = PostGisConverter.to_pose(element.geometry, element.rotation)
        if self.utm_transform:
            msg.position = self.utm_transformer.transform_point(msg.position)
        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class PoseStampedResultParser(SingleElementParser):
    """Parser for pose stamped results."""

    TYPE = "PoseStamped"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for pose stamped results."""
        return super().set_params(params) + [(self.topic, PoseStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for pose stamped results."""
        msg = PostGisConverter.to_pose_stamped(geometry=element.geometry,
                                               orientation=element.rotation,
                                               header=Header(frame_id=self.get_frame_id(element),
                                                             stamp=time))
        if self.utm_transform:
            # TODO: handle orientation # pylint: disable=fixme
            msg.pose.position = self.utm_transformer.transform_point(
                msg.pose.position)
        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PC2ResultParser(StampedTopicParser):
    """Parser for pointcloud2 results."""

    TYPE = "PointCloud2"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for pointcloud2 results."""
        return super().set_params(params) + [(self.topic,  PointCloud2)]

    def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
        """Implement API of parse_result for pointcloud2 results."""
        pointcloud_msg = PointCloud2()
        header = Header(frame_id=self.frame_id, stamp=time)
        points = [
            PostGisConverter.to_point_xyz(element.geometry) for element in result.all()
        ]

        if self.utm_transform:
            points = [self.utm_transformer.transform(
                point) for point in points]

        pointcloud_msg = point_cloud2.create_cloud_xyz32(header, points)
        return [(self.topic, pointcloud_msg)]

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class PolygonResultParser(SingleElementParser):
    """Parser for polygon results."""

    TYPE = "Polygon"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for polygon results."""
        return super().set_params(params) + [(self.topic, Polygon)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for polygon results."""
        msg = PostGisConverter.to_polygon(element.geometry)

        if self.utm_transform:
            msg.points = [self.utm_transformer.transform_point32(
                point) for point in msg.points]

        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class PolygonStampedResultParser(SingleElementParser):
    """Parser for polygon stamped results."""

    TYPE = "PolygonStamped"

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for polygon stamped results."""
        return super().set_params(params) + [(self.topic, PolygonStamped)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for polygon stamped results."""
        msg = PostGisConverter.to_polygon_stamped(
            geometry=element.geometry,
            header=Header(frame_id=self.get_frame_id(element),
                          stamp=time))

        if self.utm_transform:
            msg.polygon.points = [self.utm_transformer.transform_point32(
                point) for point in msg.polygon.points]

        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using topic: {self.topic})"


class MarkerResultParser(SingleElementParser):
    """Parser for marker results."""

    TYPE = "Marker"

    def __init__(self):
        super().__init__()
        self.marker_type = None
        self.marker_ns = None
        self.marker_color = None
        self.marker_scale = None

    def declare_params(
            self,
            defaults: QueryResultDefaultParameters) -> Iterable[Tuple[str, Any,
                                                                      ParameterDescriptor]]:
        """Implement API of declare_params for marker results."""
        return super().declare_params(defaults) + [
            ('marker_type', Parameter.Type.STRING, ParameterDescriptor()),
            ('marker_ns', Parameter.Type.STRING, ParameterDescriptor()),
            ('marker_color', Parameter.Type.DOUBLE_ARRAY, ParameterDescriptor()),
            ('marker_scale', Parameter.Type.DOUBLE_ARRAY, ParameterDescriptor()),
        ]

    def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
        """Implement API of set_params for marker results."""
        topics = super().set_params(params)
        self.marker_type = params['marker_type'].value
        self.marker_ns = params['marker_ns'].value if params['marker_ns'].value else ''
        self.marker_color = params['marker_color'].value if params['marker_color'].value else [
            1.0, 0.0, 0.0, 1.0]
        self.marker_scale = params['marker_scale'].value if params['marker_scale'].value else [
            0.1, 0.1, 0.1]
        return topics + [(self.topic, Marker)]

    def parse_single_element(self, element: Row, time: Time) -> Tuple[str, Any]:
        """Implement API of parse_single_element for marker results."""
        def get_id(elem):
            """Get id of element if it has one, otherwise return 0."""
            return elem.id if hasattr(elem, 'id') else 0

        marker_color = ColorRGBA(r=self.marker_color[0],
                                 g=self.marker_color[1],
                                 b=self.marker_color[2],
                                 a=self.marker_color[3])
        marker_types = {
            "visualization_msgs::Marker::ARROW": Marker.ARROW,
            "visualization_msgs::Marker::CUBE": Marker.CUBE,
            "visualization_msgs::Marker::SPHERE": Marker.SPHERE,
            "visualization_msgs::Marker::CYLINDER": Marker.CYLINDER,
        }
        marker_type = marker_types.get(self.marker_type, Marker.ARROW)
        marker_scale = Vector3(
            x=self.marker_scale[0], y=self.marker_scale[1], z=self.marker_scale[2])

        msg = PostGisConverter.to_marker(header=Header(frame_id=self.get_frame_id(element),
                                                       stamp=time),
                                         as_hex=True,
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
            msg.points = [self.utm_transformer.transform_point(
                point) for point in msg.points]
            # todo handle orientation
            if msg.type not in [Marker.LINE_STRIP, Marker.LINE_LIST, Marker.POINTS]:
                msg.pose.position = self.utm_transformer.transform_point(
                    msg.pose.position)

        return (self.topic, msg)

    def __repr__(self) -> str:
        return super().__repr__() + f" (using frame_id: {self.frame_id} and topic: {self.topic})"


class BasicStampedArrayParserFactory:
    """Factory for basic stamped array parsers."""

    @staticmethod
    def create_array_parser(cls: SingleElementParser, msg: Any, field: str):
        """Create array parser for given message and field."""
        class ArrayParserMessage(cls):
            """Array parser for given message and field."""

            def __init__(self) -> None:
                super().__init__()
                self._has_header = hasattr(msg, 'header')

            def set_params(self, params: Dict[str, Parameter]) -> Iterable[Tuple[str, Any]]:
                """Implement API of set_params for array parser."""
                super().set_params(params)
                return [(self.topic, msg)]

            def parse_result(self, result: Result, time: Time) -> Iterable[Tuple[str, Any]]:
                """Implement API of parse_result for array parser."""
                # TODO: toplevel header (frame_id, timestamp can be different
                #       from internal elements).
                #       Add addition toplevel param and if not defined use header of first element.
                args = {}

                if self._has_header:
                    args['header'] = Header(
                        frame_id=self.get_frame_id(None), stamp=time)

                args.update({field: [self.parse_single_element(
                    element, time)[1] for element in result]})

                message = msg(**args)
                return [(self.topic, message)]

            def __repr__(self) -> str:
                return f"{super().TYPE}Array "\
                    f"(using frame_id: {self.frame_id} and topic: {self.topic})"

        return ArrayParserMessage
