from typing import Union

from std_msgs.msg import Header
from geometry_msgs.msg import Point, Pose, PoseStamped, Quaternion, Polygon, PolygonStamped, Point32
from scipy.spatial.transform import Rotation
from shapely import wkb
import shapely.geometry
from visualization_msgs.msg import Marker


class PostGisConverter:

    @staticmethod
    def load(geometry: Union[bytes, str], hex=True):
        return wkb.loads(geometry, hex=hex)

    @staticmethod
    def to_point(geometry: Union[bytes, str], hex=True) -> Point:
        if not geometry:
            return Point(x=0.0, y=0.0, z=0.0)
        point = wkb.loads(geometry, hex=hex)
        return PostGisConverter.to_point_(point)

    @staticmethod 
    def to_point_(point: shapely.geometry.Point) -> Point:
        return Point(x=point.x, y=point.y, z=point.z if point.has_z else 0.0)

    @staticmethod
    def to_point_tuple(geometry: Union[bytes, str], hex=True):
        point = wkb.loads(geometry, hex=hex)
        return (point.x, point.y, point.z) if point.has_z else (point.x, point.y)

    @staticmethod
    def to_orientation(orientation: Union[bytes, str], hex=True) -> Quaternion:
        # TODO: check quaternion order match with ROS
        if not orientation:
            return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        orientation = wkb.loads(orientation, hex=hex)
        return PostGisConverter.to_orientation_(orientation)

    @staticmethod
    def to_orientation_(orientation: shapely.geometry.Point, hex=True) -> Quaternion:
        if not orientation:
            return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        q = Rotation.from_rotvec([orientation.x, orientation.y, orientation.z]).as_quat()
        return Quaternion(x=q[0], y=q[1], z=q[2], w=q[3])
    

    @staticmethod
    def to_points_from_line_sring(geometry: shapely.geometry.LineString) -> list[Point]:
        return [Point(x=point[0], y=point[1], z=point[2] if geometry.has_z else 0.0) for point in geometry.coords]

    @staticmethod
    def to_points_from_polygon(geometry: shapely.geometry.LineString) -> list[Point]:
        return [Point(x=point[0], y=point[1], z=point[2] if geometry.has_z else 0.0) for point in geometry.coords]


    @staticmethod
    def to_marker(header:Header, geometry: Union[bytes, str], orientation: Union[bytes, str]=None, hex=True, *args, **kwargs):
        geometry = PostGisConverter.load(geometry, hex=hex)
        geometry_orientation = PostGisConverter.load(orientation, hex=hex) if orientation else None

        marker = Marker(header=header, *args, **kwargs)

        if geometry.geom_type == "Point":
            marker.pose=PostGisConverter.to_pose_(geometry, geometry_orientation)
        elif geometry.geom_type == "LineString":
            marker.points = PostGisConverter.to_points_from_line_sring(geometry)
            marker.type = Marker.LINE_STRIP
        elif geometry.geom_type == "Polygon":
            # TODO untested 
            marker.points = [Point(x=point[0], y=point[1], z=point[2] if geometry.has_z else 0.0) for point in geometry.exterior.coords]
            marker.type = Marker.LINE_STRIP
        elif geometry.geom_type == "MultiPolygon":
            # TODO: output only first polygon of multipolygon
            geom = geometry.geoms[0]
            marker.points = [Point(x=point[0], y=point[1], z=point[2] if geometry.has_z else 0.0) for point in geom.exterior.coords]
            marker.type = Marker.LINE_STRIP
        else:
            raise ValueError(
                f"Unsupported geometry type: {geometry.geom_type}")
        return marker

    @staticmethod
    def to_pose(geometry: Union[bytes, str], orientation: Union[bytes, str], hex=True) -> Pose:
        geometry_position = PostGisConverter.load(geometry)
        geometry_orientation = PostGisConverter.load(orientation)
        return PostGisConverter.to_pose_(geometry_position, geometry_orientation)

    @staticmethod
    def to_pose_(point:shapely.geometry.Point, orientation:shapely.geometry.Point, hex=True) -> Pose:
        return Pose(position=PostGisConverter.to_point_(point), orientation=PostGisConverter.to_orientation_(orientation))
    
    
    @staticmethod
    def to_pose_stamped(header: Header, geometry: Union[bytes, str], orientation: Union[bytes, str], hex=True) -> PoseStamped:
        pose = PostGisConverter.to_pose(geometry, orientation, hex=hex)
        return PoseStamped(header=header, pose=pose)

    @staticmethod
    def to_polygon(geometry: Union[bytes, str], hex=True) -> Polygon:
        polygon = wkb.loads(geometry, hex=hex)
        return Polygon(
            points=[Point32(x=x, y=y, z=z) for x, y, z in polygon.boundary.coords]) if polygon.has_z else Polygon(points=[Point32(x=x, y=y, z=0.0) for x, y in polygon.boundary.coords])

    @staticmethod
    def to_polygon_stamped(header: Header, geometry: Union[bytes, str], hex=True) -> PolygonStamped:
        polygon = PostGisConverter.to_polygon(geometry, hex=hex)
        return PolygonStamped(header=header, polygon=polygon)
