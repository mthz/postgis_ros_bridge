from typing import Union

from std_msgs.msg import Header
from geometry_msgs.msg import Point, Pose, PoseStamped, Quaternion, Polygon, PolygonStamped, Point32
from scipy.spatial.transform import Rotation
from shapely import wkb
from visualization_msgs.msg import Marker


class PostGisConverter:

    @staticmethod
    def to_point(geometry: Union[bytes, str], hex=True) -> Point:
        if not geometry:
            return Point(x=0.0, y=0.0, z=0.0)
        point = wkb.loads(geometry, hex=hex)
        return Point(x=point.x, y=point.y, z=point.z if point.has_z else 0.0)

    @staticmethod
    def to_point_tuple(geometry: Union[bytes, str], hex=True):
        point = wkb.loads(geometry, hex=hex)
        return (point.x, point.y, point.z) if point.has_z else (point.x, point.y)

    @staticmethod
    def to_marker(header: Header, geometry: Union[bytes, str], orientation: Union[bytes, str], hex=True, *args, **kwargs) -> Marker:
        return Marker(header=header, pose=PostGisConverter.to_pose(geometry=geometry, orientation=orientation, hex=hex), *args, **kwargs)

    @staticmethod
    def to_marker_polygon(header: Header, geometry: Union[bytes, str], hex=True, *args, **kwargs):
        geometry = wkb.loads(geometry, hex=hex)
        points = []
        if geometry.geom_type == "LineString":
            points = [Point(x=point[0], y=point[1], z=point[2] if len(
                point) > 2 else 0.0) for point in geometry.coords]
        elif geometry.geom_type == "Polygon":
            # TODO untested
            points = [Point(x=point[0], y=point[1], z=point[2] if len(
                point) > 2 else 0.0) for point in geometry.exterior.coords]
        elif geometry.geom_type == "MultiPolygon":
            # TODO: output only first polygon of multipolygon
            geom = geometry.geoms[0]
            points = [Point(x=point[0], y=point[1], z=point[2] if len(
                point) > 2 else 0.0) for point in geom.exterior.coords]
        else:
            raise ValueError(
                f"Unsupported geometry type: {geometry.geom_type}")
        return Marker(header=header, points=points, *args, **kwargs)

    @staticmethod
    def to_orientation(orientation: Union[bytes, str], hex=True) -> Quaternion:
        # TODO: check quaternion order match with ROS
        if not orientation:
            return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        rot_vec = wkb.loads(orientation, hex=hex)
        q = Rotation.from_rotvec([rot_vec.x, rot_vec.y, rot_vec.z]).as_quat()
        return Quaternion(x=q[0], y=q[1], z=q[2], w=q[3])

    @staticmethod
    def to_pose(geometry: Union[bytes, str], orientation: Union[bytes, str], hex=True) -> Pose:
        position = PostGisConverter.to_point(geometry)
        orientation = PostGisConverter.to_orientation(orientation)
        return Pose(position=position, orientation=orientation)

    @staticmethod
    def to_pose_stamped(header: Header, geometry: Union[bytes, str], orientation: Union[bytes, str], hex=True) -> PoseStamped:
        pose = PostGisConverter.to_pose(geometry=geometry, orientation=orientation, hex=hex)
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
