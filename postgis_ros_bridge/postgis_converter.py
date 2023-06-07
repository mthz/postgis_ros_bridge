from typing import Union

from geometry_msgs.msg import Point, Pose, PoseStamped, Quaternion
from scipy.spatial.transform import Rotation
from shapely import wkb
from visualization_msgs.msg import Marker


class PostGisConverter:

    @staticmethod
    def to_point(geometry, hex=True) -> Point:
        if not geometry:
            return Point(x=0.0, y=0.0, z=0.0)
        point = wkb.loads(geometry, hex=hex)
        return Point(x=point.x, y=point.y, z=point.z)

    @staticmethod
    def to_point_tuple(geometry, hex=True):
        point = wkb.loads(geometry, hex=hex)
        return (point.x, point.y, point.z)

    @staticmethod
    def to_marker(header, geometry, orientation, hex=True, *args, **kwargs) -> Marker:
        return Marker(header=header, pose=PostGisConverter.to_pose(geometry, orientation, hex=hex), *args, **kwargs)

    @staticmethod
    def to_orientation(orientation: Union[bytes, str], hex=True) -> Quaternion:
        # TODO: check quaternion order match with ROS
        if not orientation:
            return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        rot_vec = wkb.loads(orientation, hex=hex)
        q = Rotation.from_rotvec([rot_vec.x, rot_vec.y, rot_vec.z]).as_quat()
        return Quaternion(x=q[0], y=q[1], z=q[2], w=q[3])

    @staticmethod
    def to_pose(geometry, orientation, hex=True) -> Pose:
        position = PostGisConverter.to_point(geometry)
        orientation = PostGisConverter.to_orientation(orientation)
        return Pose(position=position, orientation=orientation)
    
    @staticmethod
    def to_pose_stamped(geometry, orientation, header, hex=True) -> PoseStamped:
        pose = PostGisConverter.to_pose(geometry, orientation, hex=hex)
        return PoseStamped(header=header, pose=pose)
