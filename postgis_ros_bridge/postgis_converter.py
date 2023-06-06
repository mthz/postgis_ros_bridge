from geometry_msgs.msg import Point
from shapely import wkb


class PostGisConverter:
    @staticmethod
    def to_point(geometry):
        point = wkb.loads(geometry, hex=True)
        return Point(x=point.x, y=point.y, z=point.z)

    def to_point_tuple(geometry):
        point = wkb.loads(geometry, hex=True)
        return (point.x, point.y, point.z)
