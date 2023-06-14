
import pytest
from builtin_interfaces.msg import Duration, Time
from pytest_postgresql import factories
from rclpy.parameter import Parameter
from scipy.spatial.transform import Rotation
from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2
from shapely import wkt
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from std_msgs.msg import Header

from postgis_ros_bridge.query_result_parser import (
    PC2ResultParser, PointResultParser, PolygonResultParser,
    PolygonStampedResultParser, PoseResultParser, PoseStampedResultParser,
    QueryResultDefaultParameters)

test_sql_files = {
    "postgis_test":
        "/workspaces/ws_openschema/src/postgis_ros_bridge/test/sql_data/postgis_test.sql",
}


@pytest.fixture
def db_session(postgresql):
    """Create a database session for testing."""
    connection_uri = f'postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'
    engine = create_engine(connection_uri, poolclass=NullPool)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def db_session_test_db(db_session):
    """Fill the database with test data."""
    for sql_file in test_sql_files.values():
        db_session.execute(text(open(sql_file).read()))
    db_session.commit()
    return db_session


def test_simple_dummy_db_connection(db_session_test_db):
    """Test if the dummy database connection works."""
    session = db_session_test_db
    result = session.execute(text("SELECT * FROM point"))
    assert result.rowcount == 5


params = {
    'rate': Parameter(name='rate', value=1),
    'frame_id': Parameter(name='frame_id', value='test_frame_id'),
    'topic': Parameter(name='topic', value='test_topic'),
    'utm_transform': Parameter(name='utm_transform', value=False),
    'utm_offset.lat': Parameter(name='utm_offset.lat', value=0.0),
    'utm_offset.lon': Parameter(name='utm_offset.lon', value=0.0),
}


@pytest.fixture
def query_result_default_parameters():
    """Create a default parameter object."""
    defaults = QueryResultDefaultParameters()
    defaults.declare_params()
    defaults.set_params(params)
    return defaults


@pytest.fixture
def point_result_parser(query_result_default_parameters):
    """Create a point result parser."""
    defaults = query_result_default_parameters
    prp = PointResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_point_result_parser(db_session_test_db, point_result_parser):
    '''Test if the point result parser works.'''
    prp = point_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'PointStamped', 'check if type is set correctly'

    for element in db.execute(text("SELECT ROW_NUMBER() OVER (ORDER BY point.id) AS id, point.geom AS geometry, 'test_frame_id' AS frame_id FROM point")).all():
        res = prp.parse_single_element(element=element, time=Time())
        assert res[0], 'check if topic is set'
        topic = res[0]
        assert params['topic'].value == topic, 'check if topic is set correctly'
        assert res[1], 'check if point is set'
        point = res[1].point
        assert point.x == pytest.approx(
            element.id-1), 'check if point.x is set correctly'
        assert point.y == pytest.approx(
            element.id-1), 'check if point.y is set correctly'
        assert point.z == pytest.approx(
            element.id-1), 'check if point.z is set correctly'


@pytest.fixture
def pose_result_parser(query_result_default_parameters):
    """Create a pose result parser."""
    defaults = query_result_default_parameters
    prp = PoseResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_pose_result_parser(db_session_test_db, pose_result_parser):
    '''Test if the pose result parser works.'''
    prp = pose_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'Pose', 'check if type is set correctly'

    for element in db.execute(text("SELECT ROW_NUMBER() OVER (ORDER BY point.id) AS id, point.geom AS geometry, point.geom AS rotation, 'test_frame_id' AS frame_id FROM point")).all():
        res = prp.parse_single_element(element=element, time=Time())
        assert res[0], 'check if topic is set'
        topic = res[0]
        assert params['topic'].value == topic, 'check if topic is set correctly'
        assert res[1], 'check if point is set'
        point = res[1].position
        id = element.id-1
        assert point.x == pytest.approx(
            id), 'check if point.x is set correctly'
        assert point.y == pytest.approx(
            id), 'check if point.y is set correctly'
        assert point.z == pytest.approx(
            id), 'check if point.z is set correctly'
        rot = res[1].orientation
        gt_rot = Rotation.from_rotvec([id, id, id]).as_quat()
        assert rot.x == pytest.approx(
            gt_rot[0]), 'check if rot.x is set correctly'
        assert rot.y == pytest.approx(
            gt_rot[1]), 'check if rot.y is set correctly'
        assert rot.z == pytest.approx(
            gt_rot[2]), 'check if rot.z is set correctly'
        assert rot.w == pytest.approx(
            gt_rot[3]), 'check if rot.w is set correctly'


@pytest.fixture
def pose_stamped_result_parser(query_result_default_parameters):
    """Create a pose result parser."""
    defaults = query_result_default_parameters
    prp = PoseStampedResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_pose_stamped_result_parser(db_session_test_db, pose_stamped_result_parser):
    '''Test if the pose stamped result parser works.'''
    prp = pose_stamped_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'PoseStamped', 'check if type is set correctly'

    for element in db.execute(text("SELECT ROW_NUMBER() OVER (ORDER BY point.id) AS id, point.geom AS geometry, point.geom AS rotation, 'test_frame_id' AS frame_id FROM point")).all():
        res = prp.parse_single_element(element=element, time=Time())
        assert res[0], 'check if topic is set'
        topic = res[0]
        assert params['topic'].value == topic, 'check if topic is set correctly'
        assert res[1], 'check if point is set'
        point = res[1].pose.position
        id = element.id-1
        assert point.x == pytest.approx(
            id), 'check if point.x is set correctly'
        assert point.y == pytest.approx(
            id), 'check if point.y is set correctly'
        assert point.z == pytest.approx(
            id), 'check if point.z is set correctly'
        rot = res[1].pose.orientation
        gt_rot = Rotation.from_rotvec([id, id, id]).as_quat()
        assert rot.x == pytest.approx(
            gt_rot[0]), 'check if rot.x is set correctly'
        assert rot.y == pytest.approx(
            gt_rot[1]), 'check if rot.y is set correctly'
        assert rot.z == pytest.approx(
            gt_rot[2]), 'check if rot.z is set correctly'
        assert rot.w == pytest.approx(
            gt_rot[3]), 'check if rot.w is set correctly'


@pytest.fixture
def pc2_result_parser(query_result_default_parameters):
    """Create a pc2 result parser."""
    defaults = query_result_default_parameters
    prp = PC2ResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_pc2_result_parser(db_session_test_db, pc2_result_parser):
    '''Test if the pc2 result parser works.'''
    prp = pc2_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'PointCloud2', 'check if type is set correctly'

    db_res = db.execute(text(
        "SELECT ROW_NUMBER() OVER (ORDER BY point.id) AS id, point.geom AS geometry, 'test_frame_id' AS frame_id FROM point"))
    res = prp.parse_result(result=db_res, time=Time())
    assert len(res) == 1, 'check if one result is returned'
    res = res[0]
    assert res[0], 'check if topic is set'
    topic = res[0]
    assert params['topic'].value == topic, 'check if topic is set correctly'
    assert res[1], 'check if pc is set'
    pc = res[1]
    assert pc.height == 1, 'check if pc.height is set correctly'
    assert pc.width == 5, 'check if pc.width is set correctly'

    pc_numpy = point_cloud2.read_points(pc, skip_nans=True)
    for i, point in enumerate(pc_numpy):
        assert point[0] == pytest.approx(
            i), 'check if point.x is set correctly'
        assert point[1] == pytest.approx(
            i), 'check if point.y is set correctly'
        assert point[2] == pytest.approx(
            i), 'check if point.z is set correctly'


gt_polygons = [
    [[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0], [0, 0, 0]],
    [[2, 2, 0], [3, 2, 0], [3, 3, 0], [2, 3, 0], [2, 2, 0]],
    [[4, 4, 0], [5, 4, 0], [5, 5, 0], [4, 5, 0], [4, 4, 0]],
    [[6, 6, 0], [7, 6, 0], [7, 7, 0], [6, 7, 0], [6, 6, 0]],
    [[8, 8, 0], [9, 8, 0], [9, 9, 0], [8, 9, 0], [8, 8, 0]],
]


@pytest.fixture
def polygon_result_parser(query_result_default_parameters):
    """Create a polygon result parser."""
    defaults = query_result_default_parameters
    prp = PolygonResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_polygon_result_parser(db_session_test_db, polygon_result_parser):
    '''Test if the polygon result parser works.'''
    prp = polygon_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'Polygon', 'check if type is set correctly'
    db_polygons = db.execute(text(
        "SELECT ROW_NUMBER() OVER (ORDER BY polygon.id) AS id, polygon.geom AS geometry, 'test_frame_id' AS frame_id FROM polygon")).all()
    assert len(db_polygons) == len(
        gt_polygons), 'check if all polygons are returned'

    for gt_polys, elements in zip(gt_polygons, db_polygons):
        res = prp.parse_single_element(element=elements, time=Time())
        assert res[0], 'check if topic is set'
        topic = res[0]
        assert params['topic'].value == topic, 'check if topic is set correctly'
        assert res[1], 'check if point is set'
        points = res[1].points
        assert len(points) == len(gt_polys), 'check if all points are returned'
        for gt_poly, element in zip(gt_polys, points):
            assert element.x == pytest.approx(
                gt_poly[0]), 'check if point.x is set correctly'
            assert element.y == pytest.approx(
                gt_poly[1]), 'check if point.y is set correctly'
            assert element.z == pytest.approx(
                gt_poly[2]), 'check if point.z is set correctly'


@pytest.fixture
def polygon_stamped_result_parser(query_result_default_parameters):
    """Create a polygon stamped result parser."""
    defaults = query_result_default_parameters
    prp = PolygonStampedResultParser()
    prp.declare_params(defaults=defaults)
    prp.set_params(params=params)
    return prp


def test_polygon_stamped_result_parser(db_session_test_db, polygon_stamped_result_parser):
    '''Test if the polygon stamped result parser works.'''
    prp = polygon_stamped_result_parser
    db = db_session_test_db

    assert prp.TYPE == 'PolygonStamped', 'check if type is set correctly'
    db_polygons = db.execute(text(
        "SELECT ROW_NUMBER() OVER (ORDER BY polygon.id) AS id, polygon.geom AS geometry, 'test_frame_id' AS frame_id FROM polygon")).all()
    assert len(db_polygons) == len(
        gt_polygons), 'check if all polygons are returned'

    for gt_polys, elements in zip(gt_polygons, db_polygons):
        res = prp.parse_single_element(element=elements, time=Time())
        assert res[0], 'check if topic is set'
        topic = res[0]
        assert params['topic'].value == topic, 'check if topic is set correctly'
        assert res[1], 'check if point is set'
        points = res[1].polygon.points
        assert len(points) == len(gt_polys), 'check if all points are returned'
        for gt_poly, element in zip(gt_polys, points):
            assert element.x == pytest.approx(
                gt_poly[0]), 'check if point.x is set correctly'
            assert element.y == pytest.approx(
                gt_poly[1]), 'check if point.y is set correctly'
            assert element.z == pytest.approx(
                gt_poly[2]), 'check if point.z is set correctly'
