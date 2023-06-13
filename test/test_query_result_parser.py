
from postgis_ros_bridge.query_result_parser import QueryResultDefaultParameters, PointResultParser, PoseResultParser, PoseStampedResultParser
from shapely import wkt
from scipy.spatial.transform import Rotation
from std_msgs.msg import Header
from builtin_interfaces.msg import Duration, Time
from rclpy.parameter import Parameter


from pytest_postgresql import factories
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from scipy.spatial.transform import Rotation

import pytest


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
        assert point.x == pytest.approx(element.id-1), 'check if point.x is set correctly'
        assert point.y == pytest.approx(element.id-1), 'check if point.y is set correctly'
        assert point.z == pytest.approx(element.id-1), 'check if point.z is set correctly'    
    

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
        assert point.x == pytest.approx(id), 'check if point.x is set correctly'
        assert point.y == pytest.approx(id), 'check if point.y is set correctly'
        assert point.z == pytest.approx(id), 'check if point.z is set correctly'  
        rot = res[1].orientation
        gt_rot = Rotation.from_rotvec([id, id, id]).as_quat()
        assert rot.x == pytest.approx(gt_rot[0]), 'check if rot.x is set correctly'
        assert rot.y == pytest.approx(gt_rot[1]), 'check if rot.y is set correctly'
        assert rot.z == pytest.approx(gt_rot[2]), 'check if rot.z is set correctly'
        assert rot.w == pytest.approx(gt_rot[3]), 'check if rot.w is set correctly'



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
        assert point.x == pytest.approx(id), 'check if point.x is set correctly'
        assert point.y == pytest.approx(id), 'check if point.y is set correctly'
        assert point.z == pytest.approx(id), 'check if point.z is set correctly'  
        rot = res[1].pose.orientation
        gt_rot = Rotation.from_rotvec([id, id, id]).as_quat()
        assert rot.x == pytest.approx(gt_rot[0]), 'check if rot.x is set correctly'
        assert rot.y == pytest.approx(gt_rot[1]), 'check if rot.y is set correctly'
        assert rot.z == pytest.approx(gt_rot[2]), 'check if rot.z is set correctly'
        assert rot.w == pytest.approx(gt_rot[3]), 'check if rot.w is set correctly'