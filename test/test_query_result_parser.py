
from postgis_ros_bridge.query_result_parser import QueryResultDefaultParameters, PointResultParser
from shapely import wkt
from scipy.spatial.transform import Rotation
from std_msgs.msg import Header
from builtin_interfaces.msg import Duration, Time
from rclpy.parameter import Parameter


from pytest_postgresql import factories
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


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

def test_point_result_parser(db_session_test_db):
    session = db_session_test_db
    defaults = QueryResultDefaultParameters()
    defaults.declare_params()
    defaults.set_params({
            'rate': Parameter(name='rate', value=1), 
            'frame_id': Parameter(name='frame_id', value='test_frame_id'), 
            'utm_transform': Parameter(name='utm_transform', value=False),
            'utm_offset.lat': Parameter(name='utm_offset.lat', value=0.0),
            'utm_offset.lon': Parameter(name='utm_offset.lon', value=0.0),
                         })

    # point_result_parser = PointResultParser()
    # point_result_parser.declare_params(defaults=defaults)
    # point_result_parser.set_params(defaults.declare_params())
    # test = point_result_parser.parse_single_element(element=session.execute(text("SELECT point.geom AS geometry, 'test_frame_id' AS frame_id FROM point")).fetchone(), time=Time())


    # session.execute(text("SELECT * FROM point"))
    # print("test_example_postgres")
