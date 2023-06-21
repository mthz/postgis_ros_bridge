# PostGIS ROS2 Bridge

ROS2 node connecting a PostgreSQL database with spatial PostGIS data with the ROS world.

## Basic Usage

The publishing node can be started with

````bash
ros2 run postgis_ros_bridge postgis_ros_bridge_publisher_node --ros-args --params-file /PATH_TO/params.yaml
````
The `yaml` file is structured in two fixed sections (postgres and list of publisher), one optional default section, and n=len(list of publish) sections for the queries:

The `postgresql`-section with username / password (either as plain text or give the name of an environmen variable holding the password), as well as the hostname, port and the schema to be used, e.g,:
````yaml
postgresql:
        user: "postgres"
        pass_env: "POSTGRES_PASSWORD" # read password from environment variable (recommended)
        pass: "postgres" # Alternative to pass_env (store password in plaintext (not recommended))
        host: "localhost" 
        port: 5432
        schema: "postgres_alchemy_ait"
````
This is followed by a list of query publishers, for the example in `cfg/example.yaml`:
````yaml
publish:
        - query_point
        - query_marker
        - query_marker_array
        - query_pointcloud
        - query_pose
        - query_pose_array
        - query_pose_stamped
        - query_polygon
        - query_polygon_stamped
````
For each of these list elements, there needs to be a section with parameters, depending on the type of desired message beeing published.

## Supported ROS2 Messages

The publisher(s) are set up in the defined sections from above. Every section has at least a SQL query that contains a column `geometry`.
Depending on the targeted message type, there is also a special column `rotation`, `frame_id`, and `id`.
Parameters for all sections can be set in a `query_defaults` section, e.g.:
````yaml
query_defaults:
        rate: 10.0
        frame_id: "map"
````

### geometry_msgs/msg/PointStamped [(ROS2 reference)](https://docs.ros2.org/latest/api/geometry_msgs/msg/PointStamped.html)
````yaml
query_point:
        query: "SELECT position AS geometry, 'test_frame_id' AS frame_id FROM landmark;"
        type: "PointStamped" 
        topic: "point"
````
This is a simple example to query the `position` column from a table called `landmark` and rename it to the defined `geometry` keyword.
The frame id is set static for all points to `test_frame_id`. This valued could be fetched from the database as well using a more advanced query. The type is set using the `type` parameter, and the topic to publish the result as `topic`.
The parameters set in the query_defaults (`rate` and `frame_id`) could be set as all here to overwrite the defaults.

### visualization_msgs/msg/Marker [(ROS2 reference)](https://docs.ros2.org/galactic/api/visualization_msgs/msg/Marker.html)
````yaml
query_marker:
        query: "SELECT ROW_NUMBER() OVER (ORDER BY pose.id) AS id, pose.position AS geometry, pose.rotation_vector AS rotation, test_frame_id' AS frame_id FROM pose;"
        type: "Marker"
        marker_type: "visualization_msgs::Marker::SPHERE" # marker_type or here | default sphere
        topic: "marker"
````
Example of a marker message generation. As PostGIS does not support 6DoF poses, additionally to the `geometry` column containing the position (x,y,z) of the marker, a second column `rotation` is needed. This column is expected to hold the rotation as scaled euler rotation (see [scipy](https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.transform.Rotation.html)) in (roll, pitch, yaw). The `id` field of the ROS2 marker message can be filled for each row using the `id` column.
Implemented marker_types are `ARROW`, `CUBE`, `SPHERE`, and  `CYLINDER` (list of supported types can be extended easily in code, see `postgis_ros_bridge/query_result_parser.py`).

### visualization_msgs/msg/MarkerArray [(ROS2 reference)](https://docs.ros2.org/galactic/api/visualization_msgs/msg/MarkerArray.html)
````yaml
query_marker_array:
        query: "SELECT ROW_NUMBER() OVER (ORDER BY pose.id) AS id, pose.position AS geometry, pose.rotation_vector AS rotation FROM pose;"
        type: "MarkerArray"
        marker_type: "visualization_msgs::Marker::SPHERE" 
        topic: "marker_array"
        frame_id: "test_frame_id"
````
For suitable message types, there is also the `Array` version implemented. For this example, the type is simply set to `MarkerArray`. In this example, the `frame_id` is set in the parameter section. It is also possible to define the `frame_id` in the query itself or as here, in the defaults. Query result overwrite the value set in the query section, and this parameter overwrites one in the defaults section, i.e. query result -> parameter section -> default value.



