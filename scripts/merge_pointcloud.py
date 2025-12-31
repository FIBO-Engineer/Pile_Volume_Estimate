#!/usr/bin/python3
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2, PointField
from sensor_msgs_py import point_cloud2 as pc2
import numpy as np
import yaml
import tf_transformations
from tf2_ros import Buffer, TransformListener
from message_filters import Subscriber, ApproximateTimeSynchronizer

class LidarMerger(Node):
    def __init__(self):
        super().__init__('lidar_merger')
        
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        self.declare_parameter('config_path', '/home/ponwalai/fibo-technovation/volume_estimate/src/params/lidar_pose.yaml')
        config_path = self.get_parameter('config_path').value
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.topics = [list(item.keys())[0] for item in self.config]
        self.target_frame = 'base_link'
        
        self.subs = [Subscriber(self, PointCloud2, t) for t in self.topics]
        self.ts = ApproximateTimeSynchronizer(self.subs, queue_size=10, slop=0.1)
        self.ts.registerCallback(self.sync_callback)
        
        self.publisher = self.create_publisher(PointCloud2, '/merged_cloud', 10)
        self.get_logger().info("Merger Started")

    def get_matrix_from_tf(self, target_frame, source_frame):
        try:
            trans = self.tf_buffer.lookup_transform(target_frame, source_frame, rclpy.time.Time())
            t = [trans.transform.translation.x, 
                 trans.transform.translation.y, 
                 trans.transform.translation.z]
            q = [trans.transform.rotation.x, 
                 trans.transform.rotation.y, 
                 trans.transform.rotation.z, 
                 trans.transform.rotation.w]
            mat = tf_transformations.quaternion_matrix(q)
            mat[:3, 3] = t
            return mat
        except Exception:
            return None

    def sync_callback(self, *cloud_msgs):
        all_clouds_data = []
        
        for topic, msg in zip(self.topics, cloud_msgs):
            matrix = self.get_matrix_from_tf(self.target_frame, topic)
            if matrix is None: continue

            gen = pc2.read_points(msg)
            pts_array = np.fromiter(gen, dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4')])
            pts_array = pts_array.view('float32').reshape(-1, 3)
            
            if pts_array.shape[0] == 0: continue

            xyz = pts_array[:, :3]

            points_h = np.ones((xyz.shape[0], 4))
            points_h[:, :3] = xyz
            transformed_xyz = (matrix @ points_h.T).T[:, :3]

            all_clouds_data.append(transformed_xyz)

        if not all_clouds_data:
            return

        merged_points = np.vstack(all_clouds_data)

        fields = [
            PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
            PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
        ]

        output_msg = pc2.create_cloud(
            header=cloud_msgs[0].header,
            fields=fields,
            points=merged_points
        )
        
        output_msg.header.frame_id = self.target_frame
        self.publisher.publish(output_msg)

def main(args=None):
    rclpy.init(args=args)
    node = LidarMerger()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()