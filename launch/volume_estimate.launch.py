#!/usr/bin/env python3

import os
from launch import LaunchDescription
from launch_ros.actions import Node
from launch.actions import TimerAction
from ament_index_python.packages import get_package_share_directory

def generate_launch_description():
    pkg_name = 'volume_estimate'
    pkg_path = get_package_share_directory(pkg_name)

    config = os.path.join(
    pkg_path,
    'params',
    'params.yaml'
    )

    lidar_estimate = Node(
        package=pkg_name,
        executable='lidar_calibrator.py',
        parameters = [config]
    )

    merger = Node(
        package=pkg_name,
        executable='merge_pointcloud.py',
        parameters = [config]
    )

    clustering = Node(
        package=pkg_name,
        executable='clustering.py',
        parameters = [config]
    )

    estimate = Node(
        package=pkg_name,
        executable='estimate.py',
        parameters = [config]
    )

    delayed_launch = TimerAction(
        period=5.0,
        actions=[merger, clustering, estimate]
    )

    return LaunchDescription([
        lidar_estimate,
        delayed_launch
    ])