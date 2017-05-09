#include <iostream>
#include "ctime"
#include "getYamlInfo.h"

void get_time_difference(clock_t& begin){
    std::cout << "\n" << float(clock() - begin) /CLOCKS_PER_SEC << "\n";
    begin = clock();
}


int main(){
    clock_t begin_time = clock();
    rosbag::printYamlInfo("/media/olga/Expansion Drive/40gb.bag", "compression");
    get_time_difference(begin_time);
    return 0;
}
/*
 path: /home/olga/Загрузки/2012-05-03-13-33-14.bag
version: 2.0
duration: 1264.804456
start: 1336077194.586362
end: 1336078459.390818
size: 23727983109
messages: 608541
indexed: True
compression: none
types:
    - type: geometry_msgs/PoseWithCovarianceStamped
      md5: 953b798c0f514ff060a53a3498ce6246
    - type: nav_msgs/Odometry
      md5: cd5e73d190d741a2f92e81eda573aca7
    - type: pr2_mechanism_controllers/BaseOdometryState
      md5: 8a483e137ebc37abafa4c26549091dd6
    - type: pr2_mechanism_controllers/Odometer
      md5: 1f1d53743f4592ee455aa3eaf9019457
    - type: sensor_msgs/CameraInfo
      md5: c9a58c1b0b154e0e6da7578cb991d214
    - type: sensor_msgs/Image
      md5: 060021388200f6f0f447d0fcd9c64743
    - type: sensor_msgs/Imu
      md5: 6a62c6daae103f4ff57a132d6f95cec2
    - type: sensor_msgs/LaserScan
      md5: 90c7ef2dc6895d81024acba2ac42f369
    - type: std_msgs/Bool
      md5: 8b94c1b53db61fb6aed406028ad6332a
    - type: tf/tfMessage
      md5: 94810edda583a504dfda3829e70d7eec
topics:
    - topic: /base_odometry/odom
      type: nav_msgs/Odometry
      messages: 116327
    - topic: /base_odometry/odometer
      type: pr2_mechanism_controllers/Odometer
      messages: 1264
    - topic: /base_odometry/state
      type: pr2_mechanism_controllers/BaseOdometryState
      messages: 1264
    - topic: /base_scan
      type: sensor_msgs/LaserScan
      messages: 25171
    - topic: /robot_pose_ekf/odom_combined
      type: geometry_msgs/PoseWithCovarianceStamped
      messages: 34353
    - topic: /tf
      type: tf/tfMessage
      messages: 129943
      connections: 2
    - topic: /tilt_scan
      type: sensor_msgs/LaserScan
      messages: 25293
    - topic: /torso_lift_imu/data
      type: sensor_msgs/Imu
      messages: 126487
    - topic: /torso_lift_imu/is_calibrated
      type: std_msgs/Bool
      messages: 1
    - topic: /wide_stereo/left/camera_info
      type: sensor_msgs/CameraInfo
      messages: 37109
    - topic: /wide_stereo/left/image_raw
      type: sensor_msgs/Image
      messages: 37109
    - topic: /wide_stereo/right/camera_info
      type: sensor_msgs/CameraInfo
      messages: 37110
    - topic: /wide_stereo/right/image_raw
      type: sensor_msgs/Image
      messages: 37110



*/