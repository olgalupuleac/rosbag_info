#include <iostream>
#include "rosbag/bag.h"
#include "rosbag/view.h"

class BagInfo{
private:
    rosbag::Bag bag_;
    rosbag::View view_;
    ros::Time start_time_;
    ros::Time end_time_;
    ros::Time duration_;
    std::string filename_;


};

int main(){
    rosbag::Bag bag("/home/olga/ros_comm/tools/rosbag/example.bag");
    std::cout << bag.getFileName() << " " << bag.getMajorVersion() << "." <<
              bag.getMinorVersion() << " " << bag.getCompression() << " " << bag.getSize()  << "\n";
    rosbag::View view(bag);
    std::cout << view.getBeginTime() << " " << view.getEndTime() << " " << view.getEndTime() - view.getBeginTime() << "\n";
    const auto& connections = view.getConnections();
    for(const auto& connection : connections){
        std::cout << /*connection->id << " " <<*/ connection->topic << " " <<
                  connection->datatype << " " << connection->md5sum << /*" " << connection->msg_def << */"\n";
    }
    return 0;
}
/*1295878707.865144
end: 1295878932.387626

  type: geometry_msgs/PoseWithCovarianceStamped
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
      messages: 21032
    - topic: /base_odometry/odometer
      type: pr2_mechanism_controllers/Odometer
      messages: 224
    - topic: /base_odometry/state
      type: pr2_mechanism_controllers/BaseOdometryState
      messages: 224
    - topic: /base_scan
      type: sensor_msgs/LaserScan
      messages: 4489
    - topic: /robot_pose_ekf/odom_combined
      type: geometry_msgs/PoseWithCovarianceStamped
      messages: 6333
    - topic: /tf
      type: tf/tfMessage
      messages: 12687
    - topic: /tilt_scan
      type: sensor_msgs/LaserScan
      messages: 4489
    - topic: /torso_lift_imu/data
      type: sensor_msgs/Imu
      messages: 22451
    - topic: /torso_lift_imu/is_calibrated
      type: std_msgs/Bool
      messages: 1
    - topic: /wide_stereo/left/camera_info_throttle
      type: sensor_msgs/CameraInfo
      messages: 220
    - topic: /wide_stereo/left/image_rect_throttle
      type: sensor_msgs/Image
      messages: 220
    - topic: /wide_stereo/right/camera_info_throttle
      type: sensor_msgs/CameraInfo
      messages: 220
    - topic: /wide_stereo/right/image_rect_throttle
      type: sensor_msgs/Image
      messages: 220

*/