#pragma once

#include <bag_recorder/Rosbag.h>
#include <ros/ros.h>
#include <std_msgs/String.h>

#include <bag_recorder/bag_recorder.h>

#include <heartbeat.h>

#include <string>
#include <vector>

namespace bag_launcher_node {

using namespace bag_recorder;

struct BLOptions {
  BLOptions();

  std::string configuration_directory;
  std::string data_directory;
  std::string record_start_topic;
  std::string record_stop_topic;
  bool publish_name;
  std::string name_topic;
  bool publish_heartbeat;
  std::string heartbeat_topic;
  double heartbeat_interval;
  bool default_record_all;
};

class BagLauncher {
public:
  BagLauncher(ros::NodeHandle nh, BLOptions options);
  ~BagLauncher();

  void check_all();

private:
  void Start_Recording(const bag_recorder::Rosbag::ConstPtr &msg);

  void Stop_Recording(const std_msgs::String::ConstPtr &msg);

  std::string sanitize_topic(std::string topic);

  bool load_config(std::string config_file_name,
                   std::vector<std::string> &topics,
                   std::set<std::string> loaded = std::set<std::string>());

private:
  ros::NodeHandle nh_;
  std::string config_location_;
  std::string data_folder_;
  ros::Subscriber record_start_subscriber_;
  ros::Subscriber record_stop_subscriber_;
  bool publish_name_;
  bool publish_heartbeat_;
  std::string heartbeat_topic_;
  double heartbeat_interval_;
  ros::Publisher name_publisher_;
  bool default_record_all_;

  std::map<std::string, std::shared_ptr<HeartBeat>> heartbeats_;
  std::map<std::string, std::shared_ptr<BagRecorder>> recorders_;
}; // BagLauncher

} // namespace bag_launcher_node
