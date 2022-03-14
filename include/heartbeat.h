#pragma once

#include <ros/ros.h>
#include <std_msgs/String.h>

class HeartBeat {
public:
  HeartBeat(ros::NodeHandle nh, std::string topic, std_msgs::String message,
            double interval);
  void start();
  void stop();
  void beat();

private:
  ros::Publisher heartbeat_publisher_;
  std_msgs::String message_;
  ros::Duration interval_;
  bool beat_;
  ros::Time next_beat_;
};
