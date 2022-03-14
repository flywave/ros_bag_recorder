#pragma once

// Main ROS
#include <ros/ros.h>
#include <ros/time.h>
#include <topic_tools/shape_shifter.h>

// rosbag tools
#include "rosbag/bag.h"
#include "rosbag/macros.h"
#include "rosbag/stream.h"

// Thread management
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

// std tools
#include <queue>
#include <string>
#include <vector>

namespace bag_recorder {

class OutgoingMessage {
public:
  OutgoingMessage(std::string const &_topic,
                  topic_tools::ShapeShifter::ConstPtr _msg,
                  boost::shared_ptr<ros::M_string> _connection_header,
                  ros::Time _time);

  std::string topic;
  topic_tools::ShapeShifter::ConstPtr msg;
  boost::shared_ptr<ros::M_string> connection_header;
  ros::Time time;
};

class BagRecorder {
public:
  BagRecorder(std::string data_folder, bool append_date = true);
  ~BagRecorder();

  std::string start_recording(std::string bag__name,
                              std::vector<std::string> topics,
                              bool record_all_topics = false);
  void stop_recording();
  void immediate_stop_recording();

  bool is_active();
  bool can_log();
  bool is_subscribed_to(std::string topic);

  std::string get_bagname();

private:
  void subscribe_all();
  boost::shared_ptr<ros::Subscriber>
  generate_subscriber(std::string const &topic);
  void subscriber_callback(
      const ros::MessageEvent<topic_tools::ShapeShifter const> &msg_event,
      std::string const &topic, boost::shared_ptr<ros::Subscriber> subscriber,
      boost::shared_ptr<int> count);
  void unsubscribe_all();

  void queue_processor();

  void run_scheduled_checks();
  void check_disk();

  static std::string get_time_str();

private:
  std::string data_folder_;
  bool append_date_;
  bool recording_all_topics_;
  unsigned long long min_recording_space_ = 1024 * 1024 * 1024;
  std::string min_recording_space_str_ = "1G";

  rosbag::Bag bag_;
  std::string bag_filename_ = "";
  bool bag_active_ = false;

  bool clear_queue_signal_;
  bool stop_signal_;
  boost::mutex start_stop_mutex_;

  std::vector<boost::shared_ptr<ros::Subscriber>> subscribers_;
  std::set<std::string> subscribed_topics_;
  ros::WallTime subscribe_all_next_;
  boost::mutex subscribers_mutex_;

  boost::condition_variable_any queue_condition_;
  boost::mutex queue_mutex_;
  std::queue<OutgoingMessage> *message_queue_;

  boost::thread record_thread_;

  bool checks_failed_ = false;
  ros::WallTime check_disk_next_;
  ros::WallTime warn_next_;

  double subscribe_all_interval_ = 5.0;
  double check_disk_interval_ = 10.0;
}; // BagRecorder

} // namespace bag_recorder
