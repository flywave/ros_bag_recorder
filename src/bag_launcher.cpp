#include "bag_launcher.h"

#include <fstream>

namespace bag_launcher_node {

BLOptions::BLOptions()
    : configuration_directory("/data/"), data_directory("/media/data"),
      record_start_topic("/bag/start"), record_stop_topic("/bag/stop"),
      publish_name(true), name_topic("/bag/name"), publish_heartbeat(true),
      heartbeat_topic("/bag/heartbeat"), heartbeat_interval(10),
      default_record_all(false) {}

BagLauncher::BagLauncher(ros::NodeHandle nh, BLOptions options)
    : nh_(nh), config_location_(options.configuration_directory),
      data_folder_(options.data_directory),
      record_start_subscriber_(nh.subscribe(
          options.record_start_topic, 10, &BagLauncher::Start_Recording, this)),
      record_stop_subscriber_(nh.subscribe(options.record_stop_topic, 10,
                                           &BagLauncher::Stop_Recording, this)),
      publish_name_(options.publish_name),
      publish_heartbeat_(options.publish_heartbeat),
      heartbeat_topic_(options.heartbeat_topic),
      heartbeat_interval_(options.heartbeat_interval),
      default_record_all_(options.default_record_all) {

  if (options.publish_name) {
    name_publisher_ = nh.advertise<bag_recorder::Rosbag>(
        sanitize_topic(options.name_topic), 5);
  }
}

BagLauncher::~BagLauncher() {
  for (std::map<std::string, std::shared_ptr<BagRecorder>>::iterator it =
           recorders_.begin();
       it != recorders_.end(); ++it)
    it->second->immediate_stop_recording();
}

void BagLauncher::Start_Recording(const bag_recorder::Rosbag::ConstPtr &msg) {
  std::map<std::string, std::shared_ptr<BagRecorder>>::iterator recorder =
      recorders_.find(msg->config);

  if (recorder == recorders_.end()) {
    std::shared_ptr<BagRecorder> new_recorder(new BagRecorder(data_folder_));
    recorders_[msg->config] = new_recorder;
  }

  if (recorders_[msg->config]->is_active()) {
    ROS_WARN("Bag configuration %s is already recording to %s.",
             msg->config.c_str(),
             recorders_[msg->config]->get_bagname().c_str());
    return;
  }

  std::vector<std::string> topics;
  std::string full_bag_name = "";
  if (load_config(msg->config, topics)) {
    full_bag_name =
        recorders_[msg->config]->start_recording(msg->bag_name, topics);
  } else {
    ROS_ERROR(
        "No such config: %s, was able to be loaded from. Recorder not started.",
        msg->config.c_str());
    return;
  }

  if (full_bag_name == "") {
    ROS_WARN("Error prevented %s configuration from recording.",
             msg->config.c_str());
    return;
  }

  if (publish_name_) {
    bag_recorder::Rosbag message;
    message.config = msg->config;
    message.bag_name = full_bag_name;
    name_publisher_.publish(message);
  }
  if (publish_heartbeat_) {
    std_msgs::String message;
    message.data = msg->config;

    std::map<std::string, std::shared_ptr<HeartBeat>>::iterator heartbeat =
        heartbeats_.find(msg->config);

    if (heartbeat == heartbeats_.end()) {
      std::shared_ptr<HeartBeat> beat(
          new HeartBeat(nh_, heartbeat_topic_, message, heartbeat_interval_));
      heartbeats_[msg->config] = beat;
    }

    heartbeats_[msg->config]->start();
  }

  ROS_INFO("Recording %s configuration to %s.", msg->config.c_str(),
           full_bag_name.c_str());

  return;
}

void BagLauncher::Stop_Recording(const std_msgs::String::ConstPtr &msg) {
  std::map<std::string, std::shared_ptr<BagRecorder>>::iterator recorder =
      recorders_.find(msg->data);

  if (recorder != recorders_.end()) {
    recorder->second->stop_recording();
    ROS_INFO("%s configuration recorder stopped.", msg->data.c_str());
  } else {
    ROS_INFO("%s configuration recorder did not exist.", msg->data.c_str());
  }

  if (publish_heartbeat_) {
    std::map<std::string, std::shared_ptr<HeartBeat>>::iterator heartbeat =
        heartbeats_.find(msg->data);

    if (heartbeat != heartbeats_.end()) {
      heartbeat->second->stop();
    }
  }

  return;
}

std::string BagLauncher::sanitize_topic(std::string topic) {
  if (topic.substr(0, 1) != "/")
    topic = "/" + topic;
  return topic;
}

bool BagLauncher::load_config(std::string config_name,
                              std::vector<std::string> &topics,
                              std::set<std::string> loaded) {
  std::string config_file_name = config_location_ + config_name + ".config";
  std::ifstream fd(config_file_name.c_str());
  std::string line;

  if (loaded.find(config_name) == loaded.end()) {
    loaded.insert(config_name);
  } else {
    ROS_WARN("%s config loaded alread, circular reference detected.",
             config_name.c_str());
    return false;
  }

  if (!fd) {
    if (loaded.size() <= 1 && default_record_all_) {
      ROS_ERROR("Topic input file name invalid, recording everything");
      topics.push_back("*");
      return true;
    } else {
      ROS_WARN("Linked config: %s is invalid.", config_name.c_str());
      return false;
    }
  } else {
    while (std::getline(fd, line)) {
      if (line == "" || line.substr(0, 1) == " " || line.substr(0, 1) == "#")
        continue;
      if (line.substr(0, 1) == "$") {
        load_config(line.substr(1), topics, loaded);
        continue;
      }
      topics.push_back(sanitize_topic(line));
    }
    return true;
  }
}

void BagLauncher::check_all() {
  for (std::map<std::string, std::shared_ptr<HeartBeat>>::iterator it =
           heartbeats_.begin();
       it != heartbeats_.end(); ++it)
    it->second->beat();
}

} // namespace bag_launcher_node
