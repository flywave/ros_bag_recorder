#include "heartbeat.h"

HeartBeat::HeartBeat(ros::NodeHandle nh, std::string topic,
                     std_msgs::String message, double interval)
    : heartbeat_publisher_(nh.advertise<std_msgs::String>(topic, 10)),
      message_(message), interval_(ros::Duration().fromSec(interval)),
      beat_(false) {}

void HeartBeat::start() {
  next_beat_ = ros::Time::now();
  beat_ = true;

  beat();
}

void HeartBeat::stop() { beat_ = false; }

void HeartBeat::beat() {
  if (!beat_ || ros::Time::now() < next_beat_)
    return;

  next_beat_ += interval_;

  heartbeat_publisher_.publish(message_);
}
