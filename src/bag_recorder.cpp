#include <bag_recorder/bag_recorder.h>
#include <std_msgs/Empty.h>
#include <std_msgs/String.h>

#include <boost/filesystem.hpp>
#include <sys/stat.h>

#if BOOST_FILESYSTEM_VERSION < 3
#include <sys/statvfs.h>
#endif
#if !defined(_MSC_VER)
#include <termios.h>
#include <unistd.h>
#endif

#include <boost/date_time/local_time/local_time.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/xtime.hpp>

#define foreach BOOST_FOREACH

namespace bag_recorder {

using boost::shared_ptr;
using ros::Time;
using std::string;

OutgoingMessage::OutgoingMessage(
    string const &_topic, topic_tools::ShapeShifter::ConstPtr _msg,
    boost::shared_ptr<ros::M_string> _connection_header, Time _time)
    : topic(_topic), msg(_msg), connection_header(_connection_header),
      time(_time) {}

BagRecorder::BagRecorder(std::string data_folder, bool append_date)
    : data_folder_(data_folder), append_date_(append_date) {

  ros::NodeHandle nh;

  if (!nh.ok())
    return;

  if (!ros::Time::waitForValid(ros::WallDuration(2.0)))
    ROS_WARN("/use_sim_time set to true and no clock published.  Still waiting "
             "for valid time...");

  ros::Time::waitForValid();

  if (!nh.ok())
    return;
}

BagRecorder::~BagRecorder() {
  if (is_active())
    immediate_stop_recording();
  delete message_queue_;
}

std::string BagRecorder::start_recording(std::string bag_name,
                                         std::vector<std::string> topics,
                                         bool record_all_topics) {
  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);

  if (bag_active_)
    return "";
  bag_active_ = true;
  stop_signal_ = false;
  clear_queue_signal_ = false;

  size_t ind = bag_name.rfind(".bag");
  if (ind != std::string::npos && ind == bag_name.size() - 4) {
    bag_name.erase(ind);
  }

  if (append_date_)
    bag_name += string("_") + get_time_str();

  if (bag_name.length() == 0) {
    ROS_ERROR("Bag Name has length 0. Unable to record.");
    return "";
  }

  bag_name += string(".bag");
  bag_filename_ = data_folder_ + bag_name;

  message_queue_ = new std::queue<OutgoingMessage>;

  foreach (string const &topic, topics) {
    if (topic.find("*") != std::string::npos) {
      record_all_topics = true;
    }
  }

  if (record_all_topics) {
    recording_all_topics_ = true;
    subscribe_all();
  } else if (topics.size() > 0) {
    foreach (string const &topic, topics)
      if (subscribed_topics_.find(topic) == subscribed_topics_.end()) {
        try {
          subscribers_.push_back(generate_subscriber(topic));
        } catch (ros::InvalidNameException) {
          ROS_ERROR("Invalid topic name: %s, no subscriber generated.",
                    topic.c_str());
        }
      }
  } else {
    ROS_ERROR("No Topics Supplied to be recorded. Aborting bag %s.",
              bag_name.c_str());
    return "";
  }

  bag_.setCompression(rosbag::compression::Uncompressed);
  bag_.setChunkThreshold(1024 * 768);

  try {
    bag_.open(bag_filename_ + string(".active"), rosbag::bagmode::Write);
  } catch (rosbag::BagException e) {
    ROS_ERROR("Error writing: %s", e.what());
    bag_active_ = false;
    return "";
  }

  record_thread_ =
      boost::thread(boost::bind(&BagRecorder::queue_processor, this));
  queue_condition_.notify_all();

  return bag_name;
}

void BagRecorder::stop_recording() {
  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);

  if (!bag_active_)
    return;

  clear_queue_signal_ = true;

  foreach (boost::shared_ptr<ros::Subscriber> sub, subscribers_)
    sub->shutdown();

  subscribed_topics_.clear();

  ROS_INFO("Stopping BagRecorder, clearing queue.");
}

void BagRecorder::immediate_stop_recording() {
  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);

  if (!bag_active_)
    return;

  stop_signal_ = true;

  foreach (boost::shared_ptr<ros::Subscriber> sub, subscribers_)
    sub->shutdown();

  subscribed_topics_.clear();

  ROS_INFO("Stopping BagRecorder immediately.");
}

bool BagRecorder::is_active() {
  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);
  return bag_active_;
}

std::string BagRecorder::get_bagname() { return bag_filename_; }

bool BagRecorder::can_log() {
  if (!checks_failed_)
    return true;

  if (ros::WallTime::now() >= warn_next_) {
    warn_next_ += ros::WallDuration().fromSec(5.0);
    ROS_WARN("Not logging message because logging disabled.  Most likely cause "
             "is a full disk.");
  }
  return false;
}

bool BagRecorder::is_subscribed_to(std::string topic) {
  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);
  return (subscribed_topics_.find(topic) != subscribed_topics_.end());
}

shared_ptr<ros::Subscriber>
BagRecorder::generate_subscriber(string const &topic) {
  ROS_DEBUG("Subscribing to %s", topic.c_str());

  ros::NodeHandle nh;
  shared_ptr<int> count(boost::make_shared<int>(0));
  shared_ptr<ros::Subscriber> sub(boost::make_shared<ros::Subscriber>());

  ros::SubscribeOptions ops;
  ops.topic = topic;
  ops.queue_size = 100;
  ops.md5sum = ros::message_traits::md5sum<topic_tools::ShapeShifter>();
  ops.datatype = ros::message_traits::datatype<topic_tools::ShapeShifter>();
  ops.helper = boost::make_shared<ros::SubscriptionCallbackHelperT<
      const ros::MessageEvent<topic_tools::ShapeShifter const> &>>(
      boost::bind(&BagRecorder::subscriber_callback, this, _1, topic, sub,
                  count));
  *sub = nh.subscribe(ops);

  ROS_INFO("Subscribing to topic: %s", topic.c_str());
  subscribed_topics_.insert(topic);

  return sub;
}

void BagRecorder::subscriber_callback(
    const ros::MessageEvent<topic_tools::ShapeShifter const> &msg_event,
    string const &topic, shared_ptr<ros::Subscriber> subscriber,
    shared_ptr<int> count) {
  (void)subscriber;
  (void)count;

  Time rectime = Time::now();

  OutgoingMessage out(topic, msg_event.getMessage(),
                      msg_event.getConnectionHeaderPtr(), rectime);

  {
    boost::mutex::scoped_lock queue_lock(queue_mutex_);
    message_queue_->push(out);
  }

  queue_condition_.notify_all();
}

void BagRecorder::queue_processor() {
  ROS_INFO("Recording to %s.", bag_filename_.c_str());

  warn_next_ = ros::WallTime();
  check_disk_next_ =
      ros::WallTime::now() + ros::WallDuration().fromSec(check_disk_interval_);
  subscribe_all_next_ = ros::WallTime::now() +
                        ros::WallDuration().fromSec(subscribe_all_interval_);
  check_disk();

  ros::NodeHandle nh;
  while ((nh.ok() || !message_queue_->empty())) {
    boost::unique_lock<boost::mutex> queue_lock(queue_mutex_);

    bool finished = false;
    while (message_queue_->empty()) {
      {
        boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);
        if (stop_signal_ || clear_queue_signal_) {
          finished = true;
          break;
        }
      }

      if (!nh.ok()) {
        queue_lock.release()->unlock();
        finished = true;
        break;
      }

      run_scheduled_checks();

      boost::xtime xt;
#if BOOST_VERSION >= 105000
      boost::xtime_get(&xt, boost::TIME_UTC_);
#else
      boost::xtime_get(&xt, boost::TIME_UTC);
#endif
      xt.nsec += 250000000;
      queue_condition_.timed_wait(queue_lock, xt);
    }
    {
      boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);
      if (finished || stop_signal_)
        break;
    }

    OutgoingMessage out = message_queue_->front();
    message_queue_->pop();

    queue_lock.release()->unlock();

    run_scheduled_checks();

    if (can_log())
      bag_.write(out.topic, out.time, *out.msg, out.connection_header);
  }

  ROS_INFO("Closing %s.", bag_filename_.c_str());
  bag_.close();
  rename((bag_filename_ + string(".active")).c_str(), bag_filename_.c_str());

  while (!message_queue_->empty())
    message_queue_->pop();

  boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);
  bag_active_ = false;
}

void BagRecorder::run_scheduled_checks() {
  if (ros::WallTime::now() < check_disk_next_) {
    check_disk_next_ += ros::WallDuration().fromSec(check_disk_interval_);
    check_disk();
  }

  if (ros::WallTime::now() >= subscribe_all_next_) {
    subscribe_all_next_ += ros::WallDuration().fromSec(subscribe_all_interval_);

    boost::mutex::scoped_lock start_stop_lock(start_stop_mutex_);

    if (!(stop_signal_ || clear_queue_signal_) && recording_all_topics_) {
      subscribe_all();
    }
  }
}

void BagRecorder::subscribe_all() {
  ros::master::V_TopicInfo topics;
  if (ros::master::getTopics(topics)) {
    foreach (ros::master::TopicInfo const &t, topics) {
      if (subscribed_topics_.find(t.name) == subscribed_topics_.end())
        subscribers_.push_back(generate_subscriber(t.name));
    }
  }
}

void BagRecorder::check_disk() {
#if BOOST_FILESYSTEM_VERSION < 3
  struct statvfs fiData;
  if ((statvfs(bag_.getFileName().c_str(), &fiData)) < 0) {
    ROS_WARN("Failed to check filesystem stats.");
    return;
  }
  unsigned long long free_space = 0;
  free_space = (unsigned long long)(fiData.f_bsize) *
               (unsigned long long)(fiData.f_bavail);
  if (free_space < min_recording_space_) {
    ROS_ERROR(
        "Less than %s of space free on disk with %s.  Disabling recording.",
        min_recording_space_str_.c_str(), bag_.getFileName().c_str());
    checks_failed_ = true;
    return;
  } else if (free_space < 5 * min_recording_space_) {
    ROS_WARN("Less than 5 x %s of space free on disk with %s.",
             min_recording_space_str_.c_str(), bag_.getFileName().c_str());
  } else {
    checks_failed_ = false;
  }
#else
  boost::filesystem::path p(
      boost::filesystem::system_complete(bag_.getFileName().c_str()));
  p = p.parent_path();
  boost::filesystem::space_info info;
  try {
    info = boost::filesystem::space(p);
  } catch (boost::filesystem::filesystem_error &e) {
    ROS_WARN("Failed to check filesystem stats [%s].", e.what());
    checks_failed_ = true;
    return;
  }
  if (info.available < min_recording_space_) {
    ROS_ERROR(
        "Less than %s of space free on disk with %s.  Disabling recording.",
        min_recording_space_str_.c_str(), bag_.getFileName().c_str());
    checks_failed_ = true;
    return;
  } else if (info.available < 5 * min_recording_space_) {
    ROS_WARN("Less than 5 x %s of space free on disk with %s.",
             min_recording_space_str_.c_str(), bag_.getFileName().c_str());
    checks_failed_ = false;
  } else {
    checks_failed_ = false;
  }
#endif
  return;
}

std::string BagRecorder::get_time_str() {
  std::stringstream msg;
  const boost::posix_time::ptime now =
      boost::posix_time::second_clock::local_time();
  boost::posix_time::time_facet *const f =
      new boost::posix_time::time_facet("%Y-%m-%d-%H-%M-%S");
  msg.imbue(std::locale(msg.getloc(), f));
  msg << now;
  return msg.str();
}

} // namespace bag_recorder
