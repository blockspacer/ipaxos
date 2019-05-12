#ifndef _PAXOS_CONFIG_H_
#define _PAXOS_CONFIG_H_
#include <cstdint>

#define PAXOS_DEFAULT_COMMIT_TIMEOUT 600
#define PAXOS_DEFAULT_VOTE_TIMEOUT 600
#define PAXOS_DEFAULT_LEARN_TIMEOUT 600

#define PAXOS_DEFAULT_CONNECTION_CHECK_TIMEOUT 10
#define PAXOS_DEFAULT_COMPACT_INTERVAL 20
#define PAXOS_DEFAULT_RANDOM_WAIT_LOW 150
#define PAXOS_DEFAULT_RANDOM_WAIT_HIGH 300
#define PAXOS_DEFAULT_BATCH_SIZE 50

class PaxosConfig;

class PaxosConfig {
public:
  inline void set_batch_interval(uint32_t t) {
    batch_interval_ = t;
  }
  inline uint32_t batch_interval() { return batch_interval_; }

  inline void set_compact_interval(uint32_t t) {
    compact_interval_ = t;
  }
  inline uint32_t compact_interval() { return compact_interval_; }

  inline void set_connection_check_timeout(uint32_t t) {
    connection_check_timeout_ = t;
  }
  inline uint32_t connection_check_timeout() { return connection_check_timeout_; }

  inline void set_random_wait_low(uint32_t t) {
    random_wait_low_ = t;
  }
  inline uint32_t random_wait_low() { return random_wait_low_; }

  inline void set_random_wait_high(uint32_t t) {
    random_wait_high_ = t;
  }
  inline uint32_t random_wait_high() { return random_wait_high_; }

  inline void set_batch_size(uint32_t t) {
    batch_size_ = t;
  }
  inline uint32_t batch_size() { return batch_size_; }
private:
  // TODO: use batch interval
  uint32_t batch_interval_ = 100;
  uint32_t compact_interval_ = PAXOS_DEFAULT_COMPACT_INTERVAL;
  uint32_t connection_check_timeout_ = PAXOS_DEFAULT_CONNECTION_CHECK_TIMEOUT;
  uint32_t random_wait_low_ = PAXOS_DEFAULT_RANDOM_WAIT_LOW;
  uint32_t random_wait_high_ = PAXOS_DEFAULT_RANDOM_WAIT_HIGH;
  uint32_t batch_size_ = PAXOS_DEFAULT_BATCH_SIZE;
};


#endif
