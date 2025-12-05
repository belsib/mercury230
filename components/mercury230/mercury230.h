#pragma once

#include "esphome/core/component.h"
#include "esphome/components/uart/uart.h"
#include "esphome/components/sensor/sensor.h"

#ifdef USE_BINARY_SENSOR
#include "esphome/components/binary_sensor/binary_sensor.h"
#endif

#include <cstdint>
#include <string>
#include <memory>
#include <map>
#include <list>
#include <iomanip> 
#include <sstream>

#include "mercury230_uart.h"
#include "mercury230_sensor.h"
#include "object_locker.h"

namespace esphome {
namespace mercury230 {

static const size_t MAX_IN_BUF_SIZE = 256;
static const size_t MAX_OUT_BUF_SIZE = 84;

const uint8_t VAL_NUM = 12;
using ValueRefsArray = std::array<char *, VAL_NUM>;

using SensorMap = std::multimap<std::string, Mercury230SensorBase *>;
using SingleRequests = std::list<std::string>;

using FrameStopFunction = std::function<bool(uint8_t *buf, size_t size)>;
using ReadFunction = std::function<size_t()>;
using ProcessorFunction = std::function<bool()>;

enum class CEMeterModel : uint8_t {
  MODEL_UNKNOWN = 0,
  MODEL_230 = 1,
  MODEL_206 = 2,
  MODEL_COUNT,
};

class MercuryComponent : public PollingComponent, public uart::UARTDevice {
 public:
  MercuryComponent() : tag_(generateTag()){};

  void setup() override;
  void dump_config() override;
  void loop() override;
  void update() override;
  float get_setup_priority() const override { return setup_priority::DATA; };
  
  void set_meter_model(CEMeterModel model) { this->meter_model_ = model; }
  void set_meter_address(uint8_t addr) { this->meter_address_ = addr; };
  void set_password(uint32_t password) { this->password_ = password; }
  void set_sn(uint32_t sn) { this->sn_ = sn; }
  void set_auth_required(bool auth) { this->auth_required_ = auth; };
  void set_auth_level(bool level) { this->auth_level_ = level; };
  void set_baud_rates(uint32_t baud_rate_handshake, uint32_t baud_rate) {
    this->baud_rate_handshake_ = baud_rate_handshake;
    this->baud_rate_ = baud_rate;
  };
  void set_receive_timeout_ms(uint32_t timeout) { this->receive_timeout_ms_ = timeout; };
  void set_delay_between_requests_ms(uint32_t delay) { this->delay_between_requests_ms_ = delay; };

  void register_sensor(Mercury230SensorBase *sensor);
  void set_reboot_after_failure(uint16_t number_of_failures) { this->failures_before_reboot_ = number_of_failures; }

  void queue_single_read(const std::string &req);

#ifdef USE_BINARY_SENSOR
  SUB_BINARY_SENSOR(transmission)
  SUB_BINARY_SENSOR(session)
  SUB_BINARY_SENSOR(connection)
#endif

 protected:
  CEMeterModel meter_model_{CEMeterModel::MODEL_230};
  uint8_t meter_address_{0};
  uint32_t password_{0};
  uint32_t sn_{0};
  uint8_t cn_in_{0};
  bool auth_required_ {false};
  bool auth_level_ {false};
  uint32_t receive_timeout_ms_{500};
  uint32_t delay_between_requests_ms_{50};
  float value_response[VAL_NUM]{};
  uint8_t out_buffer_[MAX_OUT_BUF_SIZE];
  uint8_t out_buffer_size_ {0};
  ProcessorFunction RESPONSE_PROCESSOR[20];


  std::unique_ptr<Mercury230Uart> iuart_;

  SensorMap sensors_;
  SingleRequests single_requests_;

  sensor::Sensor *crc_errors_per_session_sensor_{};

  enum class State : uint8_t {
    NOT_INITIALIZED,
    IDLE,
    TRY_LOCK_BUS,
    WAIT,
    WAITING_FOR_RESPONSE,
    OPEN_SESSION,
    DATA_ENQ,
    DATA_RECV,
    DATA_NEXT,
    CLOSE_SESSION,
    PUBLISH,
  } state_{State::NOT_INITIALIZED};
  State last_reported_state_{State::NOT_INITIALIZED};

  struct {
    uint32_t start_time{0};
    uint32_t delay_ms{0};
    State next_state{State::IDLE};
  } wait_;

  bool is_idling() const { return this->state_ == State::WAIT || this->state_ == State::IDLE; };

  void set_next_state_(State next_state) { state_ = next_state; };
  void set_next_state_delayed_(uint32_t ms, State next_state);

  void read_reply_and_go_next_state_(ReadFunction read_fn, ReadFunction script_fn, State next_state, uint8_t retries, bool mission_critical,
                                     bool check_crc);
  struct {
    ReadFunction read_fn;
	ReadFunction script_fn;
    State next_state;
    bool mission_critical;
    bool check_crc;
    uint8_t tries_max;
    uint8_t tries_counter;
    uint32_t err_crc;
    uint32_t err_invalid_frames;
  } reading_state_{nullptr, nullptr, State::IDLE, false, false, 0, 0, 0, 0};
  size_t received_frame_size_{0};

  uint32_t baud_rate_handshake_{9600};
  uint32_t baud_rate_{9600};
  ReadFunction script_fn{nullptr};
  uint32_t last_rx_time_{0};
  uint8_t curr_reqest_{0};
  uint8_t string_reqest_[MAX_IN_BUF_SIZE];
  
  struct {
    uint8_t in[MAX_IN_BUF_SIZE];
    size_t amount_in;
    uint8_t out[MAX_OUT_BUF_SIZE];
	uint8_t out_crc[MAX_OUT_BUF_SIZE];
    size_t amount_out;
  } buffers_;

  void clear_rx_buffers_();

  void set_baud_rate_(uint32_t baud_rate);
  bool are_baud_rates_different_() const { return baud_rate_handshake_ != baud_rate_; }

  bool check_crc_prog_frame_(uint8_t *data, size_t length);

  void prepare_frame_(const uint8_t *data, size_t length);
  void prepare_prog_frame_(const char *request, const char *param, bool write = false);

  void send_frame_(const uint8_t *data, size_t length);
  void send_frame_prepared_();

  size_t receive_frame_(FrameStopFunction stop_fn);
  uint8_t receive_frame_count_(size_t count);

  inline void update_last_rx_time_() { this->last_rx_time_ = millis(); }
  bool check_wait_timeout_() { return millis() - wait_.start_time >= wait_.delay_ms; }
  bool check_rx_timeout_() { return millis() - this->last_rx_time_ >= receive_timeout_ms_; }
  void get_values_from_batch_data_(uint8_t *data, uint16_t size, float *value);
  void get_values_from_batch_data_206_(uint8_t *data, uint32_t size, float *value);
  float ParseHexDataWork(uint8_t *data, size_t len, uint8_t rate = 0, uint8_t flag = 0);
  bool set_sensor_value_(Mercury230SensorBase *sensor, ValueRefsArray &vals);
  std::string format_hex_pretty_(const unsigned char* data, size_t length, char separator = ' ', bool sep = false);

  void report_failure(bool failure);
  void abort_mission_();
  void process_voltage_frame();
  
  void indicate_transmission(bool transmission_on);
  void indicate_session(bool session_on);
  void indicate_connection(bool connection_on);

  ProcessorFunction get_v_i_p_processor();
  ProcessorFunction get_energy_206_processor();
  ProcessorFunction get_address_206_processor();
  ProcessorFunction get_address_320_processor();
  ProcessorFunction get_serial_number_320_processor();
  ProcessorFunction get_energy_320_processor();
  ProcessorFunction get_power_320_processor();
  ProcessorFunction get_current_320_processor();
  ProcessorFunction get_voltage_320_processor();
  ProcessorFunction check_session_320();
  ProcessorFunction get_tarif_206_processor();
  ProcessorFunction get_date_fabric_206_processor();


  const char *state_to_string(State state);
  void log_state_(State *next_state = nullptr);

  struct Stats {
    uint32_t connections_tried_{0};
    uint32_t crc_errors_{0};
    uint32_t crc_errors_recovered_{0};
    uint32_t invalid_frames_{0};
    uint8_t failures_{0};

    float crc_errors_per_session() const { return (float) crc_errors_ / connections_tried_; }
  } stats_;
  void stats_dump_();

  uint8_t failures_before_reboot_{0};
  
  struct LoopState {
    uint32_t session_started_ms{0};             // start of session
    SensorMap::iterator request_iter{nullptr};  // talking to meter
    SensorMap::iterator sensor_iter{nullptr};   // publishing sensor values
  } loop_state_;

  bool try_lock_uart_session_();
  void unlock_uart_session_();
  
  
  void create_request_(const uint8_t *param, uint8_t size, bool password = false);

 private:
  static uint8_t next_obj_id_;
  std::string tag_;

  static std::string generateTag();

  // Data structures for time synchronization
  char serial_number_str_[8]{};
  
	};
}  // namespace energomera_iec
}  // namespace esphome