#include "esphome/core/log.h"
#include "esphome/core/helpers.h"
#include "esphome/core/application.h"
#include "esphome/core/time.h"
#include "mercury230.h"


namespace esphome {
namespace mercury230 {

static const char *TAG0 = "mercury230_";
#define TAG (this->tag_.c_str())
#define CMD_SUPLY_320    		0
#define CMD_CURRENT_320  		1
#define CMD_POWER_320    		2
#define CMD_ENERGY_320   		3
#define CMD_SN_320       		4
#define CMD_ADDRESS_320  		5
#define CMD_ADDRESS_206  		6
#define CMD_ENERGY_206   		7
#define CMD_V_I_P_206    		8
#define CMD_OPEN_SESSION_320   	9
#define CMD_CLOSE_SESSION_320  10
#define CMD_TARIF_COUNT_206    11
#define CMD_TARIF_CURRENT_206  12
#define CMD_SERIAL_206         13
#define CMD_DATE_FABRIC_206    14



static const uint8_t RESPONSE_LENGTH[] =   {0x0C, //CMD_SUPLY_320
											0x0C, //CMD_CURRENT_320
											0x0F, //CMD_POWER_320
											0x13, //CMD_ENERGY_320
											0x0A, //CMD_SN_320
											0x05, //CMD_ADDRESS_320
											0x0B, //CMD_ADDRESS_206
											0x17, //CMD_ENERGY_206
											0x0E, //CMD_V_I_P_206
											0x04, //CMD_OPEN_SESSION_320
											0x04, //CMD_CLOSE_SESSION_320
											0x08, //CMD_TARIF_COUNT_206
											0x08, //CMD_TARIF_CURRENT_206
											0x0B, //CMD_SERIAL_206
											0x0A, //CMD_DATE_FABRIC_206
											};

static const uint8_t CMD_PARAM_GET[]   =   {1, //CMD_SUPLY_320
											1, //CMD_CURRENT_320
											2, //CMD_POWER_320
											1, //CMD_ENERGY_320
											0, //CMD_SN_320
											0, //CMD_ADDRESS_320
											0, //CMD_ADDRESS_206
											1, //CMD_ENERGY_206
											1, //CMD_V_I_P_206
											0, //CMD_OPEN_SESSION_320
											0, //CMD_CLOSE_SESSION_320
											0, //CMD_OPEN_SESSION_320
											0, //CMD_CLOSE_SESSION_320
											0, //CMD_SERIAL_206
											0, //CMD_DATE_FABRIC_206
											};

const std::unordered_map<uint8_t, std::vector<uint8_t>> CMD_CODES = {
															{CMD_SUPLY_320,   		{0x08, 0x16, 0x11}}, 
															{CMD_CURRENT_320, 		{0x08, 0x16, 0x21}},
															{CMD_POWER_320,   		{0x08, 0x16, 0x00}},
															{CMD_ENERGY_320,  		{0x05, 0x00, 0x00}},
															{CMD_SN_320,      		{0x08, 0x00}},
															{CMD_ADDRESS_320, 		{0x08, 0x05}},
															{CMD_ADDRESS_206, 		{0x20}},
															{CMD_ENERGY_206,  		{0x27}},
															{CMD_V_I_P_206,   		{0x63}},
															{CMD_OPEN_SESSION_320,  {0x01}},
															{CMD_TARIF_COUNT_206,   {0x2E}},
															{CMD_TARIF_CURRENT_206, {0x60}},
															{CMD_SERIAL_206,        {0x2F}},
															{CMD_CLOSE_SESSION_320, {0x02}},
															{CMD_DATE_FABRIC_206,   {0x66}}
};

const std::unordered_map<uint8_t, std::unordered_map<std::string, uint8_t>> CMD_PARAM = {
	{1,  {
			{"V",     1},
			{"A",     2},
			{"W",     3},
			{"T1",    1},
			{"T2",    2},
			{"T3",    3},
			{"T4",    3},
			{"A",     1},
			{"B",     2},
			{"C",     3},
			{"A+",    1},
			{"A-",    2},
			{"R+",    3},
			{"R-",    4},
			{"SUMM",  5},
		 }
	},
	{2,  {
			{"A",     2},
			{"B",     3},
			{"C",     4},
			{"SUMM",  1},
		 }
	}
};

const std::unordered_map<CEMeterModel, std::unordered_map<std::string, uint8_t>> CMD_NAME = {
	{CEMeterModel::MODEL_230,  {
									{"VOLTA",    CMD_SUPLY_320},
									{"CURR",     CMD_CURRENT_320},
									{"POWER",    CMD_POWER_320},
									{"ENER",     CMD_ENERGY_320},
									{"SERN",     CMD_SN_320},
									{"ADDR",     CMD_ADDRESS_320}
								}
	},
	{CEMeterModel::MODEL_206,  {
									{"VIP",    	 CMD_V_I_P_206},
									{"ENER",     CMD_ENERGY_206},
									{"SERN",     CMD_SN_320},
									{"ADDR",     CMD_ADDRESS_206},
									{"TARCNT",   CMD_TARIF_COUNT_206},
									{"TARCUR",   CMD_TARIF_CURRENT_206},
									{"SERIAL",   CMD_SERIAL_206},
									{"DTFAB",    CMD_DATE_FABRIC_206}
								}
	}
};	

static constexpr uint8_t BOOT_WAIT_S = 10;

uint8_t baud_rate_to_byte(uint32_t baud) {
  constexpr uint16_t BAUD_BASE = 300;
  constexpr uint8_t BAUD_MULT_MAX = 6;

  uint8_t idx = 0;  // 300
  for (size_t i = 0; i <= BAUD_MULT_MAX; i++) {
    if (baud == BAUD_BASE * (1 << i)) {
      idx = i;
      break;
    }
  }
  return idx + '0';
}

void MercuryComponent::set_baud_rate_(uint32_t baud_rate) {
  ESP_LOGV(TAG, "Setting baud rate %u bps", baud_rate);
  iuart_->update_baudrate(baud_rate);
}

void MercuryComponent::setup() {
  // ESP_LOGD(TAG, "setup");
  ESP_LOGI(TAG, "setup");
#ifdef USE_ESP32_FRAMEWORK_ARDUINO
  iuart_ = make_unique<Mercury230Uart>(*static_cast<uart::ESP32ArduinoUARTComponent *>(this->parent_));
#endif

#ifdef USE_ESP_IDF
  iuart_ = make_unique<Mercury230Uart>(*static_cast<uart::IDFUARTComponent *>(this->parent_));
#endif

#if USE_ESP8266
  iuart_ = make_unique<Mercury230Uart>(*static_cast<uart::ESP8266UartComponent *>(this->parent_));
#endif

  this->RESPONSE_PROCESSOR[CMD_V_I_P_206]    		= get_v_i_p_processor();
  this->RESPONSE_PROCESSOR[CMD_ENERGY_206]   		= get_energy_206_processor();
  this->RESPONSE_PROCESSOR[CMD_ADDRESS_206]  		= get_address_206_processor();
  this->RESPONSE_PROCESSOR[CMD_SN_320]       		= get_serial_number_320_processor();
  this->RESPONSE_PROCESSOR[CMD_ADDRESS_320]  		= get_address_320_processor();
  this->RESPONSE_PROCESSOR[CMD_POWER_320]    		= get_power_320_processor();
  this->RESPONSE_PROCESSOR[CMD_ENERGY_320]   		= get_energy_320_processor();
  this->RESPONSE_PROCESSOR[CMD_SUPLY_320]    		= get_voltage_320_processor();
  this->RESPONSE_PROCESSOR[CMD_CURRENT_320]  		= get_current_320_processor();
  this->RESPONSE_PROCESSOR[CMD_OPEN_SESSION_320]  	= check_session_320();
  this->RESPONSE_PROCESSOR[CMD_TARIF_COUNT_206]  	= get_tarif_206_processor();
  this->RESPONSE_PROCESSOR[CMD_TARIF_CURRENT_206]  	= get_tarif_206_processor();
  this->RESPONSE_PROCESSOR[CMD_SERIAL_206]  		= get_address_206_processor();
  this->RESPONSE_PROCESSOR[CMD_DATE_FABRIC_206]  	= get_date_fabric_206_processor();
  this->indicate_transmission(false);
  this->set_baud_rate_(this->baud_rate_handshake_);
  this->set_timeout(BOOT_WAIT_S * 1000, [this]() {
  ESP_LOGD(TAG, "Boot timeout, component is ready to use");
  this->clear_rx_buffers_();
  this->set_next_state_(State::IDLE);
  });
}

void MercuryComponent::dump_config() {
  ESP_LOGCONFIG(TAG, "Mercury: %p", this);

  LOG_UPDATE_INTERVAL(this);
  ESP_LOGCONFIG(TAG, "  Receive Timeout: %ums", this->receive_timeout_ms_);
  ESP_LOGCONFIG(TAG, "  Supported Meter Types: 230/231/232/233");
  ESP_LOGCONFIG(TAG, "  Sensors:");
  for (const auto &sensors : sensors_) {
    auto &s = sensors.second;
    ESP_LOGCONFIG(TAG, "    REQUEST: %s", s->get_request().c_str());
  }
}

void MercuryComponent::register_sensor(Mercury230SensorBase *sensor) {
  this->sensors_.insert({sensor->get_request(), sensor});
}

void MercuryComponent::abort_mission_() {
  // try close connection ?
  ESP_LOGE(TAG, "Abort mission. Closing session");
  if (this->meter_model_ == CEMeterModel::MODEL_230) { 
	this->create_request_(CMD_CODES.at(CMD_CLOSE_SESSION_320).data(), 
							CMD_CODES.at(CMD_CLOSE_SESSION_320).size());
	auto read_fn = [this]() { return this->receive_frame_count_(0); };
	this->send_frame_(this->out_buffer_, this->out_buffer_size_ + 1);
  }
  this->unlock_uart_session_();
  this->set_next_state_(State::IDLE);
  this->report_failure(true);
}

void MercuryComponent::report_failure(bool failure) {
  if (!failure) {
    this->stats_.failures_ = 0;
    return;
  }

  this->stats_.failures_++;
  if (this->failures_before_reboot_ > 0 && this->stats_.failures_ > this->failures_before_reboot_) {
    ESP_LOGE(TAG, "Too many failures in a row. Let's try rebooting device.");
    delay(100);
    App.safe_reboot();
  }
}

void MercuryComponent::loop() {
  if (!this->is_ready() || this->state_ == State::NOT_INITIALIZED)
    return;

  ValueRefsArray vals;                                  // values from brackets, refs to this->buffers_.in
  char *in_param_ptr = (char *) &this->buffers_.in[1];  // ref to second byte, first is STX/SOH in R1 requests
  std::vector<uint8_t> req_access;

  switch (this->state_) {
    case State::IDLE: {// ------------------------------------------------------------------------------------------IDLE
      this->update_last_rx_time_();
      this->indicate_transmission(false);
      this->indicate_session(false);
    } break;

    case State::TRY_LOCK_BUS: {// ----------------------------------------------------------------------------------TRY_LOCK_BUS
      this->log_state_();
      if (this->try_lock_uart_session_()) {
        this->indicate_session(true);
        this->indicate_connection(true);
        this->set_next_state_(State::OPEN_SESSION);
      } else {
        ESP_LOGV(TAG, "UART Bus is busy, waiting ...");
        this->set_next_state_delayed_(1000, State::TRY_LOCK_BUS);
      }
    } break;

    case State::WAIT: 
      if (this->check_wait_timeout_()) {
        this->set_next_state_(this->wait_.next_state);
        this->update_last_rx_time_();
      }
      break;

    case State::WAITING_FOR_RESPONSE: { // -------------------------------------------------------------------------WAITING_FOR_RESPONSE
      this->log_state_(&reading_state_.next_state);
      received_frame_size_ = reading_state_.read_fn();
      bool crc_is_ok = true;
      if (reading_state_.check_crc && received_frame_size_ > 0) {
        crc_is_ok = check_crc_prog_frame_(this->buffers_.in, received_frame_size_);
      }
      // happy path first
      if (received_frame_size_ > 0 && crc_is_ok) {
        this->set_next_state_(reading_state_.next_state);
        this->update_last_rx_time_();
        this->stats_.crc_errors_ += reading_state_.err_crc;
        this->stats_.crc_errors_recovered_ += reading_state_.err_crc;
        this->stats_.invalid_frames_ += reading_state_.err_invalid_frames;
		ESP_LOGI(TAG, "RX: %s", format_hex_pretty(this->buffers_.in, received_frame_size_).c_str());
		reading_state_.script_fn();
        return;
      }
      // half-happy path
      // if not timed out yet, wait for data to come a little more
      if (crc_is_ok && !this->check_rx_timeout_()) {
        return;
      }
      this->indicate_connection(false);
      this->indicate_transmission(false);

      if (received_frame_size_ == 0) {
        this->reading_state_.err_invalid_frames++;
        ESP_LOGW(TAG, "RX timeout.");
      } else if (!crc_is_ok) {
        this->reading_state_.err_crc++;
        ESP_LOGW(TAG, "Frame received, but CRC failed.");
      } else {
        this->reading_state_.err_invalid_frames++;
        ESP_LOGW(TAG, "Frame corrupted.");
      }

      // if we are here, we have a timeout and no data
      // it means we have a failure
      // - either no reply from the meter at all
      // - or corrupted data and id doesn't trigger stop function
      if (this->buffers_.amount_in > 0) {
        // most likely its CRC error in STX/SOH/ETX. unclear.
        this->stats_.crc_errors_++;
        ESP_LOGV(TAG, "RX: %s", format_frame_pretty(this->buffers_.in, this->buffers_.amount_in).c_str());
        ESP_LOGVV(TAG, "RX: %s", format_hex_pretty(this->buffers_.in, this->buffers_.amount_in).c_str());
      }
      this->clear_rx_buffers_();

      if (reading_state_.mission_critical) {
        this->stats_.crc_errors_ += reading_state_.err_crc;
        this->stats_.invalid_frames_ += reading_state_.err_invalid_frames;
        this->abort_mission_();
        return;
      }

      if (reading_state_.tries_counter < reading_state_.tries_max) {
        reading_state_.tries_counter++;
        ESP_LOGW(TAG, "Retrying [%d/%d]...", reading_state_.tries_counter, reading_state_.tries_max);
        this->send_frame_prepared_();
        this->update_last_rx_time_();
        return;
      }
      received_frame_size_ = 0;
      // failure, advancing to next state with no data received (frame_size = 0)
      this->stats_.crc_errors_ += reading_state_.err_crc;
      this->stats_.invalid_frames_ += reading_state_.err_invalid_frames;
      this->set_next_state_(reading_state_.next_state);
    } break;

    case State::OPEN_SESSION: { // ----------------------------------------------------------------------------------OPEN_SESSION
      this->stats_.connections_tried_++;
      this->loop_state_.session_started_ms = millis();
      this->log_state_();

      this->clear_rx_buffers_();
      if (this->are_baud_rates_different_()) {
        this->set_baud_rate_(this->baud_rate_handshake_);
        delay(5);
      }

      this->loop_state_.request_iter = this->sensors_.begin();
      this->set_next_state_(State::DATA_ENQ);

      // mission crit, no crc
      if (this->meter_model_ == CEMeterModel::MODEL_230) { 
        req_access = CMD_CODES.at(CMD_OPEN_SESSION_320);
        req_access.push_back(this->auth_level_ + 1);
        this->create_request_(req_access.data(), req_access.size(),	true);
        auto read_fn = [this]() { return this->receive_frame_count_(0); };
        this->send_frame_(this->out_buffer_, this->out_buffer_size_ + 1);
        this->cn_in_ = RESPONSE_LENGTH[CMD_OPEN_SESSION_320];
        this->script_fn = RESPONSE_PROCESSOR[CMD_OPEN_SESSION_320];
        this->read_reply_and_go_next_state_(read_fn, script_fn, State::DATA_ENQ, 0, true, true);
      }
    } break;

    case State::DATA_ENQ: {      
      this->log_state_();
      if (this->loop_state_.request_iter == this->sensors_.end()) {
        ESP_LOGD(TAG, "All requests done");
        if (this->meter_model_ == CEMeterModel::MODEL_230) { 
          this->set_next_state_(State::CLOSE_SESSION);
        }
        break;
      } else {
        auto req = this->loop_state_.request_iter->second->get_function();
        auto par = this->loop_state_.request_iter->second->get_param();
        ESP_LOGD(TAG, "Requesting data for '%s'", req.c_str());
        this->set_next_state_(State::IDLE);
        this->prepare_prog_frame_(req.c_str(), par.c_str());
        this->send_frame_prepared_();
        auto read_fn = [this]() { return this->receive_frame_count_(0); };
        this->read_reply_and_go_next_state_(read_fn, this->script_fn, State::DATA_RECV, 3, false, true);
      }
	} break;

    case State::DATA_RECV: {  // --------------------------------------------------------------------------------DATA_RECV
      this->log_state_();
      this->set_next_state_(State::DATA_NEXT);

      if (received_frame_size_ == 0) {
        ESP_LOGD(TAG, "Response not received or corrupted. Next.");
        this->update_last_rx_time_();
        this->clear_rx_buffers_();
        return;
      }

      auto req = this->loop_state_.request_iter->first;

      auto range = sensors_.equal_range(req);
      for (auto it = range.first; it != range.second; ++it) {
        if (!it->second->is_failed())
          set_sensor_value_(it->second, vals);
      }
    } break;

    case State::DATA_NEXT: {  // ----------------------------------------------------------------------------------DATA_NEXT
      this->log_state_();
      this->loop_state_.request_iter = this->sensors_.upper_bound(this->loop_state_.request_iter->first);
      if (this->loop_state_.request_iter != this->sensors_.end()) {
        this->set_next_state_delayed_(this->delay_between_requests_ms_, State::DATA_ENQ);
      } else {
        this->set_next_state_delayed_(this->delay_between_requests_ms_, State::CLOSE_SESSION);
      }
	} break;

    case State::CLOSE_SESSION: {  // ------------------------------------------------------------------------------CLOSE_SESSION
      this->log_state_();
      ESP_LOGD(TAG, "Closing session");
      if (this->meter_model_ == CEMeterModel::MODEL_230) { 
        this->create_request_(CMD_CODES.at(CMD_CLOSE_SESSION_320).data(), 
                              CMD_CODES.at(CMD_CLOSE_SESSION_320).size());
        auto read_fn = [this]() { return this->receive_frame_count_(0); };
        this->send_frame_(this->out_buffer_, this->out_buffer_size_ + 1);
      }
      this->set_next_state_(State::PUBLISH);
      ESP_LOGD(TAG, "Total connection time: %u ms", millis() - this->loop_state_.session_started_ms);
      this->loop_state_.sensor_iter = this->sensors_.begin();
	} break;

    case State::PUBLISH: {  // ------------------------------------------------------------------------------------PUBLISH
      this->log_state_();
      ESP_LOGD(TAG, "Publishing data");
      this->update_last_rx_time_();

      if (this->loop_state_.sensor_iter != this->sensors_.end()) {
        this->loop_state_.sensor_iter->second->publish();
        this->loop_state_.sensor_iter++;
      } else {
        this->stats_dump_();
        if (this->crc_errors_per_session_sensor_ != nullptr) {
          this->crc_errors_per_session_sensor_->publish_state(this->stats_.crc_errors_per_session());
        }
        this->report_failure(false);
        this->unlock_uart_session_();
        this->set_next_state_(State::IDLE);
      }
	} break;

    default:
      break;
  }
}

void MercuryComponent::update() {
  if (this->state_ != State::IDLE) {
    ESP_LOGD(TAG, "Starting data collection impossible - component not ready");
    return;
  }
  // ESP_LOGD(TAG, "Starting data collection");
  this->set_next_state_(State::TRY_LOCK_BUS);

}

uint8_t get_id_data(uint8_t command, const std::string& param) {
	uint8_t id = CMD_PARAM_GET[command];
    if (CMD_PARAM.count(id) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестнвый код %u", id);
        return 0xFF; // Error code
    }
    const auto& inner_map = CMD_PARAM.at(id); 
    if (inner_map.count(param) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестный параметр %s", param.c_str());
        return 0xFF; // Error code
    }
    return inner_map.at(param);
}

uint8_t get_command_code_for_model(CEMeterModel model, const std::string& command_name) {
    if (CMD_NAME.count(model) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестная модель %d", static_cast<int>(model));
        return 0xFF; // Error code
    }
    const auto& inner_map = CMD_NAME.at(model); 
    if (inner_map.count(command_name) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестная команда %s", command_name.c_str());
        return 0xFF; // Error code
    }
    return inner_map.at(command_name);
}

uint8_t get_param_code(uint8_t command, const std::string& param) {
    if (CMD_PARAM.count(command) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестная команда %u", command);
        return 0; // Error code
    }
    const auto& inner_map = CMD_PARAM.at(command); 
    if (inner_map.count(param) == 0) {
        ESP_LOGE("Mercury230", "Ошибка: Неизвестный параметр %s", param.c_str());
        return 0; // Error code
    }
    return inner_map.at(param);
}

bool MercuryComponent::set_sensor_value_(Mercury230SensorBase *sensor, ValueRefsArray &vals) {
  auto type = sensor->get_type();
  bool ret = true;
  uint8_t idx{0};
  uint8_t command;
  auto param = sensor->get_param();
  if (!param.empty()) {
    auto request = sensor->get_function();
    command = get_command_code_for_model(this->meter_model_, request.c_str());
    if (CMD_PARAM_GET[command]) {
      idx = get_param_code(CMD_PARAM_GET[command], param) - 1;
    }
  }
  // idx = sensor->get_index() - 1;
  // ESP_LOGI(TAG, "Index %u - %u", idx, index_param);
  
  if (idx >= VAL_NUM) {
    ESP_LOGE(TAG, "Invalid sensor index %u", idx);
    return false;
  }

  if (type == SensorType::SENSOR) {
	static_cast<Mercury230Sensor *>(sensor)->set_value(this->value_response[idx]);
  } else {
#ifdef USE_TEXT_SENSOR
	static_cast<Mercury230TextSensor *>(sensor)->set_value((const char*)this->string_reqest_);
#endif
  }
  return ret;
}

bool MercuryComponent::check_crc_prog_frame_(uint8_t *data, size_t length) {
  uint16_t с_crc = crc16(data, length - 2);
  uint16_t r_crc = (data[length - 1] << 8) | data[length - 2];
  
  // ESP_LOGI(TAG, "CRC: %s -> %s", format_hex_pretty(с_crc, 2).c_str(), format_hex_pretty(r_crc, 2).c_str());
  return с_crc == r_crc;
}

void MercuryComponent::set_next_state_delayed_(uint32_t ms, State next_state) {
  if (ms == 0) {
    set_next_state_(next_state);
  } else {
    ESP_LOGV(TAG, "Short delay for %u ms", ms);
    set_next_state_(State::WAIT);
    wait_.start_time = millis();
    wait_.delay_ms = ms;
    wait_.next_state = next_state;
  }
}

void MercuryComponent::read_reply_and_go_next_state_( ReadFunction read_fn, ReadFunction script_fn, 
                                                      State next_state, uint8_t retries,
                                                      bool mission_critical, bool check_crc) {
  reading_state_ = {};
  reading_state_.read_fn = read_fn;
  reading_state_.script_fn = script_fn;
  reading_state_.mission_critical = mission_critical;
  reading_state_.tries_max = retries;
  reading_state_.tries_counter = 0;
  reading_state_.check_crc = check_crc;
  reading_state_.next_state = next_state;
  received_frame_size_ = 0;
  set_next_state_(State::WAITING_FOR_RESPONSE);
}

std::string MercuryComponent::format_hex_pretty_(const unsigned char* data, size_t length, char separator, bool sep) {
    std::stringstream ss;
    ss << std::uppercase
       << std::hex
       << std::setfill('0');
    for (size_t i = 0; i < length; ++i) {
        ss << std::setw(2) << static_cast<int>(data[i]);
        // Добавляем пробел, если это не последний элемент
		if (sep && (i < length - 1)) {
            ss << separator;
        }
    }
    return ss.str();
}

bool char2long(const char *str, long &value) {
  char *end;
  value = strtol(str, &end, 10);
  return *end == '\0';
}

float MercuryComponent::ParseHexDataWork(uint8_t *data, size_t len, uint8_t rate, uint8_t flag) {
	uint32_t DT_{0};
	uint8_t direction{0};
    const size_t MIN_LENGTH = 3;
    const size_t MAX_LENGTH = 4;

    if (data == nullptr || len < MIN_LENGTH || len > MAX_LENGTH) {
        return 0.0f; 
    }
	if (flag) {
		uint8_t direction_bit = (data[len - 3] & 0xC0) >> 6;
		data[0] &= 0x3F;
		if (flag == direction_bit) {
			direction = 1;
		} else {
			direction = 0;
		}
	}
	memcpy(&DT_, data, len);
	if (len < 4) { 
		DT_ = ((DT_ & 0x00ffff00) << 8) | (DT_ & 0x000000ff);
	}
	DT_ = ((DT_ & 0x0000ffff) << 16)|((DT_ & 0xffff0000) >> 16);
	float result = (float)DT_ / ((rate)?pow(10.0f, rate):1.0f);
	if (direction) {
		result *= -1.0f;
	}
	// ESP_LOGI(TAG, "VOLTA()        : %s - %f", format_hex_pretty(data, len).c_str(), 
												        // result);
	return result;
}

void MercuryComponent::get_values_from_batch_data_(uint8_t *data, uint16_t size, float *value) {
	data += 1;
	uint8_t count_item = (size & 0x0F00) >> 8;
	uint8_t size_item = (size & 0x00F0) >> 4;
	uint8_t rate = (size & 0x000F) >> 2;
	uint8_t flag = (size & 0x000F) & 0b00000011;
	for (size_t i = 0; i < count_item; i++) {
		value[i] = this->ParseHexDataWork(data, size_item, rate, flag);
		data += size_item;
	}
	ESP_LOGI(TAG, "Value: %.02f, %.02f, %.02f, %.02f", value[0], value[1], value[2], value[3]);
}

uint get_size_item(uint32_t flag, uint8_t item) {
	uint8_t move = 3 + (item * 5) + 2;
	uint32_t mask = 7 << move;
	flag &= mask;
	return (flag >> move);
}

uint get_rate_item(uint32_t flag, uint8_t item) {
	uint8_t move = 3 + (item * 5);
	uint32_t mask = 3 << move;
	flag &= mask;
	return (flag >> move);
}

void MercuryComponent::get_values_from_batch_data_206_(uint8_t *data, uint32_t size, float *value) {
	data += 5;
	uint8_t count_item = (size & 0x0000000F) & 0b00000111;
	bool ret = true;
	long DT_ = 0;
	char *str;
	char str_buffer[9];
	
	for (size_t i = 0; i < count_item; i++) {
		memset(str_buffer, '\0', sizeof(str_buffer));
		strncpy(str_buffer, format_hex_pretty_(data, get_size_item(size, i)).c_str(), 9);	
		str	= str_buffer;
		ret = str && str[0] && char2long(str, DT_);
		if (ret) {
			this->value_response[i] = (float)DT_ / pow(10.0f, get_rate_item(size, i));
		}		
		data += get_size_item(size, i);
	}
	ESP_LOGI(TAG, "Value: %.02f, %.02f, %.02f, %.02f", value[0], value[1], value[2], value[3]);
}

void add_request_(uint8_t *buffer, uint8_t *data, uint8_t *length, uint8_t size) {
	size_t len = *length;
	memcpy(buffer + len, data, size);
	*length += size;
}

void add_request_(uint8_t *buffer, const uint8_t *data, uint8_t *length, uint8_t size) {
	size_t len = *length;
	memcpy(buffer + len, data, size);
	*length += size;
}

void parse_pass_(uint8_t *buffer, uint32_t password) {
	uint8_t ind {0};
	for (size_t i = 0; i <= 5; i++) {
		ind = 5 - i;
		buffer[ind] = password % 10;
		password /= 10;
	}
}

uint32_t reverse_byte(const uint32_t &data) {
	return  ((data >> 24) & 0xFF) | 
          ((data >> 8) & 0xFF00) | 
          ((data << 8) & 0xFF0000) | 
          ((data << 24) & 0xFF000000);
}

void MercuryComponent::create_request_(const uint8_t *param, uint8_t size, bool password) {
	uint8_t pass[6] = {0};
	this->out_buffer_size_ = 0;
	uint32_t reversed_sn = reverse_byte(this->sn_); 
	switch (this->meter_model_) {
        case CEMeterModel::MODEL_230:
		  add_request_(this->out_buffer_, &this->meter_address_, &this->out_buffer_size_, sizeof(this->meter_address_));
		  break;
        case CEMeterModel::MODEL_206:
          add_request_(this->out_buffer_, (uint8_t*)&reversed_sn, &this->out_buffer_size_, 4);
          break;
        default:
          ESP_LOGW(TAG, "Unsupported meter model for tariff reading");
          break;
	}
    add_request_(this->out_buffer_, param, &this->out_buffer_size_, size);
	if (password) {
		parse_pass_(pass, this->password_);
		add_request_(this->out_buffer_, pass, &this->out_buffer_size_, 6);
	}
}

void MercuryComponent::prepare_prog_frame_(const char *request, const char *param, bool write) {
  // we assume request has format "XXXX(params)"
  // we assume it always has brackets
  ESP_LOGI(TAG, "-------------------------------------------------------------");
  ESP_LOGI(TAG, "request: %s, param: %s", request, param);
	uint8_t command = get_command_code_for_model(this->meter_model_, request);
	if (command == 0xFF) {
		command = CMD_ADDRESS_206;
    }
	this->create_request_(CMD_CODES.at(command).data(), CMD_CODES.at(command).size());
	this->prepare_frame_(this->out_buffer_, this->out_buffer_size_ + 1); //0x0E
	this->cn_in_ = RESPONSE_LENGTH[command];
	this->script_fn = RESPONSE_PROCESSOR[command];
}

ProcessorFunction MercuryComponent::get_voltage_320_processor() {
	return [this]() { 
		this->get_values_from_batch_data_(this->buffers_.in, 0x0338, this->value_response);
			return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_current_320_processor() {
	return [this]() { 
		this->get_values_from_batch_data_(this->buffers_.in, 0x033C, this->value_response);
			return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_power_320_processor() {
	return [this]() { 
	    this->get_values_from_batch_data_(this->buffers_.in, 0x043A, this->value_response);
			return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_energy_320_processor() {
	return [this]() { 
	    this->get_values_from_batch_data_(this->buffers_.in, 0x044C, this->value_response);
		return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_serial_number_320_processor() {
	return [this]() { 
	    uint8_t open_cmd_len = snprintf((char *) this->string_reqest_, MAX_OUT_BUF_SIZE,
                                     		"%02u%02u%02u%02u", this->buffers_.in[1], 
		                                                        this->buffers_.in[2], 
		                                                        this->buffers_.in[3], 
		                                                        this->buffers_.in[4]);
		return  0;
	  };
}

ProcessorFunction MercuryComponent::get_address_320_processor() {
	return [this]() { 
		    this->meter_address_ = this->buffers_.in[2];
			uint8_t open_cmd_len = snprintf((char *) this->string_reqest_, MAX_OUT_BUF_SIZE,
                                     		"%03u", this->buffers_.in[2]);
			return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_address_206_processor() {
	return [this]() { 
			uint32_t serial_number = 	(this->buffers_.in[5] << 24) + 
										(this->buffers_.in[6] << 16) + 
										(this->buffers_.in[7] << 8) +
										 this->buffers_.in[8];
			uint8_t open_cmd_len = snprintf((char *) this->string_reqest_, MAX_OUT_BUF_SIZE,
                                     		"%u", serial_number);
			ESP_LOGI(TAG, "Value: %s", this->string_reqest_);
			return true; 
		  };
}

ProcessorFunction MercuryComponent::get_date_fabric_206_processor() {
	return [this]() { 
	
			
			uint8_t *data = this->buffers_.in;
			data += 5;
			strncpy(reinterpret_cast<char*>(this->string_reqest_), // Destination
										format_hex_pretty_(data, 3, '.', true).c_str(),                      // Source (const char*)
										sizeof(this->string_reqest_) - 1); 
			// uint8_t open_cmd_len = snprintf((char *) this->string_reqest_, MAX_OUT_BUF_SIZE,
                                     		// "%02u.%02u.%02u", 	this->buffers_.in[5],
																// this->buffers_.in[6],
																// this->buffers_.in[7]);
			ESP_LOGI(TAG, "Value: %s", this->string_reqest_);
			return true; 
		  };
}

ProcessorFunction MercuryComponent::get_tarif_206_processor() {
	return [this]() { 
			uint8_t open_cmd_len = snprintf((char *) this->string_reqest_, MAX_OUT_BUF_SIZE,
                                     		"%u", this->buffers_.in[5]);
			return 0; 
		  };
}

ProcessorFunction MercuryComponent::get_energy_206_processor() {
	return [this]() { 
	        this->get_values_from_batch_data_206_(this->buffers_.in, 0x4A5294, this->value_response);
			value_response[4] = value_response[3] + value_response[2] + value_response[1] + value_response[0];
			return true; 
		  };
}

ProcessorFunction MercuryComponent::get_v_i_p_processor() {
	return [this]() { 
		    this->get_values_from_batch_data_206_(this->buffers_.in, 0x018A4B, this->value_response);
			return true; 
		  };
}

ProcessorFunction MercuryComponent::check_session_320() {
	return [this]() { 
			return true; 
		  };
}

void MercuryComponent::send_frame_prepared_() {
  this->indicate_transmission(true);
  this->write_array(this->buffers_.out_crc, this->buffers_.amount_out+2);
  this->flush();
  ESP_LOGI(TAG, "TX: %s", format_hex_pretty(this->buffers_.out_crc, this->buffers_.amount_out+2).c_str());
}

void MercuryComponent::prepare_frame_(const uint8_t *data, size_t length) {
  memcpy(this->buffers_.out, data, length-1);
  this->buffers_.amount_out = length - 1;
  uint16_t crc = crc16(this->buffers_.out, this->buffers_.amount_out);
  uint8_t out_crc[MAX_OUT_BUF_SIZE];
  memcpy(out_crc, data, length - 1);
  out_crc[this->buffers_.amount_out] = crc & 0xFF;
  out_crc[this->buffers_.amount_out + 1] = (crc >> 8) & 0xFF;
  memcpy(this->buffers_.out_crc, out_crc, this->buffers_.amount_out + 2);
}

void MercuryComponent::send_frame_(const uint8_t *data, size_t length) {
  this->prepare_frame_(data, length);
  this->send_frame_prepared_();
}

size_t MercuryComponent::receive_frame_(FrameStopFunction stop_fn) {
  const uint32_t read_time_limit_ms = 25;
  size_t ret_val;

  auto count = this->available();
  if (count <= 0)
    return 0;
  
  uint32_t read_start = millis();
  uint8_t *p;
  while (count-- > 0) {
    if (millis() - read_start > read_time_limit_ms) {
      return 0;
    }

    if (this->buffers_.amount_in < MAX_IN_BUF_SIZE) {
      p = &this->buffers_.in[this->buffers_.amount_in];
      if (!iuart_->read_one_byte(p)) {
        return 0;
      }
      this->buffers_.amount_in++;
    } else {
      memmove(this->buffers_.in, this->buffers_.in + 1, this->buffers_.amount_in - 1);
      p = &this->buffers_.in[this->buffers_.amount_in - 1];
      if (!iuart_->read_one_byte(p)) {
        return 0;
      }
    }

    if (stop_fn(this->buffers_.in, this->buffers_.amount_in)) {
	  ret_val = this->buffers_.amount_in;
	  received_frame_size_ = this->buffers_.amount_in;
      this->buffers_.amount_in = 0;
      this->update_last_rx_time_();
	  return ret_val;
    }
	// if (this->meter_model_ == CEMeterModel::MODEL_230) { 
		// ESP_LOGI(TAG, "RX: %s", format_hex_pretty(this->buffers_.in, received_frame_size_).c_str());
	// }
    yield();
    App.feed_wdt();
  }
  return 0;
}

uint8_t MercuryComponent::receive_frame_count_(size_t count) {
  // "data<CR><LF>"
  if (count > 0) {
	this->cn_in_ = count;  
  }
  
  auto frame_end_check_crlf = [this](uint8_t *b, size_t s) {
	auto ret = s >= this->cn_in_;
    if (ret) {
      ESP_LOGVV(TAG, "Frame CRLF Stop");
    }
    return ret;
  };
  return receive_frame_(frame_end_check_crlf);
}

void MercuryComponent::clear_rx_buffers_() {
  int available = this->available();
  if (available > 0) {
    ESP_LOGVV(TAG, "Cleaning garbage from UART input buffer: %d bytes", available);
  }

  int len;
  while (available > 0) {
    len = std::min(available, (int) MAX_IN_BUF_SIZE);
    this->read_array(this->buffers_.in, len);
    available -= len;
  }
  memset(this->buffers_.in, 0, MAX_IN_BUF_SIZE);
  this->buffers_.amount_in = 0;
}

const char *MercuryComponent::state_to_string(State state) {
  switch (state) {
    case State::NOT_INITIALIZED:
      return "NOT_INITIALIZED";
    case State::IDLE:
      return "IDLE";
    case State::TRY_LOCK_BUS:
      return "TRY_LOCK_BUS";
    case State::WAIT:
      return "WAIT";
    case State::WAITING_FOR_RESPONSE:
      return "WAITING_FOR_RESPONSE";
    case State::OPEN_SESSION:
      return "OPEN_SESSION";
    case State::DATA_ENQ:
      return "DATA_ENQ";
    case State::DATA_RECV:
      return "DATA_RECV";
    case State::DATA_NEXT:
      return "DATA_NEXT";
    case State::CLOSE_SESSION:
      return "CLOSE_SESSION";
    case State::PUBLISH:
      return "PUBLISH";
    default:
      return "UNKNOWN";
  }
}

void MercuryComponent::log_state_(State *next_state) {
  if (this->state_ != this->last_reported_state_) {
    if (next_state == nullptr) {
      ESP_LOGV(TAG, "State::%s", this->state_to_string(this->state_));
    } else {
      ESP_LOGV(TAG, "State::%s -> %s", this->state_to_string(this->state_), this->state_to_string(*next_state));
    }
    this->last_reported_state_ = this->state_;
  }
}

void MercuryComponent::stats_dump_() {
  ESP_LOGV(TAG, "============================================");
  ESP_LOGV(TAG, "Data collection and publishing finished.");
  ESP_LOGV(TAG, "Total number of sessions ............. %u", this->stats_.connections_tried_);
  ESP_LOGV(TAG, "Total number of invalid frames ....... %u", this->stats_.invalid_frames_);
  ESP_LOGV(TAG, "Total number of CRC errors ........... %u", this->stats_.crc_errors_);
  ESP_LOGV(TAG, "Total number of CRC errors recovered . %u", this->stats_.crc_errors_recovered_);
  ESP_LOGV(TAG, "CRC errors per session ............... %f", this->stats_.crc_errors_per_session());
  ESP_LOGV(TAG, "Number of failures ................... %u", this->stats_.failures_);
  ESP_LOGV(TAG, "============================================");
}

bool MercuryComponent::try_lock_uart_session_() {
  if (AnyObjectLocker::try_lock(this->parent_)) {
    ESP_LOGVV(TAG, "UART bus %p locked by %s", this->parent_, this->tag_.c_str());
    return true;
  }
  ESP_LOGVV(TAG, "UART bus %p busy", this->parent_);
  return false;
}

void MercuryComponent::unlock_uart_session_() {
  AnyObjectLocker::unlock(this->parent_);
  ESP_LOGVV(TAG, "UART bus %p released by %s", this->parent_, this->tag_.c_str());
}

void MercuryComponent::indicate_transmission(bool transmission_on) {
#ifdef USE_BINARY_SENSOR
  if (this->transmission_binary_sensor_) {
    this->transmission_binary_sensor_->publish_state(transmission_on);
  }
#endif
}

void MercuryComponent::indicate_session(bool session_on) {
#ifdef USE_BINARY_SENSOR
  if (this->session_binary_sensor_) {
    this->session_binary_sensor_->publish_state(session_on);
  }
#endif
}

void MercuryComponent::indicate_connection(bool connection_on) {
#ifdef USE_BINARY_SENSOR
  if (this->connection_binary_sensor_) {
    this->connection_binary_sensor_->publish_state(connection_on);
  }
#endif
}

uint8_t MercuryComponent::next_obj_id_ = 0;

std::string MercuryComponent::generateTag() { return str_sprintf("%s%03d", TAG0, ++next_obj_id_); }

}  // namespace energomera_iec
}  // namespace esphome

