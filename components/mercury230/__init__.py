import re
from esphome import pins
import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import uart, binary_sensor #,  time
from esphome.const import (
    CONF_ID,
    CONF_ADDRESS,
    CONF_AUTH,
    CONF_BAUD_RATE,
    CONF_RECEIVE_TIMEOUT,
    CONF_UPDATE_INTERVAL,
    CONF_PASSWORD,
)

CODEOWNERS = ["@belsib"]

AUTO_LOAD = ["binary_sensor"]

DEPENDENCIES = ["uart"]

MULTI_CONF = True

DEFAULTS_MAX_SENSOR_INDEX = 12
DEFAULTS_BAUD_RATE_HANDSHAKE = 9600
DEFAULTS_BAUD_RATE_SESSION = 9600
DEFAULTS_RECEIVE_TIMEOUT = "500ms"
DEFAULTS_DELAY_BETWEEN_REQUESTS = "50ms"
DEFAULTS_UPDATE_INTERVAL = "30s"

CONF_MERCURY230_ID = "mercury_id"
CONF_REQUEST = "request"
CONF_DELAY_BETWEEN_REQUESTS = "delay_between_requests"
CONF_SUB_INDEX = "sub_index"
CONF_MODEL = "model"
CONF_MASTER_MODE = "master_mode"
CONF_SERIAL_NUMBER = "serial"
CONF_INDICATOR = "indicator"
CONF_REBOOT_AFTER_FAILURE = "reboot_after_failure"

CONF_BAUD_RATE_HANDSHAKE = "baud_rate_handshake"

mercury230_ns = cg.esphome_ns.namespace("mercury230")
Mercury230 = mercury230_ns.class_(
    "MercuryComponent", cg.Component, uart.UARTDevice
)

BAUD_RATES = [300, 600, 1200, 2400, 4800, 9600, 19200]

CEMeterModel = mercury230_ns.enum("CEMeterModel", True)
METER_MODELS = {
    "UNKNOWN": CEMeterModel.MODEL_UNKNOWN,
    "MERCURY_230": CEMeterModel.MODEL_230,
    "MERCURY_206": CEMeterModel.MODEL_206,
}

def validate_request_format(value):
    if not value.endswith(")"):
        value += "()"

    pattern = r"\b[a-zA-Z_][a-zA-Z0-9_]*\s*\([^)]*\)$"
    if not re.match(pattern, value):
        raise cv.Invalid(
            "Invalid request format. Proper is 'REQUEST' or 'REQUEST()' or 'REQUEST(ARGS)'"
        )

    if len(value) > 25:
        raise cv.Invalid(
            "Request length must be no longer than 15 characters including ()"
        )
    return value


def validate_meter_address(value):
    if len(value) > 3:
        raise cv.Invalid("Meter address length must be no longer than 15 characters")
    return value


CONFIG_SCHEMA = cv.All(
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(Mercury230),
            
            cv.Optional(CONF_MODEL, default="MERCURY_320"): cv.enum(METER_MODELS, upper=True),
            cv.Optional(CONF_ADDRESS, default=0): cv.int_range(min=0, max=255),
            cv.Optional(CONF_PASSWORD, default=111111): cv.int_range(min=0x0, max=999999),
            cv.Optional(CONF_SERIAL_NUMBER, default=0): cv.int_range (min=0x0, max=0xffffffff),
            cv.Optional(CONF_AUTH, default=False): cv.boolean,
            cv.Optional(CONF_MASTER_MODE, default=False): cv.boolean,
            cv.Optional(CONF_BAUD_RATE_HANDSHAKE, default=DEFAULTS_BAUD_RATE_HANDSHAKE
            ): cv.one_of(*BAUD_RATES),
            cv.Optional(CONF_BAUD_RATE, default=DEFAULTS_BAUD_RATE_SESSION): cv.one_of(*BAUD_RATES),
            cv.Optional(CONF_RECEIVE_TIMEOUT, default=DEFAULTS_RECEIVE_TIMEOUT
            ): cv.positive_time_period_milliseconds,
            cv.Optional(CONF_DELAY_BETWEEN_REQUESTS, default=DEFAULTS_DELAY_BETWEEN_REQUESTS
            ): cv.positive_time_period_milliseconds,
            cv.Optional(CONF_UPDATE_INTERVAL, default=DEFAULTS_UPDATE_INTERVAL
            ): cv.update_interval,
            cv.Optional(CONF_REBOOT_AFTER_FAILURE, default=0): cv.int_range(
                min=0, max=100
            ),
        }
    )
    .extend(cv.COMPONENT_SCHEMA)
    .extend(uart.UART_DEVICE_SCHEMA)
)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)
    await uart.register_uart_device(var, config)

    if indicator_config := config.get(CONF_INDICATOR):
        sens = cg.new_Pvariable(indicator_config[CONF_ID])
        await binary_sensor.register_binary_sensor(sens, indicator_config)
        cg.add(var.set_indicator(sens))

       
    cg.add(var.set_meter_model(config[CONF_MODEL]))
    cg.add(var.set_meter_address(config[CONF_ADDRESS]))
    cg.add(var.set_auth_required(config[CONF_AUTH]))
    cg.add(var.set_auth_level(config[CONF_MASTER_MODE]))
    cg.add(var.set_password(config[CONF_PASSWORD]))
    cg.add(var.set_sn(config[CONF_SERIAL_NUMBER]))
    cg.add(var.set_baud_rates(config[CONF_BAUD_RATE_HANDSHAKE], config[CONF_BAUD_RATE]))
    cg.add(var.set_receive_timeout_ms(config[CONF_RECEIVE_TIMEOUT]))
    cg.add(var.set_delay_between_requests_ms(config[CONF_DELAY_BETWEEN_REQUESTS]))
    cg.add(var.set_update_interval(config[CONF_UPDATE_INTERVAL]))
    cg.add(var.set_reboot_after_failure(config[CONF_REBOOT_AFTER_FAILURE]))