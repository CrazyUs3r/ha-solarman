from __future__ import annotations

from typing import Any
from logging import getLogger

from homeassistant.core import HomeAssistant
from homeassistant.const import STATE_OFF, STATE_ON
from homeassistant.components.switch import SwitchEntity, SwitchDeviceClass, SwitchEntityDescription
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import *
from .common import *
from .services import *
from . import SolarmanConfigEntry
from .entity import SolarmanWritableEntity

_LOGGER = getLogger(__name__)

_PLATFORM = get_current_file_name(__name__)

async def async_setup_entry(_: HomeAssistant, config_entry: SolarmanConfigEntry, async_add_entities: AddEntitiesCallback) -> bool:
    _LOGGER.debug(f"async_setup_entry: {config_entry.options}")

    async_add_entities(SolarmanSwitchEntity(config_entry.runtime_data, d).init() for d in postprocess_descriptions(config_entry.runtime_data, _PLATFORM))

    return True

async def async_unload_entry(_: HomeAssistant, config_entry: SolarmanConfigEntry) -> bool:
    _LOGGER.debug(f"async_unload_entry: {config_entry.options}")

    return True

class SolarmanSwitchEntity(SolarmanWritableEntity, SwitchEntity):
    def __init__(self, coordinator, sensor):
        SolarmanWritableEntity.__init__(self, coordinator, sensor)
        self._attr_device_class = SwitchDeviceClass.SWITCH

        self._value_on = 1
        self._value_off = 0
        self._value_bit = None
        if "value" in sensor and (value := sensor["value"]) and not isinstance(value, int):
            if True in value:
                self._value_on = value[True]
            if "on" in value:
                self._value_on = value["on"]
            if False in value:
                self._value_off = value[False]
            if "off" in value:
                self._value_off = value["off"]
            if "bit" in value:
                self._value_bit = value["bit"]

    def _to_native_value(self, value: int) -> int:
        if self._value_bit is not None:
            return (self._get_attr_native_value & ~(1 << self._value_bit)) | (value << self._value_bit)
        return value

    @property
    def is_on(self) -> bool | None:
        """Return True if entity is on."""
        return (
            self._attr_native_value >> self._value_bit & 1
            if self._attr_native_value is not None and self._value_bit is not None
            else self._attr_native_value
        ) != self._value_off

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the entity on."""
        value = self._to_native_value(self._value_on)
        await self.write(value, value)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the entity off."""
        value = self._to_native_value(self._value_off)
        await self.write(value, value)
