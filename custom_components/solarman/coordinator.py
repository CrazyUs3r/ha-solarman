from __future__ import annotations

import logging

from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import *
from .device import Device

_LOGGER = logging.getLogger(__name__)

class Coordinator(DataUpdateCoordinator[dict[str, Any]]):
    def __init__(self, hass: HomeAssistant, device: Device):
        super().__init__(hass, _LOGGER, name = device.config.name, update_interval = TIMINGS_UPDATE_INTERVAL, always_update = False)
        self.device = device
        self._counter = 0
        self._last_successful_data: dict[str, Any] | None = None
        self._is_offline_logged = False

    async def _async_setup(self) -> None:
        try:
            return await self.device.load()
        except Exception as e:
            if isinstance(e, TimeoutError):
                raise
            raise UpdateFailed(e) from e

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            try:
                data = await self.device.get(int(self._counter * self._update_interval_seconds))
                self._last_successful_data = data
                self._is_offline_logged = False
                return data
                #return await self.device.get(int(self._counter * self._update_interval_seconds))
            finally:
                self._counter += 1
        except Exception as e:
            self._counter = 0

            if not self._is_offline_logged:
                _LOGGER.warning("Error retrieving data. Use last known data.")
                self._is_offline_logged = True

            if self._last_successful_data is not None:
                return self._last_successful_data

            if isinstance(e, TimeoutError):
                raise
            raise UpdateFailed(e) from e

    async def async_shutdown(self) -> None:
        _LOGGER.debug("async_shutdown")
        await super().async_shutdown()
        await self.device.shutdown()
