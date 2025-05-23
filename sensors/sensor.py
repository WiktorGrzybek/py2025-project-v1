from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol, Union
import numpy as np

# -- Helper RNG --
def default_rng():
    return np.random.default_rng()

class Observer(Protocol):
    def update(self, sensor: "Sensor", value: Any) -> None:
        ...

@dataclass
class SensorConfig:
    sensor_id: str
    name: str
    unit: str
    min_value: float
    max_value: float
    frequency: float  # seconds between reads
    calibration_factor: float = 1.0

class Sensor(ABC):
    """
    Bazowa klasa czujnika (Subject w wzorcu Observer).
    Generuje pomiary i powiadamia obserwatorów.
    """
    def __init__(self, config: SensorConfig, rng: Optional[np.random.Generator] = None) -> None:
        self.config = config
        self._rng = rng or default_rng()
        self._observers: List[Union[Observer, Callable]] = []
        self._active = False
        self._last_value: Optional[float] = None

    def attach(self, observer: Union[Observer, Callable]) -> None:
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: Union[Observer, Callable]) -> None:
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self, value: Any) -> None:
        for obs in self._observers:
            if hasattr(obs, 'update'):
                obs.update(self, value)
            else:
                obs(self, value)

    def start(self) -> None:
        self._active = True

    def stop(self) -> None:
        self._active = False

    def read(self) -> float:
        if not self._active:
            raise RuntimeError(f"Sensor '{self.config.name}' is not active.")
        raw = self._generate()
        calibrated = raw * self.config.calibration_factor
        self._last_value = calibrated
        self.notify(calibrated)
        return calibrated

    @abstractmethod
    def _generate(self) -> float:
        """Zwraca niezmodyfikowany odczyt czujnika."""
        ...

    def get_last(self) -> float:
        if self._last_value is None:
            return self.read()
        return self._last_value

# --- Implementacje czujników ---
class TemperatureSensor(Sensor):
    def _generate(self) -> float:
        mean = (self.config.min_value + self.config.max_value) / 2
        std = (self.config.max_value - self.config.min_value) / 6
        return float(np.clip(self._rng.normal(mean, std),
                              self.config.min_value,
                              self.config.max_value))

class HumiditySensor(Sensor):
    def _generate(self) -> float:
        a, b = 2, 5
        val = self._rng.beta(a, b)
        return float(np.clip(val * (self.config.max_value - self.config.min_value) +
                              self.config.min_value,
                              self.config.min_value,
                              self.config.max_value))

class PressureSensor(Sensor):
    def _generate(self) -> float:
        base = self._rng.uniform(self.config.min_value, self.config.max_value)
        drift = np.sin(np.deg2rad(self.config.frequency)) * 0.5
        return float(np.clip(base + drift,
                              self.config.min_value,
                              self.config.max_value))

class LightSensor(Sensor):
    def _generate(self) -> float:
        # symulacja zmian oświetlenia: sinusoidalny cykl dnia
        t = np.mod(self.config.frequency, 24)
        lux = (np.sin(2 * np.pi * t / 24) + 1) / 2 * (self.config.max_value - self.config.min_value) + self.config.min_value
        noise = self._rng.normal(0, (self.config.max_value - self.config.min_value) * 0.05)
        return float(np.clip(lux + noise,
                              self.config.min_value,
                              self.config.max_value))

class AirQualitySensor(Sensor):
    def _generate(self) -> float:
        # AQI symulacja: mieszanka losowa z okazjonalnymi pikami
        base = self._rng.uniform(self.config.min_value, self.config.max_value)
        spike = self._rng.choice([0, self._rng.uniform(50, 150)], p=[0.9, 0.1])
        return float(np.clip(base + spike,
                              self.config.min_value,
                              self.config.max_value))

class AccelerometerSensor(Sensor):
    def _generate(self) -> float:
        # trzyelementowy wektor przyspieszenia (X, Y, Z) ze zwracaniem długości wektora
        vec = self._rng.uniform(-1, 1, size=3)
        magnitude = np.linalg.norm(vec) * (self.config.max_value)
        return float(np.clip(magnitude,
                              self.config.min_value,
                              self.config.max_value))

class ProximitySensor(Sensor):
    def _generate(self) -> float:
        # symulacja odległości z progami
        val = self._rng.uniform(self.config.min_value, self.config.max_value)
        return float(val)