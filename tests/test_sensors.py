from sensors.sensor import Sensor, TemperatureSensor

class DummyObserver:
    def __init__(self):
        self.notifications = []
    def update(self, sensor, value):

        self.notifications.append((sensor.name, value))

def test_sensor_inheritance():
    temp = TemperatureSensor("Temp1")
    assert isinstance(temp, Sensor)

def test_sensor_observer_notification():
    temp = TemperatureSensor("Temp2")
    dummy = DummyObserver()
    temp.attach(dummy)
    value = temp.read()

    assert dummy.notifications[0][0] == "Temp2"
    assert dummy.notifications[0][1] == value
