import os
from logger.logger import Logger

def test_logger_writes_and_rotates():

    try:
        os.remove("logs/sensor_data.csv")
    except FileNotFoundError:
        pass
    for i in range(1, 6):
        try:
            os.remove(f"logs/sensor_data.csv.{i}.zip")
        except FileNotFoundError:
            pass

    logger = Logger()

    logger.max_lines = 2
    logger.backup_count = 2

    logger.update(type("FakeSensor", (), {"name": "TestSensor"}), 123)
    logger.update(type("FakeSensor", (), {"name": "TestSensor"}), 456)

    assert logger.line_count == 0
    logger.update(type("FakeSensor", (), {"name": "TestSensor"}), 789)

    assert logger.line_count == 1

    assert os.path.exists("logs/sensor_data.csv.1.zip")
