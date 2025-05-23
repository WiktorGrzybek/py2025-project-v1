from pathlib import Path
import json
import csv
import threading
from datetime import datetime, timedelta
import zipfile
from typing import List, Optional, Iterator, Dict

class Logger:
    def __init__(
        self,
        config_path: str
    ) -> None:
        self._lock = threading.Lock()
        cfg = json.loads(Path(config_path).read_text())
        self.log_dir = Path(cfg['log_dir'])
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir = self.log_dir / 'archive'
        self.archive_dir.mkdir(exist_ok=True)
        self.filename_pattern = cfg['filename_pattern']
        self.rotate_every = cfg['rotate_every_hours']
        self.max_size = cfg['max_size_mb']
        self.rotate_after = cfg['rotate_after_lines']
        self.retention = cfg['retention_days']
        self.current_file = self.log_dir / datetime.now().strftime(self.filename_pattern)
        self._open_file()
        self._line_count = 0

    def _open_file(self) -> None:
        is_new = not self.current_file.exists()
        self._fp = self.current_file.open('a', newline='')
        self._writer = csv.DictWriter(self._fp, fieldnames=['timestamp', 'sensor_id', 'value', 'unit'])
        if is_new:
            self._writer.writeheader()
            self._fp.flush()

    def update(self, sensor, value: float) -> None:
        rec = {
            'timestamp': datetime.now().isoformat(),
            'sensor_id': sensor.config.sensor_id,
            'value': value,
            'unit': sensor.config.unit
        }
        with self._lock:
            self._writer.writerow(rec)
            self._fp.flush()
            self._line_count += 1
            if self._needs_rotation():
                self._rotate()

    def _needs_rotation(self) -> bool:
        age = datetime.now() - datetime.fromtimestamp(self.current_file.stat().st_mtime)
        if self._line_count >= self.rotate_after:
            return True
        if age >= timedelta(hours=self.rotate_every):
            return True
        if self.current_file.stat().st_size >= self.max_size * 1024 * 1024:
            return True
        return False

    def _rotate(self) -> None:
        self._fp.close()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        archive = self.archive_dir / f"{self.current_file.name}.{timestamp}.zip"
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(self.current_file, arcname=self.current_file.name)
        self.current_file.unlink()
        self._line_count = 0
        self.current_file = self.log_dir / datetime.now().strftime(self.filename_pattern)
        self._open_file()
        self._cleanup_archives()

    def _cleanup_archives(self) -> None:
        expire = datetime.now() - timedelta(days=self.retention)
        for z in self.archive_dir.iterdir():
            try:
                ts = datetime.strptime(z.stem.split('.')[-1], '%Y%m%d%H%M%S')
                if ts < expire:
                    z.unlink()
            except Exception:
                continue

    def read_logs(
        self,
        start: datetime,
        end: datetime,
        sensor_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Wczytuje wpisy z bieżących i zarchiwizowanych plików CSV.
        """
        entries: List[Dict] = []

        for csv_path in [self.current_file]:
            if csv_path.exists():
                with csv_path.open() as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ts = datetime.fromisoformat(row['timestamp'])
                        if start <= ts <= end and (sensor_id is None or row['sensor_id']==sensor_id):
                            entries.append({
                                'timestamp': ts,
                                'sensor_id': row['sensor_id'],
                                'value': float(row['value']),
                                'unit': row['unit']
                            })

        for z in self.archive_dir.glob('*.zip'):
            with zipfile.ZipFile(z) as zf:
                for name in zf.namelist():
                    with zf.open(name) as f:
                        reader = csv.DictReader(map(lambda b: b.decode(), f))
                        for row in reader:
                            ts = datetime.fromisoformat(row['timestamp'])
                            if start <= ts <= end and (sensor_id is None or row['sensor_id']==sensor_id):
                                entries.append({
                                    'timestamp': ts,
                                    'sensor_id': row['sensor_id'],
                                    'value': float(row['value']),
                                    'unit': row['unit']
                                })

        return sorted(entries, key=lambda x: x['timestamp'])

    def close(self) -> None:
        self._fp.close()