from datetime import datetime, timezone


def current_timestamps():
    load_ts = datetime.now(timezone.utc)
    return load_ts.isoformat(), load_ts.date().isoformat()

