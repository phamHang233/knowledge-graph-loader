
def timestamp_days_ago(end_timestamp: int, days: int) -> int:
  return end_timestamp - days * 24 * 3600
