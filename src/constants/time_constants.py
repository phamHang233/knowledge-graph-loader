
SLEEP_DURATION = 3  # seconds


class TimeConstants:
    A_MINUTE = 60
    MINUTES_5 = 300
    MINUTES_15 = 900
    A_HOUR = 3600
    A_DAY = 86400
    DAYS_7 = 7 * A_DAY
    DAYS_30 = 30 * A_DAY
    DAYS_31 = 31 * A_DAY
    A_YEAR = 365 * A_DAY


class TimeInterval:
    hourly = 'hourly'
    daily = 'daily'
    monthly = 'monthly'

    mapping = {
        hourly: TimeConstants.A_HOUR,
        daily: TimeConstants.A_DAY,
        monthly: TimeConstants.DAYS_30
    }
