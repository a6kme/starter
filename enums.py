from enum import IntEnum

weather_status = IntEnum(
    "status", "sunny partly_cloudy cloudy windy precipitation", start=0
)
