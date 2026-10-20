DATE_COL = "FlightDate"
TARGET = "ArrDel15"

STRING_COLS = [
    "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CancellationCode",
]
INT32_COLS = [
    "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek",
    "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime",
]
INT8_COLS = ["Cancelled", "Diverted", "DepDel15", "ArrDel15"]
FLOAT32_COLS = [
    "CRSElapsedTime", "Distance", "DepDelay", "ArrDelay", "TaxiOut", "TaxiIn",
    "ActualElapsedTime", "AirTime", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay",
]
RAW_COLUMNS = [DATE_COL] + STRING_COLS + INT32_COLS + INT8_COLS + FLOAT32_COLS

KEY_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "CRSDepTime",
]

# Describe what happened during or after the flight. NEVER use as model features.
# They may only be used to compute history from BEFORE the prediction time T.
OUTCOME_COLUMNS = [
    "DepTime", "DepDelay", "DepDel15", "ArrTime", "ArrDelay", "TaxiOut", "TaxiIn",
    "ActualElapsedTime", "AirTime", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay", "Cancelled", "Diverted", "CancellationCode",
]