from twelvedata import TDClient
import warnings

# Suppress FutureWarnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
# Initialize client - apikey parameter is requiered
td = TDClient(apikey="3a08aff23f824e18b12b51581ed3e109")

# Construct the necessary time series
ts = td.time_series(
    symbol="AAPL",
    interval="1min",
    outputsize=20,
    timezone="Africa/Casablanca",
)

# Returns pandas.DataFrame
df = ts.as_pandas()
print(df)