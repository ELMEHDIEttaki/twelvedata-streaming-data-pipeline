from twelvedata import TDClient
import warnings

# Suppress FutureWarnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
# Initialize client - apikey parameter is requiered
td = TDClient(apikey="")

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