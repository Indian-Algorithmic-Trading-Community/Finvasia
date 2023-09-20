import polars as pl
from typing import Literal

def prepare_data(dataframe: pl.DataFrame(), 
                 filtered: bool=True,
                 ) -> pl.DataFrame():

    df = dataframe.drop(
        ["stat", "time", "v", "oi"]
        ).rename(
            mapping={
            "ssboe" : "Timestamp", 
            "into" : "Open", 
            "inth" : "High",
            "intl" : "Low",
            "intc" : "Close",
            "intvwap" : "VWAP",
            "intv" : "Volume",
            "intoi" : "OI"
            }
        ).with_columns(
          pl.from_epoch(
              "Timestamp", time_unit="s").dt.replace_time_zone(
                  time_zone="UTC").dt.convert_time_zone(
                    time_zone="Asia/Kolkata")
                )
    
    if filtered:
        df_market = df.filter(
            pl.col("Timestamp").dt.time().is_between(
                    lower_bound=pl.time(9,15,00),
                    upper_bound=pl.time(15,30,00)
                )
            )
        return df_market
    return df

def resample_data(dataframe: pl.DataFrame(),
                  timeframe: Literal['3m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1mo', '1y']='5m'
                  ) -> pl.DataFrame():
    df_resampled = dataframe.sort("Timestamp").group_by_dynamic(
        "Timestamp",every=timeframe).agg(
            pl.col("Open").first(), 
            pl.col("High").max(), 
            pl.col("Low").min(), 
            pl.col("Close").last(),
            pl.col("VWAP").mean(), 
            pl.col("Volume").sum(), 
            pl.col("OI").sum()
        )
    return df_resampled

if __name__ == '__main__':

    df = pl.read_csv("Sample.csv")
    prepared_df = prepare_data(df)
    resampled_df = resample_data(dataframe=prepared_df, timeframe='1mo')
    print(resampled_df)
    #resampled_df.write_csv("resampled_data.csv")

'''
We can use day volume and day oi columns by taking the last() value when resampling 
'''




