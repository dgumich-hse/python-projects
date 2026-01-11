from multiprocessing import Pool, cpu_count

import pandas as pd


def process_city(city_df):
    city_df = city_df.sort_values('timestamp')

    city_df['temp_ma_30'] = (
        city_df['temperature']
        .rolling(window=30, min_periods=1)
        .mean()
    )

    stats = (
        city_df
        .groupby('season')['temperature']
        .agg(['mean', 'std'])
        .rename(columns={'mean': 'season_mean', 'std': 'season_std'})
        .reset_index()
    )

    city_df = city_df.merge(stats, on='season', how='left')

    city_df['is_anomaly'] = (
            (city_df['temperature'] > city_df['season_mean'] + 2 * city_df['season_std']) |
            (city_df['temperature'] < city_df['season_mean'] - 2 * city_df['season_std'])
    )
    return city_df


def parallel_analysis(df):
    city_groups = [group for _, group in df.groupby("city")]

    with Pool(processes=cpu_count()) as pool:
        result_parts = pool.map(process_city, city_groups)

    return pd.concat(result_parts)