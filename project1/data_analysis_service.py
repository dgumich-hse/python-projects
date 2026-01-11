def sequential_analysis(df):
    result = df.copy()

    result['temp_ma_30'] = (
        result
        .groupby('city')['temperature']
        .rolling(window=30, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    stats = (get_stats(result))

    result = result.merge(stats, on=['city', 'season'], how='left')

    result['is_anomaly'] = (
            (result['temperature'] > result['season_mean'] + 2 * result['season_std']) |
            (result['temperature'] < result['season_mean'] - 2 * result['season_std'])
    )

    return result[result['is_anomaly'] == True]


def get_stats(df):
    return (df.groupby(['city', 'season'])['temperature']
            .agg(['mean', 'std'])
            .rename(columns={'mean': 'season_mean', 'std': 'season_std'})
            .reset_index())
