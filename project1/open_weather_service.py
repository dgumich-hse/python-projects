import asyncio
from datetime import datetime

import aiohttp
import pandas as pd
import requests

BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'


def get_season(date):
    month = date.month
    if month in (12, 1, 2):
        return 'winter'
    elif month in (3, 4, 5):
        return 'spring'
    elif month in (6, 7, 8):
        return 'summer'
    else:
        return 'autumn'


def get_current_temperature_sync(city, api_key):
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'ru'
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if response.status_code != 200:
        return None, data
    return data['main']['temp'], None


def check_temperature_anomaly(city, current_temp, season_stats):
    season = get_season(datetime.utcnow())

    row = season_stats[
        (season_stats['city'] == city) &
        (season_stats['season'] == season)
        ]

    if row.empty:
        return None

    mean = row.iloc[0]['season_mean']
    std = row.iloc[0]['season_std']

    is_anomaly = abs(current_temp - mean) > 2 * std

    return {
        'city': city,
        'season': season,
        'current_temp': current_temp,
        'season_mean': round(mean.item(), 2),
        'season_std': round(std.item(), 2),
        'is_anomaly': is_anomaly.item()
    }


async def get_current_temperature_async(session, city, api_key):
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }
    async with session.get(BASE_URL, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        return city, data['main']['temp']


async def fetch_all_cities(cities, api_key):
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_current_temperature_async(session, city, api_key)
            for city in cities
        ]
        return await asyncio.gather(*tasks)


async def monitor_cities(cities, season_stats, api_key):
    results = await fetch_all_cities(cities, api_key)

    reports = []
    for city, temp in results:
        report = check_temperature_anomaly(city, temp, season_stats)
        reports.append(report)

    return pd.DataFrame(reports)
