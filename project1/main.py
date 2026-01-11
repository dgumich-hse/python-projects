import asyncio
import time

import pandas as pd

from project1.data_analysis_service import sequential_analysis, get_stats
from project1.open_weather_service import get_current_temperature_sync, check_temperature_anomaly, monitor_cities
from project1.parallel_analysis_service import parallel_analysis

API_KEY = ''

if __name__ == '__main__':
    df = pd.read_csv('temperature_data.csv', parse_dates=['timestamp'])
    df = df.sort_values(['city', 'timestamp'])

    start = time.time()
    seq_result = sequential_analysis(df)
    seq_time = time.time() - start

    print(f'Последовательный анализ: {seq_time:.2f} сек')
    season_stats = get_stats(df)

    start = time.time()
    par_result = parallel_analysis(df)
    par_time = time.time() - start

    print(f'Параллельный анализ: {par_time:.2f} сек')

    # Последовательный анализ выполняется быстрее - 0.05 сек VS 2.73. Скорее всего,
    # накладные расходы на разделение между процессорами, потом соединение данных
    # в единый дата фрейм занимает больше времени, чем работа внутри pandas с исходным дата фреймом

    city = 'Moscow'

    #Использование синхронного запроса подходит для синхронного кода + небольшой нагрузки.
    # Например, стримлит по умолчанию синхронный, а данные нужны по одному городу.
    # Поэтому для данного проекта подходит использование синхронного метода

    temp, error = get_current_temperature_sync(city, API_KEY)
    result = check_temperature_anomaly(city, temp, season_stats)

    print(f'Синхронный запрос, для города {city}')
    print(result)

    cities = ['Dubai', 'Beijing', 'Moscow', 'Los Angeles', 'Tokyo']

    print(f'Асинхронный запрос, для городов {cities}')
    df_report = asyncio.run(monitor_cities(cities, season_stats, API_KEY))
    print(df_report)
