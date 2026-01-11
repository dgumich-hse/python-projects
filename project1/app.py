from datetime import datetime

import pandas as pd
import seaborn as sns
import streamlit as st
from matplotlib import pyplot as plt

from data_analysis_service import get_stats, sequential_analysis
from open_weather_service import get_current_temperature_sync, get_season

st.set_page_config(page_title='Мониторинг температуры', layout='wide')
st.title('🌡 Мониторинг температуры по историческим данным')

uploaded_file = st.file_uploader(
    'Загрузите файл с историческими данными (CSV)',
    type='csv'
)

if uploaded_file is None:
    st.info('Загрузите temperature_data.csv для начала работы')
    st.stop()

df = pd.read_csv(uploaded_file, parse_dates=['timestamp'])

cities = sorted(df['city'].unique())
city = st.selectbox('Выберите город', cities)

city_df = df[df['city'] == city].copy()
city_df = city_df.sort_values(['timestamp'])

season_stats = get_stats(city_df)
city_df_result = sequential_analysis(city_df)


st.subheader('📊 Описательная статистика')

st.dataframe(
    city_df_result['temperature'].describe().to_frame('Температура (°C)')
)

st.subheader('📈 Временной ряд температур')

fig, ax = plt.subplots(figsize=(10, 4))

ax.plot(city_df_result['timestamp'], city_df_result['temperature'], label='Температура')
ax.scatter(
    city_df_result[city_df_result['is_anomaly']]['timestamp'],
    city_df_result[city_df_result['is_anomaly']]['temperature'],
    color='red',
    label='Аномалия',
    s=10
)

ax.set_ylabel('°C')
ax.legend()
st.pyplot(fig)

st.subheader('🍂 Сезонные профили')

fig, ax = plt.subplots(figsize=(6, 4))
sns.barplot(
    data=season_stats,
    x='season',
    y='season_mean',
    ax=ax
)

ax.set_ylabel('Средняя температура (°C)')
st.pyplot(fig)

st.subheader('🔑 OpenWeatherMap API')

api_key = st.text_input(
    'Введите API-ключ',
    type='password'
)

st.subheader('🌍 Текущая погода')

if not api_key:
    st.warning('Введите API-ключ, чтобы увидеть текущую погоду')
else:
    temp, error = get_current_temperature_sync(city, api_key)

    if error:
        st.error(error)
    else:
        season_now = get_season(datetime.utcnow())

        row = season_stats[season_stats['season'] == season_now]

        if row.empty:
            st.warning('Нет исторических данных для текущего сезона')
        else:
            mean = row.iloc[0]['season_mean']
            std = row.iloc[0]['season_std']

            is_anomaly = abs(temp - mean) > 2 * std

            st.metric('Текущая температура', f'{temp:.1f} °C')

            if is_anomaly:
                st.error('⚠ Температура АНОМАЛЬНА для текущего сезона')
            else:
                st.success('✅ Температура в пределах нормы')
