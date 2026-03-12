import os
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
API_URL = BACKEND_URL

st.set_page_config(page_title="Energy Dashboard", layout="wide")
st.title("⚡ Energy Consumption Dashboard")


@st.cache_data(ttl=10)
def get_data():
    try:
        response = requests.get(f"{API_URL}/records")
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            st.error(f"Ошибка API: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")
        return pd.DataFrame()

def add_record(record):
    try:
        response = requests.post(f"{API_URL}/records", json=record)
        if response.status_code == 200:
            st.success("Запись добавлена")
            st.cache_data.clear()
            return True
        else:
            st.error(f"Ошибка: {response.text}")
            return False
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")
        return False

def delete_record(record_id):
    try:
        response = requests.delete(f"{API_URL}/records/{record_id}")
        if response.status_code == 200:
            st.success(f"Запись {record_id} удалена")
            st.cache_data.clear()
            return True
        elif response.status_code == 404:
            st.error(f"Запись с id {record_id} не найдена")
            return False
        else:
            st.error(f"Ошибка: {response.text}")
            return False
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")
        return False

df = get_data()

if not df.empty:
    df['timestep'] = pd.to_datetime(df['timestep'])
    
    st.header("📋 Данные")
    st.dataframe(df, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Потребление энергии")
        fig1 = px.line(df, x='timestep', y=['consumption_eur', 'consumption_sib'],
                       labels={'value': 'МВт·ч', 'variable': 'Регион'},
                       title='Потребление по времени')
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.subheader("Цены на энергию")
        fig2 = px.line(df, x='timestep', y=['price_eur', 'price_sib'],
                       labels={'value': 'Руб/МВт·ч', 'variable': 'Регион'},
                       title='Цены по времени')
        st.plotly_chart(fig2, use_container_width=True)
    
    st.header("➕ Добавить запись")
    
    with st.form("add_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            date = st.date_input("Дата", datetime.now())
            time = st.time_input("Время", datetime.now().time())
            consumption_eur = st.number_input("Потребление Европа (МВт·ч)", min_value=0.0, value=50000.0)
            consumption_sib = st.number_input("Потребление Сибирь (МВт·ч)", min_value=0.0, value=15000.0)
        
        with col2:
            price_eur = st.number_input("Цена Европа (руб)", min_value=0.0, value=300.0)
            price_sib = st.number_input("Цена Сибирь (руб)", min_value=0.0, value=200.0)
        
        submitted = st.form_submit_button("Добавить")
        
        if submitted:
            timestep = datetime.combine(date, time).strftime("%Y-%m-%d %H:%M")
            new_record = {
                "timestep": timestep,
                "consumption_eur": consumption_eur,
                "consumption_sib": consumption_sib,
                "price_eur": price_eur,
                "price_sib": price_sib
            }
            add_record(new_record)
    
    st.header("Удалить запись")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        record_id = st.number_input("ID записи для удаления", min_value=1, step=1)
    with col2:
        if st.button("Удалить", type="primary"):
            delete_record(record_id)

else:
    st.warning("Нет данных для отображения")
