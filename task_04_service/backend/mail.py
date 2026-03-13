from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import List, Optional
import pandas as pd
from datetime import datetime
import os

app = FastAPI(title="Energy Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = "data.csv"


class Record(BaseModel):
    timestep: datetime
    consumption_eur: float
    consumption_sib: float
    price_eur: float
    price_sib: float
    
    @validator('consumption_eur', 'consumption_sib', 'price_eur', 'price_sib')
    def check_non_negative(cls, v):
        if v < 0:
            raise ValueError('Значение не может быть отрицательным')
        return v

class RecordWithId(Record):
    id: int


def read_data():
    df = pd.read_csv(DATA_FILE)
    df['id'] = range(1, len(df) + 1)
    return df

def save_data(df):
    df.drop('id', axis=1).to_csv(DATA_FILE, index=False)


@app.get("/")
def root():
    return {"message": "Energy Data API работает"}

@app.get("/records", response_model=List[RecordWithId])
def get_all_records():
    try:
        df = read_data()
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records", response_model=RecordWithId)
def add_record(record: Record):
    try:
        df = read_data()
        
        new_record = record.dict()
        new_id = len(df) + 1
        
        new_row = pd.DataFrame([{
            'id': new_id,
            'timestep': new_record['timestep'],
            'consumption_eur': new_record['consumption_eur'],
            'consumption_sib': new_record['consumption_sib'],
            'price_eur': new_record['price_eur'],
            'price_sib': new_record['price_sib']
        }])
        
        df = pd.concat([df, new_row], ignore_index=True)
        save_data(df)
        
        return new_row.iloc[0].to_dict()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/records/{record_id}")
def delete_record(record_id: int):
    """Удалить запись по ID"""
    try:
        df = read_data()
        
        if record_id not in df['id'].values:
            raise HTTPException(status_code=404, detail=f"Запись с id {record_id} не найдена")
        
        df = df[df['id'] != record_id]
        
        df['id'] = range(1, len(df) + 1)
        
        save_data(df)
        
        return {"message": f"Запись {record_id} удалена"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
