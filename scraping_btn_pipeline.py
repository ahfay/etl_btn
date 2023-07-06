from btn.main import ekstraktor
import sqlalchemy
from prefect import task, flow
import pandas as pd
import numpy as np

def change_valeus(x):
    new_x = ''
    x = x.lower()
    if 'shm' in x:
        new_x = 'SHM'
    elif 'hm' in x:
        new_x = 'SHM'
    elif 'm.3200' in x:
        new_x = 'SHM'
    elif 'shgb' in x:
        new_x = 'SHGB'
    elif 'shgp' in x:
        new_x = 'SHGB'
    elif 'hgb' in x:
        new_x = 'SHGB'
    elif 'shg' in x:
        new_x = 'SHGB'
    elif 'hpl' in x:
        new_x = 'HPL'
    else:
        new_x = '-'
    return new_x


@task
def run_ekstrak_btn():
    ekstraktor()

@task
def transform():
    df = pd.read_csv('btn/Indonesia/ALL PROVINSI.csv', dtype={'luas_tanah':np.int32,'luas_bangunan':np.int32,'lebar_jalan':np.int32})
    df = df.drop(labels='Unnamed: 0',axis=1)
    df = df.drop(df[df['harga'] == '-'].index) 
    df.harga = pd.to_numeric(df['harga'])
    df.sertifikat = df.sertifikat.astype('str') 
    df.sertifikat = df.sertifikat.apply(change_valeus)
    df.sertifikat = df.sertifikat.apply(lambda x: 'SHM' if x == '-' else x)
    df.tanggal = pd.to_datetime(df.tanggal)
    df = df.rename(columns={'kab/kota': 'kab_kota'})
    df = df.drop(df[(df['luas_tanah'] < 10) & (df['luas_bangunan'] < 10)].index)
    df = df.drop(df[(df['luas_tanah'] < 10)].index)
    df = df.drop(df[(df['luas_bangunan'] < 10)].index)
    df = df.drop(df[(df['luas_tanah'] > 1000) & (df['luas_bangunan'] < 20)].index)
    data = df.drop(df[(df['luas_tanah'] < 20) & (df['luas_bangunan'] > 1000)].index)
    return data

@task
def load(data):
    engine = sqlalchemy.create_engine('postgresql://postgres:admin001@127.0.0.1:5432/postgres')
    connection = engine.connect()
    connection.execute('drop table if exists btn')
    connection.execute('create table btn(harga BIGINT,luas_tanah INT,luas_bangunan INT, lebar_jalan INT, type VARCHAR(10), sertifikat VARCHAR(10), tanggal DATE, kec VARCHAR(35), kab_kota VARCHAR(35), prov VARCHAR(35), alamat_lengkap TEXT, id_rumah VARCHAR(20), link TEXT)')
    data.to_sql('btn',con=connection, if_exists='append',index=False)
    connection.close()

@flow(log_prints=True, name="etl_btn", retry_delay_seconds=5)
def etl_btn_flow():
    run_ekstrak_btn()
    print("START TRANSFORM")
    transf = transform()
    print("START LOAD TO POSTGRESQL")
    load(transf)
    print("END")

if __name__ == '__main__':
    etl_btn_flow()
    