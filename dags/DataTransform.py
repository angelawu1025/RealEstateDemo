import pandas
import glob
import logging
import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Angela Wu',
    'start_date': datetime(2021, 3, 28, 0, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def glob_csv_data():
    files = glob.glob('/opt/airflow/data_resource/**/*_lvr_land_*.csv')

    dflist = []
    for file in files:
        filenames = file.replace('.csv', '').split('/')

        try:
            filedata = pandas.read_csv(file, header=1)
            filedata.fillna({"construction to complete the years": 0, "the unit price (NTD / square meter)": 0})
            filedata['df_name'] = filenames[2] + '_' + filenames[-1].split('_')[0] + '_' + filenames[-1].split('_')[-1]
            dflist.append(filedata)
        except:

            continue

    return dflist

# Combine all dataframes
def combine_dataframe(**context):
    dflist = context['task_instance'].xcom_pull(task_ids='glob_csv_data')
    df_all = pandas.concat(dflist)

    return df_all

# Export data to csv
def export_to_csv(**context):
    df_all = context['task_instance'].xcom_pull(task_ids='combine_dataframe')

    logging.info('testing')
    logging.info(pathlib.Path().absolute())
    logging.info('files count:{}'.format(len(df_all)))

    # filter data
    except_floor = ['一層', '二層', '三層', '四層', '五層', '六層', '七層', '八層', '九層', '十層', '十一層', '十二層']
    #except_floor_data = ~df_all['total floor number'].isin(except_floor)
    filter_data = df_all[(df_all['main use']=='住家用') & (df_all['building state'].str.contains("住宅大樓")) & (~df_all['total floor number'].isin(except_floor))]
    filter_data.to_csv('/opt/airflow/export/filter.csv',encoding="utf-8-sig",index=False)


    # Calculate total count of data
    df_all.loc[:, 'total count'] = df_all['transaction sign'].count()

    # filter data by 車位
    count_data = df_all[df_all['transaction sign'] == '車位'].agg({'total count': 'max', 'transaction sign': 'count', 'total price NTD': 'mean','the berth total price NTD': 'mean'}).reset_index()
    count_data.to_csv('/opt/airflow/export/count.csv',encoding="utf-8-sig",index=False)


with DAG('data_transform', default_args=default_args) as dag:
    glob_csv_data = PythonOperator(
        task_id='glob_csv_data',
        python_callable=glob_csv_data,
        provide_context=True
    )

    combine_dataframe = PythonOperator(
        task_id='combine_dataframe',
        python_callable=combine_dataframe,
        provide_context=True
    )

    export_to_csv = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv,
        provide_context = True
    )

    glob_csv_data >> combine_dataframe
    combine_dataframe >> export_to_csv