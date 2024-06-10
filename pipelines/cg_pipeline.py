from etls.get_cg_data import get_cg_data, load_data_to_csv
from datetime import datetime, timedelta

def cg_pipeline(coin_id, **context):


    start_date = datetime.strptime(context['ds'], '%Y-%m-%d').date()
    end_date = start_date + timedelta(days=1)

    df = get_cg_data(coin_id, start_date, end_date)

    file_path = f"/opt/airflow/data/output/cg_{coin_id}_{context['ds']}"
    load_data_to_csv(df,file_path)

    return file_path