from airflow.hooks.mysql_hook import MySqlHook
from pathlib import Path
from json import loads, dumps
from pandas import DataFrame

def _eval_missing_data(ti, *args, **kwargs):
    processed_files_query = "SELECT DISTINCT CATEGORIA, DATE_FILE FROM DENUE.UNIDADES_ECONOMICAS_RAW;"
    mysql_hook = MySqlHook(mysql_conn_id = 'mysql_conn_id', schema = 'DENUE')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(processed_files_query)
    already_processed = cursor.fetchall()
    already_processed = DataFrame(map(
        lambda e: {
            "title_db": e[0],
            "date_db": e[1],
        },
        already_processed
    ))
    scrap_file = Path(ti.xcom_pull(task_ids='scrap_data'))
    available = loads(scrap_file.read_text())
    available = DataFrame(map(
        lambda e: {
            "title": e.get('title'),
            "date2": e.get('date'),
            "date": e.get('date').replace('|', ''),
            "url": e.get('url'),
        },
        available
    ))

    to_process = available.merge(
        right=already_processed,
        how='left',
        left_on=['date', 'title'],
        right_on=['date_db', 'title_db'],
        suffixes=('_left', '_right'),
    )
    to_process = to_process[["title", "date2", "url"]][to_process['title_db'].isna()]
    to_process = to_process.rename(columns={'date2': 'date'})
    to_process = list(map(
        lambda e: {"title": e.title, "date": e.date, "url": e.url},
        to_process.itertuples()
    ))

    scrap_file.write_text(dumps(to_process))
    print(len(to_process), 'elements detected.')