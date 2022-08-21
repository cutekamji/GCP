!pip install --upgrade 'google-cloud-bigquery[bqstorage,pandas]'
import pandas as pd
from google.cloud import bigquery

# 테이블 스키마 별 테이블 이름 조회
# 파라미터
## sch - 스키마 이름
def schema(sch):
    bqclient = bigquery.Client()
    query = f"""
            select table_schema,table_name from bigquery-public-data.{sch}.INFORMATION_SCHEMA.TABLES
            """
    df = (
        bqclient.query(query)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    return df
