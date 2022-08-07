# Bigquery 테이블 Vertex AI로 불러오기
# Limit으로 불러올 경우 매번 동일한 데이터셋을 불러옴. 테이블을 매 순간 랜덤하게 뽑아와야 하므로 TABLESAMPLE SYSTEM 사용

################################
# Parameter
# table - Bigquery에 있는 테이블 명(DB, Schema 정보 포함 풀 네임 기입)
# ratio - Sampling 진행 시 샘플링할 비율(int,float) - (전수로 불러올 경우 'all'로 입력)

################################
# 필요한 모듈 설치 & 라이브러리 호출
!pip install --upgrade 'google-cloud-bigquery[bqstorage,pandas]'

import pandas as pd
from google.cloud import bigquery

################################
# Customized 함수
def bq2ver(table, ratio):
    bqclient = bigquery.Client()
    if ratio != "all":
        query = f"""
            SELECT * FROM `{table}` TABLESAMPLE SYSTEM ({ratio} PERCENT)
            """
    elif ratio == "all":
        query = f"""
            SELECT * FROM `{table}`
        """
    df = (
        bqclient.query(query)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    return df
