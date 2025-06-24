import requests
import pandas as pd
import time
from datetime import datetime, timedelta

def collect_bike_rent_data(api_key: str, start_date_str: str, end_date_str: str, output_filename: str = None):
    """
    서울시 따릉이 대여 데이터를 기간별로 수집하여 CSV로 저장하는 함수

    Parameters:
    - api_key: Open API 개인 키 (문자열)
    - start_date_str: 시작 날짜 (예: '2024-07-01')
    - end_date_str: 종료 날짜 (예: '2024-08-31')
    - output_filename: 저장할 CSV 파일명 (기본: 자동 생성)

    Returns:
    - DataFrame: 수집된 데이터를 담은 pandas DataFrame
    """

    ENDPOINT = 'http://openapi.seoul.go.kr:8088'
    DATA_TYPE = 'json'
    SERVICE_NAME = 'tbCycleRentData'
    STEP = 1000
    TARGET_COLS = ['RENT_DT', 'RENT_NM', 'RTN_DT', 'RTN_NM', 'USE_MIN', 'USE_DST']

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    filtered_rows = []

    while start_date <= end_date:
        date_str = start_date.strftime('%Y%m%d')
        print(f"📆 {date_str} 데이터 수집 시작")

        start = 1
        while True:
            end = start + STEP - 1
            url = f"{ENDPOINT}/{api_key}/{DATA_TYPE}/{SERVICE_NAME}/{start}/{end}/{date_str}/1"
            response = requests.get(url)
            print(f"🔗 요청 URL: {url}")

            if response.status_code != 200:
                print(f"❌ 요청 실패: {response.status_code}")
                break

            try:
                data = response.json()
            except Exception as e:
                print("❌ JSON 파싱 실패:", e)
                print("📄 응답 내용:", response.text[:300])
                break

            if 'rentData' not in data or 'row' not in data['rentData']:
                print(f"✅ {date_str} 수집 완료 (총 {len(filtered_rows)}개 누적)")
                break

            rows = data['rentData']['row']
            if not rows:
                print(f"✅ {date_str} 수집 완료 (총 {len(filtered_rows)}개 누적)")
                break

            for row in rows:
                filtered_row = {col: row.get(col, None) for col in TARGET_COLS}
                filtered_rows.append(filtered_row)

            print(f"📦 {date_str} | Fetched {start} ~ {end}")
            start += STEP
            time.sleep(0.5)

        start_date += timedelta(days=1)

    df = pd.DataFrame(filtered_rows)

    # 저장 파일명 자동 생성
    if output_filename is None:
        output_filename = f"따릉이_{start_date_str.replace('-', '')}_{end_date_str.replace('-', '')}.csv"

    df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"✅ 저장 완료: {output_filename}")
    return df
