import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# 🔐 개인 API 키
API_KEY = '4749484f48646c77353461546d4842'
ENDPOINT = 'http://openapi.seoul.go.kr:8088'
DATA_TYPE = 'json'
SERVICE_NAME = 'tbCycleRentData'
STEP = 1000

# 🔁 수집할 날짜 범위
start_date = datetime.strptime('2024-07-01', '%Y-%m-%d')
end_date = datetime.strptime('2024-07-07', '%Y-%m-%d') # 변경 가능, 샘플로 7일

# 📦 전체 결과 저장 리스트 (필요 열만!)
filtered_rows = []

# 📜 필요한 열만 추출
target_cols = ['RENT_DT', 'RENT_NM', 'RTN_DT', 'RTN_NM', 'USE_MIN','USE_DST'] #대여일, 대여소, 반납일, 반납장소, 이용시간, 이용거리

while start_date <= end_date:
    date_str = start_date.strftime('%Y%m%d')
    print(f"📆 {date_str} 데이터 수집 시작")

    start = 1
    while True:
        end = start + STEP - 1
        url = f"{ENDPOINT}/{API_KEY}/{DATA_TYPE}/{SERVICE_NAME}/{start}/{end}/{date_str}/1"
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

        # 🧹 필요한 열만 추출해서 저장
        for row in rows:
            filtered_row = {col: row.get(col, None) for col in target_cols}
            filtered_rows.append(filtered_row)

        print(f"📦 {date_str} | Fetched {start} ~ {end}")
        start += STEP
        time.sleep(0.5)

    start_date += timedelta(days=1)

# 📊 DataFrame 생성
df = pd.DataFrame(filtered_rows)

# 💾 저장
# filename = "따릉이_20240701_0707.csv"
# df.to_csv(filename, index=False, encoding='utf-8-sig')
# print(f"✅ 저장 완료: {filename}")
