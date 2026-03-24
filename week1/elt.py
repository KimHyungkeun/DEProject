import csv
import random
import os
from datetime import datetime, timedelta
import shutil
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# 설정
TOTAL_FILES = 10
ROWS_PER_FILE = 100_000 

# --- 사용 예시 ---
db_config = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}


class ELTClass :
    def __init__(self):
        self.source_dir = 'source/csv'
        self.landing_dir = 'landing'
        self.staging_dir = 'staging'

    # 파일 추출
    def extract(self) :
        os.makedirs(self.source_dir, exist_ok=True)
        cities = ['Seoul', 'Busan', 'Incheon', 'Daegu', 'Daejeon', 'Gwangju', 'Ulsan', 'Jeju']
        start_date = datetime(2025, 1, 1)
        
        print(f"데이터 생성 시작: 총 {TOTAL_FILES * ROWS_PER_FILE:,} 건")

        for file_num in range(1, TOTAL_FILES + 1):
            file_path = os.path.join(self.source_dir, f'weather_part_{file_num}.csv')
            
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['date', 'city', 'temperature', 'humidity', 'wind_speed', 'precipitation_prob'])
                
                # date (날짜)	
                # city (도시)	
                # temperature (기온)	
                # humidity (습도)	
                # wind_speed (풍속)	
                # precipitation_prob (강수확률)
                for _ in range(ROWS_PER_FILE):
                    current_date = (start_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
                    city = random.choice(cities)
                    temp = round(random.uniform(-10.0, 35.0), 1)
                    humidity = random.randint(10, 95)
                    wind = round(random.uniform(0.5, 15.0), 1)
                    precip = random.randint(0, 100)
                    
                    writer.writerow([current_date, city, temp, humidity, wind, precip])
            
            print(f"[{file_num}/{TOTAL_FILES}] {file_path} 생성 완료")

        print("모든 샘플 데이터가 csv 폴더에 준비되었습니다.")

    # 추출한 파일을 landing zone에 적재
    def load(self) :
        os.makedirs(self.landing_dir, exist_ok=True)
        for file in glob.glob(f"{self.source_dir}/*.csv"):
            print(file)
            shutil.move(file, self.landing_dir)

    # 10월~4월용 겨울 체감온도 (풍속 반영)
    # 미국·캐나다 공동 개발 신 바람냉각지수(2001), 기상청 공식 채택
    # 출처 : https://data.kma.go.kr/climate/windChill/selectWindChillChart.do
    
    def _calculate_winter_chill(self, temp, wind_speed):
        # 풍속 단위 변환: m/s -> km/h (공식 기준)
        v = wind_speed * 3.6
        # 공식: 13.12 + 0.6215*T - 11.37*(V^0.16) + 0.3965*T*(V^0.16)
        wct = 13.12 + 0.6215 * temp - 11.37 * (v ** 0.16) + 0.3965 * temp * (v ** 0.16)
        return round(wct, 1)

    # 출처2 : (한파기준온도) https://www.weather.go.kr/w/community/knowledge/standard.do
    # 현재 접속이 안되고 있음
    def _get_weather_status(self, min_temp, month):
        if month >= 10 or month <= 4:  # 겨울
            if min_temp <= -15:
                return 'Coldwave Warning'    # 한파 경보
            if min_temp <= -12:
                return 'Coldwave Advisory'  # 한파 주의보
        return 'Normal'
    
    def _cleanup_landing_zone(self, files):
        """변환이 완료된 landing 폴더의 파일들을 삭제"""
        print("landing 폴더 정리 중...")
        for file_path in files:
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"삭제 완료: {file_path}")
            except Exception as e:
                print(f"파일 삭제 실패 ({file_path}): {e}")
        print("landing 폴더 정리가 완료되었습니다.")
    
    # landing zone의 내용을 읽어 변환하여 staging 디렉터리에 최종 적재
    def transform(self) :
        os.makedirs(self.staging_dir, exist_ok=True)
        # 중요: load()에서 파일을 옮겼으므로 landing_dir를 탐색해야 합니다.
        files = glob.glob(f"{self.landing_dir}/*.csv")

        if not files:
            print("변환할 파일이 landing 폴더에 없습니다.")
            return

        for file_path in files:
            staging_path = os.path.join(self.staging_dir, f"transformed_{os.path.basename(file_path)}")

            with open(file_path, 'r', encoding='utf-8') as f_in, \
                 open(staging_path, 'w', newline='', encoding='utf-8') as f_out:
                
                reader = csv.DictReader(f_in)
                writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames + ['apparent_temp', 'weather_status'])
                writer.writeheader()

                for row in reader:
                    # 데이터 파싱
                    date_obj = datetime.strptime(row['date'], '%Y-%m-%d')
                    month = date_obj.month
                    temp = float(row['temperature'])
                    wind = float(row['wind_speed'])

                    # 계절별 계산 분기
                    if 10 <= month or month <= 4: 
                        at = self._calculate_winter_chill(temp, wind)
                    else :
                        at = temp
                    
                    status = self._get_weather_status(at, month)

                    # 행 데이터 업데이트
                    row['apparent_temp'] = at
                    row['weather_status'] = status
                    writer.writerow(row)

            self._upsert_weather_from_csv(staging_path)
            print(f"성공: {file_path} -> {os.path.basename(staging_path)}")


        # --- 파일 삭제 로직 추가 ---
        self._cleanup_landing_zone(files)

    # staging_zone의 내용을 sql에 insert
    def _upsert_weather_from_csv(self, staging_path):
        """
        1. 지정된 SQL 파일을 실행 (전처리)
        2. CSV 파일을 읽어 PostgreSQL 테이블에 Upsert 수행
        """
        sql_file_path = "sql/transformed_weather.sql"
        conn = None

        try:
            conn = psycopg2.connect(**db_config)
            curr = conn.cursor()

            # 1. SQL 실행 (테이블이 없으면 생성하는 용도 등)
            if os.path.exists(sql_file_path):
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    curr.execute(f.read())

            # 2. 데이터 로드 (중복 제거 로직 삭제)
            df = pd.read_csv(staging_path)
            
            # NaN 처리 (NULL 값 입력 보장)
            df = df.where(pd.notnull(df), None)
            
            columns = df.columns.tolist()
            values = [tuple(x) for x in df.to_numpy()]

            # 3. 쿼리 구성 (ON CONFLICT 제거, 단순 INSERT)
            # CSV에 id가 있다면 제외 (DB SERIAL 자동 생성을 위함)
            if 'id' in columns:
                idx = columns.index('id')
                columns.pop(idx)
                values = [v[:idx] + v[idx+1:] for v in values]
            
            # 단순 INSERT 쿼리
            insert_query = f"""
                INSERT INTO public.weather_data ({", ".join(columns)})
                VALUES %s;
            """

            print(f"Executing INSERT for {len(values)} rows from {os.path.basename(staging_path)}...")
            
            # 10만 건이므로 성능을 위해 execute_values 사용
            execute_values(curr, insert_query, values)
            
            conn.commit()
            print("Success: Data inserted successfully.")

        except Exception as e:
            if conn: 
                conn.rollback()
            print(f"Error during INSERT: {e}")
        finally:
            if conn: 
                conn.close()
        