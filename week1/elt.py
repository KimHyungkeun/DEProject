import csv
import random
import os
from datetime import datetime, timedelta
import shutil
import glob

# 설정
TOTAL_FILES = 10
ROWS_PER_FILE = 100_000 


class ELTClass :
    def __init__(self):
        self.source_dir = 'source/csv'
        self.landing_dir = 'landing'
        self.target_dir = 'target'

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
    # 출처 : https://www.weather.go.kr/w/forecast/life/life-weather-index.do
    def _calculate_winter_chill(self, temp, wind_speed):
        # 풍속 단위 변환: m/s -> km/h (공식 기준)
        v = wind_speed * 3.6
        # 공식: 13.12 + 0.6215*T - 11.37*(V^0.16) + 0.3965*T*(V^0.16)
        wct = 13.12 + 0.6215 * temp - 11.37 * (v ** 0.16) + 0.3965 * temp * (v ** 0.16)
        return round(wct, 1)


    def _get_weather_status(self, apparent_temp, month):
        # 계절별 위험 수위 판단
        if 10 <= month or month <= 4:  # 겨울
            if apparent_temp <= -12: 
                return 'Coldwave Warning'
            if apparent_temp <= -6: 
                return 'Coldwave Advisory'
        
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
    
    # landing zone의 내용을 읽어 변환하여 target 디렉터리에 최종 적재
    # 겨울철 체감온도를 구하는 공식
    def transform(self) :
        os.makedirs(self.target_dir, exist_ok=True)
        # 중요: load()에서 파일을 옮겼으므로 landing_dir를 탐색해야 합니다.
        files = glob.glob(f"{self.landing_dir}/*.csv")

        if not files:
            print("변환할 파일이 landing 폴더에 없습니다.")
            return

        for file_path in files:
            target_path = os.path.join(self.target_dir, f"transformed_{os.path.basename(file_path)}")

            with open(file_path, 'r', encoding='utf-8') as f_in, \
                 open(target_path, 'w', newline='', encoding='utf-8') as f_out:
                
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

            print(f"성공: {file_path} -> {os.path.basename(target_path)}")

        # --- 파일 삭제 로직 추가 ---
        self._cleanup_landing_zone(files)
