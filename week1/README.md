# Week 1. ETL (Extract, Load, Transform)

## 1. 파일 설명
- extract_and_load.py 
    - 데이터를 추출하여 landing에 적재
- transform.py
    - landing에 있는 데이터를 변환하여 target에 적재
- landing 
    - extract_and_load.py의 결과 데이터를 적재
- target 
    - transform.py의 결과 데이터를 적재


## 2. 프로세스 순서

1) extract_and_load.py (추출)
2) transform.py (변환)