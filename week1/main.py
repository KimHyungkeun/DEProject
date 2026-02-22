from elt import ELTClass

def main() :

    # ELT 인스턴스 생성
    instance_elt = ELTClass()

    # source로 부터 데이터 추출
    instance_elt.extract()

    # 추출한 데이터를 landing에 적재
    instance_elt.load()

    # landing에서 데이터를 읽어와 target에 최종 적재
    instance_elt.transform()

if __name__ == '__main__' :
    main()