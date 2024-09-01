### 1. 프로젝트 개요
<p align="center">
  <img src="https://cmsassets.rgpub.io/sanity/images/dsfx7636/news/9eb028de391e65072d06e77f06d0955f66b9fa2c-736x316.png?auto=format&fit=fill&q=80&w=625" alt="Example Image" />
</p>

#### League-of-legends-data-solution

#### 설명
실시간으로 게임속 플레이어의 데이터가 생성되는 상황에서 데이터를 수집, 저장, 변환, 서빙을 제공합니다. 데이터가 잘 흐를수 있는 환경을 구성하고 데이터 소비자에게 비지니스 목적에 맞게 데이터를 사용할 수 있도록 합니다.

자세한 설명은 [Git Wik](https://github.com/f-lab-edu/league-of-legends-data-solution/wiki/0.-%EB%93%A4%EC%96%B4%EA%B0%80%EB%A9%B0)i를 참고 해 주세요.

#### 기술 스택
- Python 3.11.8
- API - Java 17.0.12
- AWS EMR 6.8.0
  - SPARK 3.3.0
  - HADOOP 3.2.1
  - TRINO 388
  - JUPYTERHUB 1.4.1
  - ZEPPELIN 0.10.1
- AWS GLUE 
- Kafka Cluster
  - Kafka
  - Docker
 
### 2. 데이터 아키텍처
![제목 없음-2024-07-18-2224](https://github.com/user-attachments/assets/32f3758e-69d7-4ffe-baf8-622260840ae7)

자세한 설명은 [Git Wiki](https://github.com/f-lab-edu/league-of-legends-data-solution/wiki/2.-%EB%8D%B0%EC%9D%B4%ED%84%B0-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8) 참고 해 주세요.

 ### 3. 시작하기

 #### 필수조건
 - [ ] AWS EMR을 다음과 같이 구성해야 합니다
      - [ ] Spark
      - [ ] Hadoop
      - [ ] Trino
      - [ ] JupyterHub
      - [ ] Zeppelin
 - [ ] AWS EMR과 S3 권한 설정으로 접근이 가능해야 합니다.
 - [ ] Kafka 클러스터 또는 단일서버가 존재해야 합니다.
 - [ ] Trino가 AWS GLUE를 접근할 수 있도록 AWS EMR 설정을 변경해야 합니다.
 - [ ] Zeppelin 또는 JupyterHub에서 Trino와 연동하기 위해 설정을 변경해야 합니다.
       
#### 사용법

**API**
----
1. 플레이어의 데이터를 생성하는 API를 실행합니다.
  > java -jar .\LoL_PlyerEventGenertor-1.0-SNAPSHOT.jar Player IP:Port

**Player** - 플레이어의 수를 지정합니다. <br>
**IP:Port** - Kafka Broker의 IP와 Port

**Kafka**
----
1. Kafka Topic을 생성합니다.
> kafka-topics --create --topic TOPIC --bootstrap-server KAFKA_IP:PORT

2. Kafka Topic에 데이터가 들어오는 지 확인합니다.
> kafka-console-consumer --topic TOPIC --bootstrap-server KAFKA_IP:PORT

**AWS EMR 프라이머리에서**
----

> spark-submit  \
--master yarn \
--deploy-mode cluster \
--conf spark.kafka.bootstrap.servers="IP:Port" \
--conf spark.kafka.topic="Topic" \ <br>
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 \ 
./spark_streaming_kafka_to_s3.py

1. API를 실행합니다.
2. Kafka로 플레이어의 데이터가 전송되는지 확인합니다.
3.  AWS EMR에서 spark_streaming_kafka_to_s3.py를 제출합니다.


### 4. 결과
**데이터 엔지니어링**
1. 실시간으로 생성되는 데이터를 수집합니다.
2. 실시간으로 수집되는 데이터에 대한 전처리를 진행합니다.
3. 데이터의 타입을 사전에 정의합니다.
4. 개인 식별 정보(PII)와 같은 데이터를 저장하지 않습니다. 개인 정보를 저장하면 [개인 정보 보호법](https://casenote.kr/%EB%B2%95%EB%A0%B9/%EA%B0%9C%EC%9D%B8%EC%A0%95%EB%B3%B4_%EB%B3%B4%ED%98%B8%EB%B2%95/%EC%A0%9C24%EC%A1%B0) 위반 될 수 있습니다. 계정명과 IP 정보는 PII 중 민감하지 않는 PII에 속하지만 분석 및 학습에 사용하지 않는다면 위험성을 감수하면서 저장할 이유는 없다고 판단했습니다.


**데이터 소비자(데이터 분석가 또는 데이터 과학자)**
1. 데이터 소비자는 SQL과 Python 중 선택할 수 있는 환경을 제공합니다. 이로써 데이터 소비자는 적합한 환경을 기반으로 분석하거나 모델을 개발할 수 있습니다.
2. OLAP인 Trino를 사용 해 단일 쿼리로 많은 양의 데이터를 가져올 수 있습니다.
3. 미리 전처리를 거친 데이터를 통해 분석 또는 새로운 모델 학습, 운영으로 재사용이 가능합니다.

### 5. 관련 이슈

API
---
- [#1](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/1)
- [#6](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/6)
- [#10](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/10)
- [#12](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/12)

클러스터 구축
---
- [#3](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/3)
- [#4](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/4)
- [#5](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/5)
- [#7](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/7)
- [#8](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/8)
- [#9](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/9)
- [#13](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/13)

데이터 파이프라인
----
- [#14](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/14)

실시간 데이터 처리
---
- [#17](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/17)
- [#18](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/18)

오류
----
- [#15](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/15)

#### 출처
league of legend_logo : https://www.leagueoflegends.com/en-us/
