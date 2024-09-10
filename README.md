# 1. 프로젝트 개요
<p align="center">
  <img src="https://cmsassets.rgpub.io/sanity/images/dsfx7636/news/9eb028de391e65072d06e77f06d0955f66b9fa2c-736x316.png?auto=format&fit=fill&q=80&w=625" alt="Example Image" />
</p>

## League-of-legends-data-solution

### 설명
실시간 게임 속 플레이어의 데이터가 지속적으로 생성되는 환경에서 데이터를 수집, 저장, 변환하고 최종적으로 서빙을 제공합니다. 데이터가 잘 흐를 수 있는 환경을 구성하고 데이터 소비자에게 데이터를 목적에 맞게 사용할 수 있도록 합니다.

자세한 설명은 [Git Wiki](https://github.com/f-lab-edu/league-of-legends-data-solution/wiki/0.-%EB%93%A4%EC%96%B4%EA%B0%80%EB%A9%B0)를 참고 해 주세요.

### 기술 스택
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
- Terraform v1.9.5
- Airflow
 
# 2. 데이터 아키텍처
![1 1 0](https://github.com/user-attachments/assets/70e5e21a-3b0f-4e9f-972e-c60ef581bfef)


자세한 설명은 [Git Wiki](https://github.com/f-lab-edu/league-of-legends-data-solution/wiki/2.-%EB%8D%B0%EC%9D%B4%ED%84%B0-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8) 참고 해 주세요.

 # 3. 시작하기

 ## 필수조건
 - [X] AWS EMR을 다음과 같이 구성해야 합니다
      - [X] Spark
      - [X] Hadoop
      - [X] JupyterHub
      - [X] Zeppelin
 - [X] AWS QuickSight와 Athena를 연동합니다.
 - [X] Kafka 클러스터 또는 단일서버가 존재해야 합니다.
 - [X] Terraform가 구성되어 있습니다.
 - [X] Airflow가 구성되어 있습니다.
       
## 사용법

 **Terraform**
----
1. emr-cluster.tf가 있는 디렉토리에서
> Terraform plan

> Terraform apply

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

1. 개인 식별 정보(PII)와 같은 데이터를 저장하지 않습니다. 개인 정보를 저장하면 [개인 정보 보호법](https://casenote.kr/%EB%B2%95%EB%A0%B9/%EA%B0%9C%EC%9D%B8%EC%A0%95%EB%B3%B4_%EB%B3%B4%ED%98%B8%EB%B2%95/%EC%A0%9C24%EC%A1%B0) 법률 위반문제가 발생할 수 있습니다. 계정명과 IP 정보는 PII 중 민감하지 않는 PII에 속하지만 분석 및 학습에 사용하지 않는다면 위험성을 감수하면서 저장할 이유는 없다고 판단했습니다.
2. 데이터 소비자가 SQL과 Python 중 선택할 수 있는 환경을 제공합니다. 이로써 데이터 소비자는 적합한 환경을 기반으로 분석하거나 모델을 개발할 수 있습니다.
3. OLAP인 AWS Athena를 활용해 쿼리를 통해 대용량의 데이터를 가져올 수 있습니다.
4. 전처리를 거친 데이터를 통해 다양한 목적으로 재사용 가능합니다.
5. **비전문적 데이터 소비자도 쉽게 분석된 데이터를 시각적으로 제공하며, 의사결정을 할 수 있도록 지원합니다.**
6. **Terraform을 사용하여 AWS EMR 클러스터 인프라를 코드로 관리하고 추적합니다.**
7. **Spark Batch 작업을 Airflow로 워커 플로워를 관리합니다.**


### 5.성능 지표

##### 플레이어가 100명일 때
![모니터링_시스템_100명](https://github.com/user-attachments/assets/4dc6a621-b757-4026-8439-fb8ec5302472)### 
| Message in per second| Message in per minute | Lag by Consumer Group |Message consume per minute|
| ------------- | ------------- |------------|-----------|
| 10~ 11.6  | 500 ~  657 | 5 ~ 23| 580 ~ 667|

<h3>Yarn</h3>
리소스 매니저는 스파크 어플리케이션에게 7G의 메모리와 vCPU 2개를 할당하였고 약 5분간 3,178개의 데이터를 수집하고 총 5개의 배치를 생성했습니다.
각 배치의 평균 시간은 0.78ms로 처리하고 있으며 Batch Duration은 실행 초기를 제외하면 안정적으로 1,095~2,190ms를 유지하고 있습니다.
이로써 100명을 기준으로 Yarn 클러스터는 안정적으로 스트리밍을 유지하고 있습니다.



##### 플레이어가 500명일 때
![모니터링_시스템_100명](https://github.com/user-attachments/assets/4dc6a621-b757-4026-8439-fb8ec5302472)
| Message in per second| Message in per minute | Lag by Consumer Group |Message consume per minute|
| ------------- | ------------- |------------|-----------|
| 25 ~ 30  | 1.5k ~ 1.7k | 30 ~ 48 | 1.64k|

<h3>Yarn</h3>
Yarn은 해당 어플리케이션의 모든 리소스를 할당했습니다. 하나의 클러스터에서 하나의 어플리케이션이 모든 자원을 할당 받는 상황은 좋지 않은 상황이라고 판단하고 있습니다. 할당된 자원을 온전히 사용하고 있는지 확인하고, 잘 사용하고 있다면 새로운 노드를 추가하거나, 온전히 사용하지 않는다면 리소스를 제한하는 방식의 유연한 운영 방식이 필요하다고 생각합니다. <br>

관련 PR : https://github.com/f-lab-edu/league-of-legends-data-solution/pull/30

### 6. 스케줄링
![airflow_후](https://github.com/user-attachments/assets/300935e1-8bf8-4f0f-bf13-52243c792e86)

_Dag를 실행 하는 logical_date를 기준으로 데이터를 Silver Layer와 Gold Layer의 Batch 처리를 수행합니다._


### 6. 변경 사항

## 이슈 및 PR
- [인프라 코드 관리 및 추적을 위한 Terraform 적용 #21](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/21)
  - [Feat: IaC 도구인 Terraform으로 AWS EMR 클러스터를 코드로 선언 #23](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/23)

- [SQL 엔진을 Trino -> AWS Athena로 변경합니다. #22](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/22)

- [AWS QuickSight 연결 #20](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/20)

- [Kafka 성능 및 안전성 문제 #26](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/26)
  - [feat: Kafka Lag 모니터링 시스템 구축 #30](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/30)

- [스파크 배치 어플리케이션 개발 #24](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/24)
  - [feat: Spark Batch용 어플리케이션 추가 #27](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/27)

- [Airflow를 사용 해 스파크 배치 어플리케이션 스케줄링 #25](https://github.com/f-lab-edu/league-of-legends-data-solution/issues/25)
  - [feat: Airflow Batch 스케줄링 추가 #28](https://github.com/f-lab-edu/league-of-legends-data-solution/pull/28)

#### 출처
league of legend_logo : https://www.leagueoflegends.com/en-us/

