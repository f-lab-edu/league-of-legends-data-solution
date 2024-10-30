# 1. 프로젝트 개요
<p align="center">
  <img src="https://cmsassets.rgpub.io/sanity/images/dsfx7636/news/9eb028de391e65072d06e77f06d0955f66b9fa2c-736x316.png?auto=format&fit=fill&q=80&w=625" alt="Example Image" />
</p>

## League-of-legends-data-solution

### 1.1 프로젝트 설명

League of Legends와 같은 인기 게임을 운영할 수 있도록 데이터 비즈니스 목적에 맞춘 데이터 솔루션을 제공합니다. 

이 솔루션의 주요 목표는 데이터를 분석하여 유의미한 인사이트를 도출하고, 게임 내 밸런스를 유지하며 패치를 효율적으로 진행하는 것입니다. 또한, 불법 프로그램을 구별할 수 있는 이상 탐지 모델을 지속적으로 학습시켜 정확도를 향상시키는 데 집중하고 있습니다.

실시간으로 진행되는 수많은 게임 속 플레이어 데이터가 지속적으로 생성되며, 이를 수집, 저장, 변환하고 최종적으로 서빙하는 "데이터 엔지니어링 수명 주기"를 통해 데이터의 가치가 극대화됩니다. 이러한 데이터 솔루션은 게임의 품질 향상뿐만 아니라, 플레이어 경험을 개선하는 데에도 중요한 역할을 합니다.

### 1.2 기술 스택

#### AWS Clolud Service
  - AWS S3
  - AWS Athena
  - AWS EMR
  - AWS Glue
  - AWS QuickSight

#### Data Processing Tools
- Apache Kafka
- Apache Spark
- Apache Airflow
- Prometheus
- Grafana
- Zeppelin
- Jupyter Hub
- Data Hub

#### Programming Languages
- Python
- Java  - Player Log API
 
# 2.데이터 아키텍처
![전체_아키텍처](https://github.com/user-attachments/assets/7c574f98-c18d-4dda-aa5a-b54f62968a45)

위 데이터 아키텍처는 크게 가지 주요 역할을 수행합니다.

1. Data Engineering Lifecycle : 데이터의 수집, 저장, 처리, 제공을 관리하여 데이터의 가치를 제공합니다.
2. Kafka Lag Monitoring : Kafka Lag의 상태를 지속적으로 모니터링하고, Kafka Cluster의 상태를 유추할 수 있도록 합니다.
3. Data Discovery Platform : 사용자가 데이터를 탐색하고 이해할 수 있도록 지원합니다.

## 2.1 Data Engineering Lifecycle

### 2.1.1 데이터 생성
![image](https://github.com/user-attachments/assets/8a5d4f9e-4bb3-4bec-b518-423cc1b6a2ed)

플레이어 로그를 생성하는 API는 데이터가 생성되는 원본 출처입니다.

플레이어는 위와 같은 13가지의 필드를 포함하고 있습니다.

레코드당 약 400 Byte 크기를 가지고 있으며, 각 레코드는 Kafka Topic으로 메세지로 전송됩니다.

동시 접속 플레이어는 1000 ~ 2000명을 유지하고, 일일 데이터 수집량은 약 1,300,000건의 데이터를 수집합니다. 

### 2.1.2 데이터 수집 및 처리 저장 또는 ETL

![image](https://github.com/user-attachments/assets/2538b2d1-9eee-463c-ac00-02f839c2c73f)

Spark는 Spark Streaming을 통해 Kafka 특정 Topic으로 들어오는 데이터를 스트림 처리를 합니다.

여기서 Lambda Architecture와 같이 Streaming Layer와 Batch Layer로 나눠서 운영합니다.

Streaming Layer부터 보겠습니다.

![image](https://github.com/user-attachments/assets/e87f5012-2168-4750-b4b2-dbb079b3a5f0)

Streaming Layer의 역할은 원본 데이터를 최대한 보존하는 것입니다. 

"최대한"이라는 표현을 사용한 이유는, 개인 식별 정보(PII)는 법적으로 저장할 수 없기 때문입니다. 만약 저장을 하게 되면 법적 문제가 발생할 수 있으므로, PII 정보는 익명화 또는 가명화 과정을 거친 후에 저장합니다.

또한 이상 탐지 모델을 적용 해 발생 된 플레이어의 로그가 정상적인 움직임과 비정상적인 움직임을 구별 해 불법 프로그램을 사용하는 "의심" 사용자를 예측을 수행합니다.

그렇기 떄문에 원본 데이터와 수집 데이터의 데이터 구조는 다음과 같이 다르게 구성되어 있습니다.

좌 : 원본 데이터 우 : 수집 데이터

![image](https://github.com/user-attachments/assets/224e3a07-6870-47bd-a499-8d156cd98e01)

IP는 삭제하고, 계정 이름은 SHA-256 해시 알고리즘을 적용 해 가명화를 진행했습니다. 또한 모델의 의심 사용자를 예측하는 prediction 필드가 추가되었습니다.

해당 로그를 다음과 같은 테이블로 구성합니다.

![image](https://github.com/user-attachments/assets/a97b72de-9bc4-408d-a558-0b7f1f2e63a8)

playerlogs는 처리 된 수집 데이터를 저장하는 테이블이고 invalid_data는 데이터 유효성 검사에 실패한 데이터가 저장되는 테이블 입니다.

두가지 모두 Table Format은 Delta Lake이며 create_room_date를 기준으로 Partitions 합니다. 이 과정은 모두 Bronze Layer에 속합니다.

테이블은 AWS Glue를 통해 관리됩니다.

----

다음 Batch Layer를 보겠습니다.

![image](https://github.com/user-attachments/assets/b7428264-73ab-4e88-9506-df4a5413b431)

총 3가지의 역할을 수행합니다.

1. Silver Layer
2. Gold Layer
3. ML Pipeline

먼저, Silver Layer의 역할은 Bronze Layer의 데이터를 조인 및 전처리를 통해 분석 및 학습에 사용 할 수 있는 데이터를 저장합니다.

![image](https://github.com/user-attachments/assets/df4fb946-fd25-4500-99d2-3e3b4b5c25e3)

분석용 테이블과 학습용 테이블로 구성됩니다. 데이터 분석가 또는 데이터 과학자는 이 데이터를 통해 분석 및 인사이트를 도출하고 새로운 테이블을 생성하거나 학습에 대한 여러 케이스를 사용 할 수 있도록 합니다.

다음은 Gold Layer로, 역할은 최종적으로 분석에 사용 될 데이터를 저장합니다.

![image](https://github.com/user-attachments/assets/b1786002-cfc2-4b4b-ac3a-ed324fc850a7)

분석에 사용 될 정보를 테이블로 저장하고 관리합니다.

다음은 ML Pipeline입니다.

![image](https://github.com/user-attachments/assets/8b4eed38-0872-4057-95f0-e5304263b0f5)

학습 테이블로 데이터를 읽고, K-Means 클러스터링으로 이상 탐지를 분류합니다. 학습이 완료 된 모델은 저장 후 추후에 사용할 수 있도록 합니다.

그 후 Streaming Layer와 동일하게 테이블을 AWS Glue로 관리합니다.

Streaming Layer와 다르게 Batch Layer는 Airflow로 스케줄링 해 효율적으로 운영을 지원하고 있습니다.

**Airflow**

![dag1](https://github.com/user-attachments/assets/bcb48ff3-8738-4ab4-bfbe-0771b75f0b81)
![dag2](https://github.com/user-attachments/assets/0a7aca50-d09c-454f-af79-ec9b95b9a424)

SparkBatch-Scheduling은 하루를 기준으로 실행이 되고, ML Pipeline은 한달을 기준으로 실행됩니다.

다음과 Task흐름을 가지고 있습니다.

**SparkBatch-Scheduling**
![Case1_5](https://github.com/user-attachments/assets/2e3b5ace-f7d0-4801-ab79-a68194b52544)

**ML-Pipeline**
![image](https://github.com/user-attachments/assets/1b91fee1-6dc1-4f82-8040-11d889c0f950)

## 2.2 데이터 서빙

![image](https://github.com/user-attachments/assets/6435b9ae-b409-425d-98f4-4e4d08a861ca)

데이터 처리가 완료 된 데이터를 활용할 수 있도록 데이터 소비자가 선호하는 노트북 환경 두가지를 제공 해 생산성과 효율성을 향상시켰습니다.

데이터 분석가 또는 데이터 과학자 그외는 선호하는 노트북 환경을 사용 할 수 있습니다

또한 코드에 익숙하지 않는 비전문적인 그룹, 즉 C-Level 또는 소비자들에게 클릭으로 손 쉬운 시각화를 지원 할 수 있도록 AWS QucikSight를 사용 해 지원하고 있습니다.

<img src="https://github.com/user-attachments/assets/c8af881c-8e39-4bd5-8e4a-2a55f92301d8" alt="퀵사이트" width="700"/>

## 2.3 Kafka Lag Monitoring

Kafka Lag는 운영 단계에서 중요한 지표로 사용됩니다.

Lag를 통해, 해당 토픽에 대한 파이프라인과 연계되어 있는 프로듀서와 컨슈머 상태를 유추할 수 있습니다.

![image](https://github.com/user-attachments/assets/985a50c8-9cf7-487d-b9de-9817c838b4b3)

모니터링 시스템을 구축하기 위해 다음과 같이 설계 및 구현했습니다.

1. Kafka exporter은 클러스터의 상태, 파티션 처리량, 레플리케이션의 상태 등 다양한 메트릭을 수집합니다.
2. Prometheus는 Kafka exporter가 수집하는 메트릭을 스크레이핑 방식으로 주기적으로 데이터를 가져옵니다.
3. Grafana는 Prometheus에 저장된 데이터를 기반으로 그래프와 대시보드를 생성하고 최종적으로 Kafka 클러스터의 상태를 모니터링합니다.

### 사용자가 100명일 때
<img src="https://github.com/user-attachments/assets/4dc6a621-b757-4026-8439-fb8ec5302472" alt="모니터링_시스템_100명" width="700"/>

| Message in per second| Message in per minute | Lag by Consumer Group |Message consume per minute|
| ------------- | ------------- |------------|-----------|
| 10~ 11.6  | 500 ~  657 | 5 ~ 23| 580 ~ 667|


### 사용자가 500명 일 때
<img src="https://github.com/user-attachments/assets/4337195c-b62e-4608-acdb-4bed3a5728ed" alt="모니터링_시스템_500명" width="700"/>

| Message in per second| Message in per minute | Lag by Consumer Group |Message consume per minute|
| ------------- | ------------- |------------|-----------|
| 25 ~ 30  | 1.5k ~ 1.7k | 30 ~ 48 | 1.64k|

이슈 : https://github.com/f-lab-edu/league-of-legends-data-solution/issues/26

PR: https://github.com/f-lab-edu/league-of-legends-data-solution/pull/30#issue-2512331053

## 2.4 Data Hub (DDP:Data Discovery Platform)

DataHub는 오픈 소스 메타데이터 관리 플랫폼으로, 다양한 데이터 소스에 대한 메타데이터를 중앙에서 관리하고, 이를 통해 데이터 발견, 계보 추적, 거버넌스 등의 기능을 제공합니다.

이를 통해 데이터 자산의 중앙 관리, 탐색이 용이하고, 계보 추적, 데이터 거버넌스 기능을 제공하여 조직 내 데이터 이해도를 높이는데 큰 도움을 줍니다.

### 메인화면
<img src="https://github.com/user-attachments/assets/6e0f6604-aec5-406b-b124-cc4770f55b26" alt="메인" width="500"/>


### DB
<img src="https://github.com/user-attachments/assets/bacb1a4c-58e7-44ac-be36-16ccd8badcd2" alt="화면1" width="700"/>
### 테이블
<img src="https://github.com/user-attachments/assets/dcde7c04-2721-4892-a83b-48313655f4c3" alt="화면2" width="700"/>

# 3.변경 사항

- [1.3.0](https://github.com/orgs/f-lab-edu/projects/284)
- [1.2.0](https://github.com/orgs/f-lab-edu/projects/270)
- [1.1.0](https://github.com/orgs/f-lab-edu/projects/263)
- [1.0.0](https://github.com/orgs/f-lab-edu/projects/249)

# 출처
league of legend_logo : https://www.leagueoflegends.com/en-us/