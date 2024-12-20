from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["thdwpals8975@naver.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-Integrated-Batch-Scheduling",
    "LogUri": "s3://aws-logs-172984108503-ap-northeast-2/elasticmapreduce",
    "ReleaseLabel": "emr-6.13.0",
    "ServiceRole": "arn:aws:iam::172984108503:role/test",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "ManagedScalingPolicy": {
        "ComputeLimits": {
            "UnitType": "Instances",
            "MinimumCapacityUnits": 1,
            "MaximumCapacityUnits": 5,
            "MaximumOnDemandCapacityUnits": 2,
            "MaximumCoreCapacityUnits": 2,
        }
    },
    "Instances": {
        "Ec2KeyName": "emr-pem",
        "Ec2SubnetId": "subnet-0369baa26a9585c67",
        "EmrManagedMasterSecurityGroup": "sg-0add401863d9ba4ec",
        "EmrManagedSlaveSecurityGroup": "sg-05d4edc7a41e7b357",
        "ServiceAccessSecurityGroup": "",
        "AdditionalMasterSecurityGroups": [""],
        "AdditionalSlaveSecurityGroups": [""],
        "InstanceGroups": [
            {
                "Name": "",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp3",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        }
                    ]
                },
            },
            {
                "Name": "",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp3",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        }
                    ]
                },
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Configurations": [
        {
            "Classification": "delta-defaults",
            "Properties": {"delta.enabled": "true"},
        },
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.vmem-check-enabled": "false",
                "yarn.nodemanager.pmem-check-enabled": "false",
                "yarn.scheduler.maximum-allocation-mb": "16000",
                "yarn.nodemanager.resource.memory-mb": "16000",
            },
        },
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "false"},
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.network.timeout": "800s",
                "spark.executor.heartbeatInterval": "60s",
                "spark.dynamicAllocation.enabled": "false",
                "spark.driver.memory": "4G",
                "spark.executor.memory": "4G",
                "spark.executor.cores": "2",
                "spark.executor.instances": "1",
                "spark.executor.memoryOverhead": "1G",
                "spark.driver.memoryOverhead": "1G",
                "spark.memory.fraction": "0.80",
                "spark.memory.storageFraction": "0.30",
                "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                "spark.yarn.scheduler.reporterThread.maxFailures": "5",
                "spark.storage.level": "MEMORY_AND_DISK_SER",
                "spark.rdd.compress": "true",
                "spark.shuffle.compress": "true",
                "spark.shuffle.spill.compress": "true",
                "spark.default.parallelism": "6",
            },
        },
    ],
    "EbsRootVolumeSize": 30,
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "StepConcurrencyLevel":4
}


def generate_silver_riot_analysis(logical_date):
    """
    Airflow의 logical_date를 기준으로 Spark 작업을 정의하는 EMR step을 생성합니다.
    이 작업은 지정된 logical_date를 매개변수로 사용하여 spark_batch_silver_analysis.py를 실행하는 Spark 작업을 설정합니다.
    :param logical_date: Airflow에서 제공하는 DAG의 실행 날짜
    :return: 실행된 logical_date에 대한 Spark 작업을 수행하는 EMR Step
    """
    date_str = logical_date.strftime("%Y-%m-%d")

    silver_analysis_riot_steps = [
        {
            "Name": "spark_batch_silver_layer_analysis",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "4G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.sql.shuffle.partitions=4",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=400MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/silver_analysis_riot/playerlogs/delta_spark_batch_silver_analysis.py",
                    date_str,
                ],
            },
        }
    ]

    return silver_analysis_riot_steps


def generate_silver_riot_train(logical_date):
    """
    Airflow의 logical_date를 기준으로 Spark 작업을 정의하는 EMR step을 생성합니다.
    이 작업은 지정된 logical_date를 매개변수로 사용하여 spark_batch_silver_train.py를 실행하는 Spark 작업을 설정합니다.
    :param logical_date: Airflow에서 제공하는 DAG의 실행 날짜
    :return: 실행된 logical_date에 대한 Spark 작업을 수행하는 EMR Step
    """
    date_str = logical_date.strftime("%Y-%m-%d")

    silver_train_riot_steps = [
        {
            "Name": "spark_batch_silver_train",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "4G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.sql.shuffle.partitions=6",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=400MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/silver_train_riot/playerlogs/delta_spark_batch_silver_train_v2.py",
                    date_str,
                ],
            },
        }
    ]

    return silver_train_riot_steps

def generate_gold_riot_analysis(logical_date):
    """
    Airflow의 logical_date를 기준으로 Spark 작업을 정의하는 EMR step을 생성합니다.
    이 작업은 지정된 logical_date를 매개변수로 사용하여 4가지를 실행하는 Spark 작업을 설정합니다.
    :param logical_date: Airflow에서 제공하는 DAG의 실행 날짜
    :return: 실행된 logical_date에 대한 Spark 작업을 수행하는 EMR Step
    """
    date_str = logical_date.strftime("%Y-%m-%d")

    gold_riot_steps = [
        {
            "Name": "spark_batch_gold_layer_average_death_and_time",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "2G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=200MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/gold_riot/average_death_and_time/delta_average_death_and_time.py",
                    date_str,
                ],
            },
        },
        {
            "Name": "spark_batch_gold_layer_champion_death_count",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "2G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=200MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/gold_riot/champion_death_count/delta_champion_death_count.py",
                    date_str,
                ],
            },
        },
        {
            "Name": "spark_batch_gold_layer_key_used_per",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "2G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=200MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/gold_riot/key_used_per/delta_key_used_per.py",
                    date_str,
                ],
            },
        },
        {
            "Name": "spark_batch_gold_layer_room_end_time",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "--driver-memory",
                    "2G",
                    "--executor-cores",
                    "1",
                    "--executor-memory",
                    "2G",
                    "--num-executors",
                    "1",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.executor.memoryOverhead=200MB",
                    "--conf",
                    "spark.driver.memoryOverhead=200MB",
                    "s3://sjm-simple-data/app/gold_riot/room_end_time/delta_room_end_time.py",
                    date_str,
                ],
            },
        },
    ]

    return gold_riot_steps


def get_logical_date(**kwargs):
    """
    Airflow의 logical_date를 가져와서 여러 Spark 작업을 정의한 후 Xcom을 통해 공유합니다.
    :param kwargs: Airflow 컨텍스트에서 제공하는 인자
    :return: 없음 xCom을 통해 각 Spark 작업의 steps를 전달합니다.
    """
    logical_date = kwargs["logical_date"]

    ti = kwargs["ti"]

    ti.xcom_push(
        key="spark_silver_analysis_steps",
        value=generate_silver_riot_analysis(logical_date),
    )
    ti.xcom_push(
        key="spark_silver_train_steps", value=generate_silver_riot_train(logical_date)
    )
    ti.xcom_push(
        key="spark_gold_steps", value=generate_gold_riot_analysis(logical_date)
    )

def decide_branch(**kwargs):
    logical_date = kwargs['logical_date']
    if logical_date.day == 1:
        return "ml_group"
    else :
        return "terminate_emr_cluster"

with DAG(
    dag_id="Airflow-Integrated-Batch-Scheduling",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_tasks=4
) as dag:
    """
    1단계 : EMR을 생성합니다.
    """
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id=None,
        region_name="ap-northeast-2",
    )
    """
        2단계 : logical_date를 기준으로 Spark 작업을 정의합니다.
    """
    get_logical_date_task = PythonOperator(
        task_id="get_logical_date_task",
        python_callable=get_logical_date,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable=decide_branch,
        provide_context=True,
    )

    monitor_emr_cluster = EmrJobFlowSensor(
        task_id="monitor_emr_cluster",
        job_flow_id=create_cluster.output,
        aws_conn_id="aws_default",
        target_states="WAITING",
    )

    """
        3단계 : 분석 데이터에 대한 Batch 작업을 하는 그룹입니다.
    """
    with TaskGroup(group_id="analysis_group", prefix_group_id=False) as analysis_group:
        add_steps_1 = EmrAddStepsOperator(
            task_id="add_steps_1",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps='{{ task_instance.xcom_pull(task_ids="get_logical_date_task", key="spark_silver_analysis_steps") }}',
            aws_conn_id="aws_default",
        )

        step_sensor_1 = EmrStepSensorAsync(
            task_id="step_sensor_1",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="add_steps_1", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )

        add_steps_1 >> step_sensor_1

    """
       4단계 : 학습 데이터에 대한 Batch 작업을 하는 그룹입니다.
    """
    with TaskGroup(group_id="train_group", prefix_group_id=False) as train_group:
        add_steps_2 = EmrAddStepsOperator(
            task_id="add_steps_2",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps='{{ task_instance.xcom_pull(task_ids="get_logical_date_task", key="spark_silver_train_steps") }}',
            aws_conn_id="aws_default",
        )

        step_sensor_2 = EmrStepSensorAsync(
            task_id="step_sensor_2",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="add_steps_2", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )

        add_steps_2 >> step_sensor_2

    """
       5단계 : Gold Layer의 최종 분석 데이터에 대한 Batch 작업을 하는 그룹입니다.
    """
    with TaskGroup(group_id="gold_group", prefix_group_id=False) as gold_group:
        add_steps_3 = EmrAddStepsOperator(
            task_id="add_steps_3",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps='{{ task_instance.xcom_pull(task_ids="get_logical_date_task", key="spark_gold_steps") }}',
            aws_conn_id="aws_default",
        )

        gold_group_step_sensors = []

        for index in range(4):
            sensor = EmrStepSensorAsync(
                task_id=f"step_sensor_3_{index + 1}",
                job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
                step_id='{{ task_instance.xcom_pull(task_ids="add_steps_3", key="return_value")[%d] }}'
                % index,
                aws_conn_id="aws_default",
            )
            gold_group_step_sensors.append(sensor)

        add_steps_3 >> gold_group_step_sensors

    with TaskGroup(group_id="ml_group", prefix_group_id=False) as ml_group :

        ml_pipeline_steps = [
            {
                "Name": "spark_batch_ml_pipeline",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--master",
                        "yarn",
                        "--driver-memory",
                        "2G",
                        "--executor-cores",
                        "1",
                        "--executor-memory",
                        "2G",
                        "--num-executors",
                        "2",
                        "--conf",
                        "spark.dynamicAllocation.enabled=false",
                        "--conf",
                        "spark.executor.memoryOverhead=200MB",
                        "--conf",
                        "spark.driver.memoryOverhead=200MB",
                        "s3://sjm-simple-data/app/ML/kmeans-pipeline.py",
                    ],
                },
            }
        ]

        ml_steps = EmrAddStepsOperator(
            task_id="ml_steps",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps=ml_pipeline_steps,
            aws_conn_id="aws_default",
        )

        ml_step_sensor = EmrStepSensorAsync(
            task_id="ml_step_sensor",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="ml_steps", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )

        ml_steps >> ml_step_sensor

    """
        6단계 : EMR을 종료합니다
    """
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        aws_conn_id="aws_default",
        trigger_rule="one_success"
    )

    create_cluster >> monitor_emr_cluster

    monitor_emr_cluster >> [analysis_group, train_group]

    analysis_group >> gold_group >> branch_task
    train_group >> branch_task

    branch_task >> ml_group
    branch_task >> terminate_emr_cluster

    ml_group >> terminate_emr_cluster

