from datetime import timedelta, datetime


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test1234@naver.com"],
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
    "Name": "Airflow-Batch-Scheduling",
    "LogUri": "aws.log.uri",
    "ReleaseLabel": "emr-6.13.0",
    "ServiceRole": "aws.service.role",
    "JobFlowRole": "aws.service.role",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "ManagedScalingPolicy": {
        "ComputeLimits": {
            "UnitType": "Instances",
            "MinimumCapacityUnits": 2,
            "MaximumCapacityUnits": 5,
            "MaximumOnDemandCapacityUnits": 5,
            "MaximumCoreCapacityUnits": 5,
        }
    },
    "Instances": {
        "Ec2KeyName": "emr-pem",
        "Ec2SubnetId": "aws.subnet.id",
        "EmrManagedMasterSecurityGroup": "",
        "EmrManagedSlaveSecurityGroup": "",
        "ServiceAccessSecurityGroup": "",
        "AdditionalMasterSecurityGroups": [""],
        "AdditionalSlaveSecurityGroups": [""],
        "InstanceGroups": [
            {
                "Name": "",
                "InstanceRole": "MASTER",
                "InstanceType": "r5.xlarge",
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
                "InstanceType": "r5.xlarge",
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
            "Properties": {
                "delta.enabled": "true"
            },
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
                "yarn.scheduler.maximum-allocation-mb": "32000",
                "yarn.nodemanager.resource.memory-mb": "32000"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "false"
            }
        },
        {
                 "Classification": "spark-defaults",
                 "Properties": {
                   "spark.network.timeout": "800s",
                   "spark.executor.heartbeatInterval": "60s",
                   "spark.dynamicAllocation.enabled": "false",
                   "spark.driver.memory": "28G",
                   "spark.executor.memory": "28G",
                   "spark.executor.cores": "3",
                   "spark.executor.instances": "1",
                   "spark.executor.memoryOverhead": "3G",
                   "spark.driver.memoryOverhead": "3G",
                   "spark.memory.fraction": "0.80",
                   "spark.memory.storageFraction": "0.30",
                   "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.yarn.scheduler.reporterThread.maxFailures": "5",
                   "spark.storage.level": "MEMORY_AND_DISK_SER",
                   "spark.rdd.compress": "true",
                   "spark.shuffle.compress": "true",
                   "spark.shuffle.spill.compress": "true",
                   "spark.default.parallelism": "6"
                 }
        },
    ],
    "EbsRootVolumeSize": 30,
    "Tags": [{"Key": "Name", "Value": "airflow_silver_analysis"}],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
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
                    "s3://sjm-simple-data/app/silver_analysis_riot/playerlogs/spark_batch_silver_analysis.py",
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
                    "s3://sjm-simple-data/app/silver_train_riot/playerlogs/spark_batch_silver_train.py",
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
                    "s3://sjm-simple-data/app/gold_riot/average_death_and_time/average_death_and_time.py",
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
                    "s3://sjm-simple-data/app/gold_riot/champion_death_count/champion_death_count.py",
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
                    "s3://sjm-simple-data/app/gold_riot/key_used_per/key_used_per.py",
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
                    "s3://sjm-simple-data/app/gold_riot/room_end_time/room_end_time.py",
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


with DAG(
    dag_id="airflow-batch-scheduling",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2024, 9, 7),
    schedule_interval=None,
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
    generate_silver_analysis_riot = PythonOperator(
        task_id="generate_silver_analysis_riot",
        python_callable=get_logical_date,
        provide_context=True,
    )

    generate_silver_train_riot = PythonOperator(
        task_id="generate_silver_train_riot",
        python_callable=get_logical_date,
        provide_context=True,
    )

    generate_gold_riot = PythonOperator(
        task_id="generate_gold_riot",
        python_callable=get_logical_date,
        provide_context=True,
    )

    """
        3단계 : 분석 데이터에 대한 Batch 작업을 하는 그룹입니다.
    """
    with TaskGroup(group_id="analysis_group", prefix_group_id=False) as analysis_group:
        add_steps_1 = EmrAddStepsOperator(
            task_id="add_steps_1",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps='{{ task_instance.xcom_pull(task_ids="generate_silver_analysis_riot", key="spark_silver_analysis_steps") }}',
            aws_conn_id="aws_default",
        )

        step_sensor_1 = EmrStepSensor(
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
            steps='{{ task_instance.xcom_pull(task_ids="generate_silver_train_riot", key="spark_silver_train_steps") }}',
            aws_conn_id="aws_default",
        )

        step_sensor_2 = EmrStepSensor(
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
            steps='{{ task_instance.xcom_pull(task_ids="generate_gold_riot", key="spark_gold_steps") }}',
            aws_conn_id="aws_default",
        )

        gold_group_step_sensors = []
        for index in range(4):
            sensor = EmrStepSensor(
                task_id=f"step_sensor_3_{index + 1}",
                job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
                step_id='{{ task_instance.xcom_pull(task_ids="add_steps_3", key="return_value")[%d] }}'
                % index,
                aws_conn_id="aws_default",
            )

        for sensor in gold_group_step_sensors:
            add_steps_3 >> sensor

    """
        6단계 : EMR을 종료합니다
    """
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        aws_conn_id="aws_default",
    )

    create_cluster >> generate_silver_analysis_riot
    create_cluster >> generate_silver_train_riot

    generate_silver_analysis_riot >> analysis_group
    generate_silver_train_riot >> train_group

    analysis_group >> generate_gold_riot
    train_group >> generate_gold_riot

    generate_gold_riot >> gold_group

    gold_group >> terminate_emr_cluster
