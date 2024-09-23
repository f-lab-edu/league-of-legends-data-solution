from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.utils.task_group import TaskGroup
from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync
from datetime import timedelta, datetime


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
    "Name": "Backup-Batch-Scheduling",
    "LogUri": "<aws.emr.log.uri>",
    "ReleaseLabel": "emr-6.13.0",
    "ServiceRole": "<aws.emr.service.role>",
    "JobFlowRole": "<aws.emr.job.flow.role>",
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
        "Ec2SubnetId": "<aws.subnet.id>",
        "EmrManagedMasterSecurityGroup": "<emr.group>",
        "EmrManagedSlaveSecurityGroup": "<emr.group>",
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
                "yarn.scheduler.maximum-allocation-mb": "32000",
                "yarn.nodemanager.resource.memory-mb": "32000",
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
                "spark.default.parallelism": "6",
            },
        },
    ],
    "EbsRootVolumeSize": 30,
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
}


def generate_backup(logical_date):

    date_str = logical_date.strftime("%Y-%m-%d")

    backup_steps = [
        {
            "Name": "spark_batch_partition_backup",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    "s3://sjm-simple-data/app/bronze_riot/backup.py",
                    date_str,
                ],
            },
        }
    ]

    return backup_steps


def get_logical_date(**kwargs):
    """
    Airflow의 logical_date를 가져와서 여러 Spark 작업을 정의한 후 Xcom을 통해 공유합니다.
    :param kwargs: Airflow 컨텍스트에서 제공하는 인자
    :return: 없음 xCom을 통해 각 Spark 작업의 steps를 전달합니다.
    """
    logical_date = kwargs["logical_date"]

    ti = kwargs["ti"]

    ti.xcom_push(key="backup_steps", value=generate_backup(logical_date))


with DAG(
    dag_id="Backup-Scheduler",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2024, 9, 7),
    schedule_interval="59 23 L * *",
    catchup=False,
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id=None,
        region_name="ap-northeast-2",
    )

    generate_backup_task = PythonOperator(
        task_id="generate_backup_task",
        python_callable=get_logical_date,
        provide_context=True,
    )

    with TaskGroup(group_id="backup_group", prefix_group_id=False) as backup_group:
        add_steps_1 = EmrAddStepsOperator(
            task_id="add_steps_1",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            steps='{{ task_instance.xcom_pull(task_ids="generate_backup_task", key="backup_steps") }}',
            aws_conn_id="aws_default",
        )

        step_sensor_1 = EmrStepSensorAsync(
            task_id="step_sensor_1",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="add_steps_1", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )

        add_steps_1 >> step_sensor_1

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        aws_conn_id="aws_default",
    )

    create_cluster >> generate_backup_task

    generate_backup_task >> backup_group

    backup_group >> terminate_emr_cluster
