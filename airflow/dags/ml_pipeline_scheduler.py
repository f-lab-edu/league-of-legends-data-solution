from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync
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
    "Name": "ML-Batch-Scheduling",
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
    "Tags": [{"Key": "Name", "Value": "airflow_silver_analysis"}],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
}

ml_steps = [
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

with DAG(
    dag_id="ML-PipeLine",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2024, 10, 1),
    schedule_interval="@monthly",
    catchup=False,
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

    monitor_emr_cluster = EmrJobFlowSensor(
        task_id="monitor_emr_cluster",
        job_flow_id=create_cluster.output,
        aws_conn_id="aws_default",
        target_states="WAITING",
    )

    with TaskGroup(group_id="ml_group", prefix_group_id=False) as ml_group:
        ml_step = EmrAddStepsOperator(
            task_id="ml_step",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            aws_conn_id="aws_default",
            steps=ml_steps,
        )

        ml_sensor = EmrStepSensorAsync(
            task_id="ml_sensor",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="ml_step", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )

        ml_step >> ml_sensor

    """
        6단계 : EMR을 종료합니다
    """
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        aws_conn_id="aws_default",
    )

create_cluster >> monitor_emr_cluster >> ml_group >> terminate_emr_cluster
