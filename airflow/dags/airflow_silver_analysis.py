from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from pendulum import yesterday

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test1234@naver.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
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
    "LogUri": "<s3:url>",
    "ReleaseLabel": "emr-6.8.0",
    "ServiceRole": "<aws:service-role>",
    "JobFlowRole": "<aws:jobflow-role>",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "ManagedScalingPolicy": {
        "ComputeLimits": {
            "UnitType": "Instances",
            "MinimumCapacityUnits": 1,
            "MaximumCapacityUnits": 5,
            "MaximumOnDemandCapacityUnits": 5,
            "MaximumCoreCapacityUnits": 5,
        }
    },
    "Instances": {
        "Ec2KeyName": "emr-pem",
        "Ec2SubnetId": "<aws:subnet-id>",
        "EmrManagedMasterSecurityGroup": "<aws:master_security_group>",
        "EmrManagedSlaveSecurityGroup": "<aws:slave_security_group>",
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
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Configurations": [
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
    ],
    "EbsRootVolumeSize": 30,
    "Tags": [{"Key": "Name", "Value": "airflow_silver_analysis"}],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
}

today = datetime.date.today()
yesterday = today - timedelta(days=1)

spark_steps = [
    {
        "Name": "spark_batch_silver_analysis",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                f"spark.config.partition.time={yesterday}",
                "s3://sjm-simple-data/app/spark_batch_silver_analysis.py",
            ],
        },
    },
    {
        "Name": "spark_batch_silver_layer_train",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                f"spark.config.partition.time={yesterday}",
                "s3://sjm-simple-data/app/spark_batch_silver_train.py",
            ],
        },
    },
    {
        "Name": "spark_batch_gold_layer",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                f"spark.config.partition.time={yesterday}",
                "s3://sjm-simple-data/app/spark_batch_gold.py",
            ],
        },
    },
]

with DAG(
    dag_id="airflow-emr-batch",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2024, 9, 7),
    schedule_interval="0 7 * * *",
) as dag:
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id=None,
        region_name="ap-northeast-2",
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
        steps=spark_steps,
        aws_conn_id="aws_default",
    )

    step_sensors = []
    for index in range(len(spark_steps)):
        sensor = EmrStepSensor(
            task_id=f"step_sensor_{index+1}",
            job_flow_id='{{ task_instance.xcom_pull(task_ids="create_emr_cluster", key="return_value") }}',
            step_id='{{ task_instance.xcom_pull(task_ids="add_spark_steps", key="return_value")[0] }}',
            aws_conn_id="aws_default",
        )
        step_sensors.append(sensor)

    create_cluster >> add_steps
    for sensor in step_sensors:
        add_steps >> sensor
