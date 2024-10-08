provider "aws" {
  region = "ap-northeast-2"
}

resource "aws_emr_cluster" "cluster" {
  name          = aws_cluster.name
  release_label = "emr-6.13.0"
  applications = ["Spark", "Hadoop", "JupyterHub", "Zeppelin"]
  log_uri       = aws_s3.bucket

  service_role = aws_iam_role.iam_emr_service_role.arn

  unhealthy_node_replacement        = false
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    subnet_id                         = aws_subnet.main.id
    emr_managed_master_security_group = aws_security_group.sg.id
    emr_managed_slave_security_group  = aws_security_group.sg.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
    key_name                          = aws_key
  }

  master_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
    ebs_config {
      size                 = "32"
      type                 = "gp3"
      volumes_per_instance = 2
    }
  }

  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
    ebs_config {
      size                 = "32"
      type                 = "gp3"
      volumes_per_instance = 2
    }
  }

  configurations_json = <<EOF
  [
     {
      "Classification": "delta-defaults",
      "Properties": {
        "delta.enabled": "true"
      }
    },
     {
      "Classification": "spark",
      "Properties": {
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "10",
        "spark.dynamicAllocation.executorIdleTimeout": "60"
      }
    },
    {
      "Classification" : "hive-site",
      "Properties" : {
         "hive.metastore.client.factory.class" : "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      },
       "Configurations" : []
    },
    {
      "Classification": "spark-hive-site",
      "Properties" : {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      },
      "Configurations" : []
    }
  ]
EOF

  scale_down_behavior  = "TERMINATE_AT_TASK_COMPLETION"
  ebs_root_volume_size = "30"
}

resource "aws_emr_managed_scaling_policy" "auto_scale_policy" {
  cluster_id = aws_emr_cluster.cluster.id
  compute_limits {
    maximum_capacity_units          = 2
    minimum_capacity_units          = 1
    maximum_ondemand_capacity_units = 2
    maximum_core_capacity_units     = 2
    unit_type                       = "Instances"
  }
}

data "aws_instance" "primary" {
  filter {
    name = "instance-state-name"
    values = ["running"]
  }

  filter {
    name = "tag:aws:elasticmapreduce:instance-group-role"
    values = ["MASTER"]
  }

  filter {
    name = "tag:aws:elasticmapreduce:job-flow-id"
    values = [aws_emr_cluster.cluster.id]
  }

  depends_on = [aws_emr_cluster.cluster]
}

data "aws_instances" "core" {
  filter {
    name = "instance-state-name"
    values = ["running"]
  }

  filter {
    name = "tag:aws:elasticmapreduce:instance-group-role"
    values = ["CORE"]
  }

  filter {
    name = "tag:aws:elasticmapreduce:job-flow-id"
    values = [aws_emr_cluster.cluster.id]
  }

  depends_on = [aws_emr_cluster.cluster]
}


output "emr_primary_public_ip" {
  value = data.aws_instance.primary.public_ip
}

output "emr_core_public_ips" {
  value = data.aws_instances.core.public_ips
}
