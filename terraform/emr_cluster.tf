provider "aws" {
  region = "ap-northeast-2"
}

resource "aws_emr_cluster" "cluster" {
  name = aws_cluster.name
  release_label = "emr-6.8.0"
  applications = ["Spark", "Hadoop", "JupyterHub", "Trino", "Zeppelin"]
  log_uri = aws_s3.bucket

  service_role = aws_iam_role.iam_emr_service_role.arn

  unhealthy_node_replacement = false
  termination_protection = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    subnet_id                         = aws_subnet.main.id
    emr_managed_master_security_group = aws_security_group.sg.id
    emr_managed_slave_security_group  = aws_security_group.sg.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
    key_name = aws_key
  }

  master_instance_group {
    instance_type = "m5.xlarge"
    instance_count = 1
    ebs_config {
      size = "32"
      type = "gp2"
      volumes_per_instance = 2
    }
  }

  core_instance_group {
    instance_type = "m5.xlarge"
    instance_count = 1
    ebs_config {
      size = "32"
      type = "gp2"
      volumes_per_instance = 2
    }
  }

  configurations_json = <<EOF
  [
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

  scale_down_behavior = "TERMINATE_AT_TASK_COMPLETION"
  ebs_root_volume_size = "30"
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
      name   = "instance-state-name"
      values = ["running"]
    }

    filter {
      name   = "tag:aws:elasticmapreduce:instance-group-role"
      values = ["CORE"]
    }

    filter {
      name   = "tag:aws:elasticmapreduce:job-flow-id"
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
