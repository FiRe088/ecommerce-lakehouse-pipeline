terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

resource "aws_s3_bucket" "iceberg_warehouse" {
  bucket = "ecommerce-lakehouse-iceberg-warehouse-fire088"

  tags = {
    Project     = "ecommerce-lakehouse-pipeline"
    Purpose     = "Iceberg warehouse storage"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "iceberg_warehouse_versioning" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg_warehouse_block" {
  bucket = aws_s3_bucket.iceberg_warehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}