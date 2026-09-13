terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 bucket: the data lake landing zone ---

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name

  tags = {
    Project = "crypto-stream-lakehouse"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- IAM: scoped access for the Kafka consumer to write raw trade data ---

resource "aws_iam_user" "consumer" {
  name = "crypto-stream-consumer"
}

resource "aws_iam_access_key" "consumer" {
  user = aws_iam_user.consumer.name
}

data "aws_iam_policy_document" "consumer_policy" {
  statement {
    sid    = "WriteRawTrades"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/raw/*",
    ]
  }
}

resource "aws_iam_user_policy" "consumer" {
  name   = "crypto-stream-consumer-s3-access"
  user   = aws_iam_user.consumer.name
  policy = data.aws_iam_policy_document.consumer_policy.json
}
