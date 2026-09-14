output "bucket_name" {
  description = "Name of the S3 bucket used as the lake landing zone"
  value       = aws_s3_bucket.lake.bucket
}

output "consumer_access_key_id" {
  description = "Access key ID for the Kafka consumer's IAM user"
  value       = aws_iam_access_key.consumer.id
}

output "consumer_secret_access_key" {
  description = "Secret access key for the Kafka consumer's IAM user"
  value       = aws_iam_access_key.consumer.secret
  sensitive   = true
}

output "role_arn" {
  value = module.aws_iam_databricks.role_arn
}