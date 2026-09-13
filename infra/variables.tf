variable "aws_region" {
  description = "AWS region to provision resources in"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the data lake landing zone"
  type        = string
}
