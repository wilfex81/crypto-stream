variable "aws_region" {
  description = "AWS region to provision resources in"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the data lake landing zone"
  type        = string
}

variable "databricks_cross_account_role_arn" {
  description = "Databricks' fixed cross-account master role ARN"
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "external_id" {
  description = "Placeholder for first apply; replace with real value after creating the UC storage credential"
  type        = string
  default     = "0000"
}
