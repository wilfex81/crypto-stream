variable "role_name" {
  description = "Name of the IAM role Databricks assumes for lake access"
  type        = string
  default     = "databricks-unity-catalog-crypto-stream-access"
}

variable "bucket_arn" {
  description = "ARN of the S3 lake bucket to grant access to"
  type        = string
}

variable "databricks_cross_account_role_arn" {
  description = "Databricks' own cross-account IAM role ARN (from workspace/UC storage credential setup), e.g. arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-XXXX"
  type        = string
}

variable "external_id" {
  description = "External ID for the assume-role trust policy, provided by Databricks when creating the storage credential"
  type        = string
}

variable "tags" {
  description = "Tags applied to all resources in this module"
  type        = map(string)
  default     = {}
}
