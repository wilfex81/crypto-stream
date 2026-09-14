output "role_arn" {
  description = "ARN to paste into the Databricks Unity Catalog storage credential config"
  value       = aws_iam_role.databricks_uc.arn
}

output "role_name" {
  value = aws_iam_role.databricks_uc.name
}
