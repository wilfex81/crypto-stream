# Cross-account role that Databricks (Unity Catalog storage credential) assumes
# to read/write the lake bucket. Follows Databricks' documented UC storage
# credential pattern: Databricks' AWS account assumes this role, scoped down
# with an external_id you generate when you first create the storage
# credential in the workspace.
#
# Since Jan 2025 Databricks requires "self-assuming" roles — the trust policy
# must list BOTH Databricks' master role ARN AND this role's own ARN. That's
# why the principal list below includes both.
#
# NOTE: this is a two-step process in practice —
#   1. apply this module once with external_id = "0000" (placeholder) to
#      create the role
#   2. create the Unity Catalog storage credential in Databricks, pointing at
#      this role's ARN (output below) — Databricks gives you back the real
#      external_id
#   3. re-apply with the real external_id
# That's normal and expected, not a mistake in the config.

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "databricks_uc" {
  name = var.role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = [
            var.databricks_cross_account_role_arn,
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.role_name}"
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.external_id
          }
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_policy" "crypto_stream_access" {
  name = "${var.role_name}-crypto-stream-access"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "BucketAccess"
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation",
          "s3:GetBucketNotification",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutBucketNotification"
        ]

        Resource = var.bucket_arn
      },

      {
        Sid    = "ObjectAccess"
        Effect = "Allow"

        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListMultipartUploadParts",
          "s3:PutObject"
        ]

        Resource = "${var.bucket_arn}/*"
      }
    ]
  })
}


resource "aws_iam_policy" "crypto_stream_file_events" {
  name = "${var.role_name}-file-events"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ManagedFileEventsSetup"
        Effect = "Allow"

        Action = [
          "sns:ListSubscriptionsByTopic",
          "sns:GetTopicAttributes",
          "sns:SetTopicAttributes",
          "sns:CreateTopic",
          "sns:TagResource",
          "sns:Publish",
          "sns:Subscribe",

          "sqs:CreateQueue",
          "sqs:DeleteMessage",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
          "sqs:GetQueueUrl",
          "sqs:GetQueueAttributes",
          "sqs:SetQueueAttributes",
          "sqs:TagQueue",
          "sqs:ChangeMessageVisibility",
          "sqs:PurgeQueue"
        ]

        Resource = [
          "arn:aws:sns:*:*:csms-*",
          "arn:aws:sqs:*:*:csms-*"
        ]
      },

      {
        Sid    = "ManagedFileEventsList"
        Effect = "Allow"

        Action = [
          "sns:ListTopics",
          "sqs:ListQueues",
          "sqs:ListQueueTags"
        ]

        Resource = [
          "arn:aws:sns:*:*:csms-*",
          "arn:aws:sqs:*:*:csms-*"
        ]
      },

      {
        Sid    = "ManagedFileEventsTeardown"
        Effect = "Allow"

        Action = [
          "sns:Unsubscribe",
          "sns:DeleteTopic",
          "sqs:DeleteQueue"
        ]

        Resource = [
          "arn:aws:sns:*:*:csms-*",
          "arn:aws:sqs:*:*:csms-*"
        ]
      }
    ]
  })
}


# Attach S3 permissions to the Databricks UC role
resource "aws_iam_role_policy_attachment" "crypto_stream_access" {
  role       = aws_iam_role.databricks_uc.name
  policy_arn = aws_iam_policy.crypto_stream_access.arn
}


# Attach File Events permissions to the SAME role
resource "aws_iam_role_policy_attachment" "crypto_stream_file_events" {
  role       = aws_iam_role.databricks_uc.name
  policy_arn = aws_iam_policy.crypto_stream_file_events.arn
}