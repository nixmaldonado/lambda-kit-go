locals {
  function_timeout = 10
  # AWS recommends at least 6x the function timeout for Lambda SQS sources,
  # so messages waiting for a throttled invocation don't time out and burn
  # a receive.
  visibility_timeout = 60
}

data "archive_file" "function" {
  type        = "zip"
  source_file = "${path.module}/../build/bootstrap"
  output_path = "${path.module}/../build/function.zip"
}

# --- Queues ------------------------------------------------------------------

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "orders" {
  name                       = "${var.name}-orders"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 86400

  # A message that fails 5 receives (e.g. invalid JSON) goes to the DLQ.
  # At least 5, as AWS recommends: ErrInProgress retries and throttling also
  # use up receives, and a valid order must not reach the DLQ that way.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

# --- Tables ------------------------------------------------------------------

# The idempotency store: string key "id", TTL on "expiration".
resource "aws_dynamodb_table" "idempotency" {
  name         = "${var.name}-idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  ttl {
    attribute_name = "expiration"
    enabled        = true
  }
}

# The ledger the function "charges": one counter per order.
resource "aws_dynamodb_table" "ledger" {
  name         = "${var.name}-ledger"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "order_id"

  attribute {
    name = "order_id"
    type = "S"
  }
}

# --- Function ----------------------------------------------------------------

# Created here, not by Lambda on first run, so `terraform destroy` removes it.
resource "aws_cloudwatch_log_group" "function" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 1
}

resource "aws_iam_role" "function" {
  name = "${var.name}-function"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Least privilege: exactly the permissions the README documents.
resource "aws_iam_role_policy" "function" {
  name = "${var.name}-function"
  role = aws_iam_role.function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Idempotency"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem"]
        Resource = aws_dynamodb_table.idempotency.arn
      },
      {
        Sid      = "Ledger"
        Effect   = "Allow"
        Action   = ["dynamodb:UpdateItem"]
        Resource = aws_dynamodb_table.ledger.arn
      },
      {
        Sid      = "Queue"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.orders.arn
      },
      {
        # No logs:CreateLogGroup: the function can only write to the
        # Terraform-managed group above.
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.function.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "function" {
  function_name    = var.name
  role             = aws_iam_role.function.arn
  runtime          = "provided.al2023"
  handler          = "bootstrap"
  architectures    = ["arm64"]
  filename         = data.archive_file.function.output_path
  source_code_hash = data.archive_file.function.output_base64sha256
  timeout          = local.function_timeout
  memory_size      = 128

  environment {
    variables = {
      IDEMPOTENCY_TABLE = aws_dynamodb_table.idempotency.name
      LEDGER_TABLE      = aws_dynamodb_table.ledger.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.function, aws_iam_role_policy.function]
}

resource "aws_lambda_event_source_mapping" "orders" {
  event_source_arn = aws_sqs_queue.orders.arn
  function_name    = aws_lambda_function.function.arn
  batch_size       = 10

  # Without this, Lambda ignores the batch package's failure report and
  # deletes failed messages.
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 10
  }
}
