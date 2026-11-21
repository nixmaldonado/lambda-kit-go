output "region" {
  value = var.region
}

output "queue_url" {
  value = aws_sqs_queue.orders.url
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "idempotency_table" {
  value = aws_dynamodb_table.idempotency.name
}

output "ledger_table" {
  value = aws_dynamodb_table.ledger.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.function.name
}

output "project_tag" {
  value = var.project_tag
}
