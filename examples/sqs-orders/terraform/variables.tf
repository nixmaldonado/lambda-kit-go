variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Prefix for resource names."
  type        = string
  default     = "lkg-sqs-orders"
}

variable "project_tag" {
  description = "Value of the Project tag on every resource."
  type        = string
  default     = "lambda-kit-go-e2e"
}
