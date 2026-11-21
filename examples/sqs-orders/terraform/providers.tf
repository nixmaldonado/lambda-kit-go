terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.54"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.region

  # Every resource gets these tags, so leftovers can be found by tag after
  # `terraform destroy`.
  default_tags {
    tags = {
      Project   = var.project_tag
      ManagedBy = "terraform"
    }
  }
}
