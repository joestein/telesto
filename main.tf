variable "single_table_name" {
  type = string
  default = "pogo-main"
}

resource "aws_dynamodb_table" "pogo-main" {
  name           = var.single_table_name
  
  billing_mode     = "PAY_PER_REQUEST"
  hash_key       = "PK"
  range_key      = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  attribute {
    name = "GPK1"
    type = "S"
  }

  attribute {
    name = "GSK1"
    type = "S"
  }

  # before you have data disapear on you 
  # 
  #ttl {
  #  attribute_name = "TimeToExist"
  #  enabled        = false
  #}

  global_secondary_index {
    name               = "GPSK1"
    hash_key           = "GPK1"
    range_key          = "GSK1"
    projection_type    = "INCLUDE"
    non_key_attributes = ["PK", "SK", "label", "id"]
  }

  tags = {
    Name        = var.single_table_name
  }
}