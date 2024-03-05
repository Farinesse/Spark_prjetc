resource "aws_s3_bucket" "bucket" {
  bucket = "spark-farines"

  tags = local.tags
}