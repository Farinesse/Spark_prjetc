resource "aws_glue_job" "aws_glue_job_fari" {
  name     = "aws_glue_job_fari"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/sparks-jobs/exo2/exo2_glue_job.py"
  }

  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "Standard"
  
  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "s3://${aws_s3_bucket.bucket.bucket}/data/exo2/city_zipcode.csv"
    "--PARAM_2"                         = "s3://${aws_s3_bucket.bucket.bucket}/data/exo2/clients_bdd.csv"
    "--PARAM_3"                         = "s3://${aws_s3_bucket.bucket.bucket}/source/exo2/"

  }
  tags = local.tags
}