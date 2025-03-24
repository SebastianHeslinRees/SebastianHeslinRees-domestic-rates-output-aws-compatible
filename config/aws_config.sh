#!/bin/bash

# Set AWS credentials as environment variables (DO NOT hardcode sensitive values here)
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_DEFAULT_REGION=""

# Authenticate Docker to ECR
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 637423545604.dkr.ecr.eu-west-2.amazonaws.com

echo "AWS credentials set successfully."





