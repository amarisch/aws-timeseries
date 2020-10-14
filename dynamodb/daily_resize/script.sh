# code read up: https://aws.amazon.com/blogs/database/design-patterns-for-high-volume-time-series-data-in-amazon-dynamodb/
# install AWS SAM
pip install --user aws-sam-cli

# configure your AWS credentials
aws configure

# package your raw YAML template
sam package
    --template-file template.yml \
    --s3-bucket YOUR_BUCKET \
    --output-template-file compiled.yml

# deploy your compiled YAML template
sam deploy
    --template-file compiled.yml \
    --stack-name YOUR_STACK_NAME \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides TablePrefix=YOUR_PREFIX_
