figlet -c -t Seeding Localstack

awslocal sqs create-queue --queue-name local-queue
awslocal sqs create-queue --queue-name local-queue-dl

figlet -c -t Done