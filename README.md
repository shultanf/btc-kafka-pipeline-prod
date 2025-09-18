#  📊Bitcoin Dashboard
## 📌Overview
This is an end-to-end data engineering project of Real-time Bitcoin Dashboard that uses Grafana, PostgreSQL, Kafka, and Amazon AWS in it's tech stack. 
It visualize in real-time and historical Bitcoin price data into price movement, trends, and changes. 

## Features
1. **Scaleability, security, and cost efficiency**: the tech stack, tools, and services that are used in this project are chosen with the consideration of these aspects.
2. **Real-time price tracking** (a bit delayed due to cost limitation): source data from API then distibute using Kafka
3. **Market data visualization**: candlestick, moving averages, price movement/changes
4. **Custom alert system**: Notification through email, Telegram, etc.

## Tools & Architectures
### Tools
Data sourcing and distribution:
- **Coincap API**: data source
- **Kafka** : distributing data stream from CoinCap API to storage (AWS S3, RDS Postgres)
- **AWS Lambda, EventBridge, and SQS**: Using Kafka producer hosted on Lambda, these services work together to fetch data source from CoinCap API then send it to Kafka consumer hosted on EC2 instance.

Receiving data and storage:
- **AWS EC2**: an EC2 instance act as a server and host the Kafka Consumer and Broker that receives data stream from Lambda producer which then store it in partition to an S3 Bucket.
- **AWS S3**: Raw data from Kafka Consumer (hosted on EC2 instance) are directly uploaded so an S3 bucket in partition.
- **AWS RDS PostgreSQL**: Data upload to the S3 bucket triggered Lambda function to upload/insert data into RDS Postgres.

Dashboard:
- **Grafana**: Sourcing data from the RDS postgres. Currently only accessible via Grafana snapshot link, in the future will be hosted on the web.

Containerization:
- **Docker**: containerized Kafka Consumer and Broker on the EC2 Instance.

### Architecture

## Dashboard Preview
[Grafana Snapshot ](https://bit.ly/BitcoinDashboardSF)
<img width="1440" height="726" alt="Screen Shot of the Dashboard" src="https://github.com/user-attachments/assets/dcaaac7b-ae87-4b04-9577-09ed1f62c106" />
## Repository Contents
## Use Cases
## Notes
## License

