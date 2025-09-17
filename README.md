#  📊Bitcoin Dashboard
## 📌Overview
This is an end-to-end data engineering project of Real-time Bitcoin Dashboard that uses Grafana, PostgreSQL, Kafka, and Amazon AWS in it's tech stack. 
It visualize in real-time and historical Bitcoin price data into price movement, trends, and changes. 

## Features
1. Scaleability, security, and cost efficiency: the tech stack, tools, and services that are used in this project are chosen with the consideration of these aspects.
2. Real-time price tracking (a bit delayed due to cost limitation): source data from API then distibute using Kafka
3. Market data visualization: candlestick, moving averages, price movement/changes
4. Custom alert system: Notification through email, Telegram, etc. 
## Tools & Architectures
Data sourcing and processing:
- Coincap API: data source
- Kafka : distributing data stream from Coincap API to consumer down stream client such as postgres database
- AWS Lambda, EventBridge, and SQS: these services work together to send data fetched from CoinCap API to Kafka consumer. Lambda works as the producer.

Containerization:
- Docker


## Dashboard Preview
## Repository Contents
## Use Cases
## Notes
## License

