

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, List

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = "stock-market-realtime_data"  # os.getenv('KAFKA_TOPIC_REALTIME')

# Define stocks with initial prices
STOCKS = {
    'AAPL': 180.0,
    'MSFT': 350.0,
    'GOOGL': 130.0,
    'AMZN': 130.0,
    'META': 300.0,
    'TSLA': 200.0,
    'NVDA': 400.0,
    'INTC': 35.0,
}


class StreamDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC, interval=30):
        self.logger = logger
        self.topic = topic
        self.interval = interval

        self.current_stocks = STOCKS.copy()

        # New: track today's low and high for each stock
        self.daily_stats = {
            symbol: {'low': price, 'high': price}
            for symbol, price in self.current_stocks.items()
        }

        self.producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'continuous-stock-data-producer'
        }

        try:
            self.producer = Producer(self.producer_conf)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}, Topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic()} [{msg.partition()}]")

    def generate_stock_data(self, symbol):
        current_price = self.current_stocks[symbol]

        market_factor = random.uniform(-0.005, 0.005)
        stock_factor = random.uniform(-0.005, 0.005)

        change_pct = market_factor + stock_factor

        if random.random() < 0.05:
            stock_factor += random.uniform(-0.02, 0.02)

        new_price = round(current_price * (1 + change_pct), 2)

        self.current_stocks[symbol] = new_price

        # Update daily low and high
        if new_price < self.daily_stats[symbol]['low']:
            self.daily_stats[symbol]['low'] = new_price
        if new_price > self.daily_stats[symbol]['high']:
            self.daily_stats[symbol]['high'] = new_price

        price_change = round(new_price - current_price, 2)
        percent_change = round((price_change / current_price) * 100, 2)

        volume = random.randint(1000, 100000)

        stock_data = {
            'symbol': symbol,
            'price': new_price,
            'change': price_change,
            'percent_change': percent_change,
            'volume': volume,
            'today_low': self.daily_stats[symbol]['low'],
            'today_high': self.daily_stats[symbol]['high'],
            'timestamp': datetime.now().isoformat()
        }

        return stock_data

    def produce_stock_data(self):
        self.logger.info(f"Starting continuous stock data production")

        try:
            while True:
                successful_symbols = 0
                failed_symbols = 0

                for symbol in self.current_stocks.keys():
                    try:
                        stock_data = self.generate_stock_data(symbol)

                        if stock_data:
                            message = json.dumps(stock_data)
                            self.producer.produce(
                                self.topic,
                                key=symbol,
                                value=message,
                                callback=self.delivery_report
                            )
                            self.producer.poll(0)
                            successful_symbols += 1
                        else:
                            failed_symbols += 1
                    except Exception as e:
                        self.logger.error(f"Error processing {symbol}: {e}")
                        failed_symbols += 1

                self.logger.info(f"Data Generation Summary: Successful: {successful_symbols}, Failed: {failed_symbols}")
                self.logger.info(f"Waiting {self.interval} seconds before next data generation...")
                time.sleep(self.interval)

        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.logger.info("Producer shutting down")
            self.producer.flush()


def main():
    try:
        producer = StreamDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            interval=2  # 2 seconds for faster demo
        )
        producer.produce_stock_data()
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
