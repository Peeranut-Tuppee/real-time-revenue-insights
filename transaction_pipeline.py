# Complete Transaction Data Pipeline System
# Requirements: pip install psycopg2-binary kafka-python streamlit pandas numpy requests schedule

import psycopg2
from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import time
import threading
import schedule
import requests
from typing import Dict, List
import logging
import streamlit as st
from flask import Flask, jsonify
import plotly.express as px
import plotly.graph_objects as go

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'transaction_db',
    'user': 'postgres',
    'password': 'password',
    'port': '5432'
}

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'fx_topic': 'fx_rates',
    'transaction_topic': 'transactions'
}

class DatabaseManager:
    """Handles PostgreSQL database operations"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.config)
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            
    def create_tables(self):
        """Create necessary tables"""
        cursor = self.connection.cursor()
        
        # Transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                amount DECIMAL(15, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                amount_usd DECIMAL(15, 2),
                country VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # FX Rates table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fx_rates (
                id SERIAL PRIMARY KEY,
                currency VARCHAR(3) NOT NULL,
                rate_to_usd DECIMAL(10, 6) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Processed transactions table for analytics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                amount DECIMAL(15, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                amount_usd DECIMAL(15, 2) NOT NULL,
                country VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL,
                fx_rate DECIMAL(10, 6) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.connection.commit()
        cursor.close()
        logger.info("Tables created successfully")
        
    def insert_transaction(self, transaction: dict):
        """Insert raw transaction"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO transactions (transaction_id, amount, currency, country, user_id, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            transaction['transaction_id'],
            transaction['amount'],
            transaction['currency'],
            transaction['country'],
            transaction['user_id'],
            transaction['timestamp']
        ))
        self.connection.commit()
        cursor.close()
        
    def insert_fx_rate(self, fx_data: dict):
        """Insert FX rate"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO fx_rates (currency, rate_to_usd, timestamp)
            VALUES (%s, %s, %s)
        """, (
            fx_data['currency'],
            fx_data['rate'],
            fx_data['timestamp']
        ))
        self.connection.commit()
        cursor.close()
        
    def insert_processed_transaction(self, transaction: dict):
        """Insert processed transaction with USD conversion"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO processed_transactions 
            (transaction_id, amount, currency, amount_usd, country, user_id, fx_rate, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            transaction['transaction_id'],
            transaction['amount'],
            transaction['currency'],
            transaction['amount_usd'],
            transaction['country'],
            transaction['user_id'],
            transaction['fx_rate'],
            transaction['timestamp']
        ))
        self.connection.commit()
        cursor.close()
        
    def get_latest_fx_rate(self, currency: str) -> float:
        """Get latest FX rate for currency"""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT rate_to_usd FROM fx_rates 
            WHERE currency = %s 
            ORDER BY timestamp DESC 
            LIMIT 1
        """, (currency,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 1.0

class TransactionGenerator:
    """Generates mock transaction data"""
    
    def __init__(self):
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'THB', 'SGD', 'AUD', 'CAD']
        self.countries = ['US', 'UK', 'Germany', 'Japan', 'Thailand', 'Singapore', 'Australia', 'Canada']
        
    def generate_transaction(self) -> dict:
        """Generate a single transaction"""
        return {
            'transaction_id': f'TXN_{random.randint(100000, 999999)}_{int(time.time())}',
            'amount': round(random.uniform(10, 5000), 2),
            'currency': random.choice(self.currencies),
            'country': random.choice(self.countries),
            'user_id': f'USER_{random.randint(1000, 9999)}',
            'timestamp': datetime.now()
        }
        
    def generate_batch(self, size: int = 50) -> List[dict]:
        """Generate batch of transactions"""
        return [self.generate_transaction() for _ in range(size)]

class FXRateGenerator:
    """Generates mock FX rate data"""
    
    def __init__(self):
        # Base rates (approximate)
        self.base_rates = {
            'USD': 1.0,
            'EUR': 0.85,
            'GBP': 0.75,
            'JPY': 110.0,
            'THB': 35.0,
            'SGD': 1.35,
            'AUD': 1.45,
            'CAD': 1.25
        }
        
    def generate_fx_rates(self) -> List[dict]:
        """Generate FX rates with realistic fluctuations"""
        rates = []
        for currency, base_rate in self.base_rates.items():
            if currency != 'USD':
                # Add some random fluctuation (±2%)
                fluctuation = random.uniform(-0.02, 0.02)
                rate = base_rate * (1 + fluctuation)
                rates.append({
                    'currency': currency,
                    'rate': round(rate, 6),
                    'timestamp': datetime.now()
                })
        return rates

class KafkaManager:
    """Handles Kafka operations"""
    
    def __init__(self, config: dict):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )
        
    def send_fx_rates(self, fx_rates: List[dict]):
        """Send FX rates to Kafka"""
        for rate in fx_rates:
            self.producer.send(self.config['fx_topic'], rate)
        self.producer.flush()
        
    def send_transaction(self, transaction: dict):
        """Send transaction to Kafka"""
        self.producer.send(self.config['transaction_topic'], transaction)
        self.producer.flush()

class DataPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.kafka = KafkaManager(KAFKA_CONFIG)
        self.transaction_gen = TransactionGenerator()
        self.fx_gen = FXRateGenerator()
        self.running = False
        
    def setup(self):
        """Initialize the pipeline"""
        self.db.connect()
        self.db.create_tables()
        logger.info("Pipeline setup completed")
        
    def fx_rate_consumer(self):
        """Consume FX rates from Kafka and store in DB"""
        consumer = KafkaConsumer(
            KAFKA_CONFIG['fx_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        for message in consumer:
            if not self.running:
                break
            fx_data = message.value
            self.db.insert_fx_rate(fx_data)
            logger.info(f"Stored FX rate: {fx_data['currency']} = {fx_data['rate']}")
            
    def transaction_processor(self):
        """Process transactions and convert to USD"""
        consumer = KafkaConsumer(
            KAFKA_CONFIG['transaction_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        for message in consumer:
            if not self.running:
                break
            transaction = message.value
            
            # Store raw transaction
            self.db.insert_transaction(transaction)
            
            # Get FX rate and convert to USD
            if transaction['currency'] == 'USD':
                fx_rate = 1.0
                amount_usd = transaction['amount']
            else:
                fx_rate = self.db.get_latest_fx_rate(transaction['currency'])
                amount_usd = transaction['amount'] / fx_rate
            
            # Store processed transaction
            processed_transaction = {
                **transaction,
                'amount_usd': round(amount_usd, 2),
                'fx_rate': fx_rate
            }
            self.db.insert_processed_transaction(processed_transaction)
            logger.info(f"Processed transaction: {transaction['transaction_id']}")
            
    def generate_fx_rates_job(self):
        """Job to generate and send FX rates"""
        fx_rates = self.fx_gen.generate_fx_rates()
        self.kafka.send_fx_rates(fx_rates)
        logger.info(f"Generated {len(fx_rates)} FX rates")
        
    def generate_transactions_job(self):
        """Job to generate and send transactions"""
        batch = self.transaction_gen.generate_batch(random.randint(20, 100))
        for transaction in batch:
            self.kafka.send_transaction(transaction)
        logger.info(f"Generated {len(batch)} transactions")
        
    def start(self):
        """Start the pipeline"""
        self.running = True
        
        # Start consumers in separate threads
        fx_consumer_thread = threading.Thread(target=self.fx_rate_consumer)
        transaction_processor_thread = threading.Thread(target=self.transaction_processor)
        
        fx_consumer_thread.start()
        transaction_processor_thread.start()
        
        # Schedule jobs
        schedule.every(30).seconds.do(self.generate_fx_rates_job)  # FX rates every 30 seconds
        schedule.every(60).seconds.do(self.generate_transactions_job)  # Transactions every minute
        
        # Run scheduled jobs
        while self.running:
            schedule.run_pending()
            time.sleep(1)
            
    def stop(self):
        """Stop the pipeline"""
        self.running = False

# API Server
app = Flask(__name__)

@app.route('/api/revenue/24h')
def get_24h_revenue():
    """Get total revenue in last 24 hours"""
    db = DatabaseManager(DB_CONFIG)
    db.connect()
    
    cursor = db.connection.cursor()
    cursor.execute("""
        SELECT SUM(amount_usd) as total_revenue
        FROM processed_transactions
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """)
    result = cursor.fetchone()
    cursor.close()
    
    return jsonify({
        'total_revenue_usd': float(result[0]) if result[0] else 0,
        'period': '24_hours'
    })

@app.route('/api/revenue/by_country')
def get_revenue_by_country():
    """Get revenue breakdown by country"""
    db = DatabaseManager(DB_CONFIG)
    db.connect()
    
    cursor = db.connection.cursor()
    cursor.execute("""
        SELECT country, SUM(amount_usd) as revenue
        FROM processed_transactions
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY country
        ORDER BY revenue DESC
    """)
    results = cursor.fetchall()
    cursor.close()
    
    return jsonify([{
        'country': row[0],
        'revenue_usd': float(row[1])
    } for row in results])

@app.route('/api/transactions/hourly')
def get_hourly_transactions():
    """Get hourly transaction activity"""
    db = DatabaseManager(DB_CONFIG)
    db.connect()
    
    cursor = db.connection.cursor()
    cursor.execute("""
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as transaction_count,
            SUM(amount_usd) as revenue_usd
        FROM processed_transactions
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY hour
        ORDER BY hour
    """)
    results = cursor.fetchall()
    cursor.close()
    
    return jsonify([{
        'hour': row[0].isoformat(),
        'transaction_count': row[1],
        'revenue_usd': float(row[2])
    } for row in results])

if __name__ == "__main__":
    # Example usage
    pipeline = DataPipeline()
    pipeline.setup()
    
    print("Starting data pipeline...")
    print("Available API endpoints:")
    print("- GET /api/revenue/24h")
    print("- GET /api/revenue/by_country") 
    print("- GET /api/transactions/hourly")
    
    # Start pipeline in a separate thread
    pipeline_thread = threading.Thread(target=pipeline.start)
    pipeline_thread.start()
    
    # Start API server
    app.run(host='0.0.0.0', port=5000, debug=True)