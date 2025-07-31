#!/usr/bin/env python3
"""
Script to connect to PostgreSQL and inspect Fitbit health data
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Create connection string
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def connect_to_db():
    """Create and return database engine"""
    engine = create_engine(connection_string)
    return engine

def inspect_database(engine):
    """Inspect database schema and tables"""
    inspector = inspect(engine)
    
    print("Available tables:")
    tables = inspector.get_table_names()
    for table in tables:
        print(f"  - {table}")
    
    return tables

def preview_table(engine, table_name, limit=5):
    """Preview a table's data"""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
        rows = result.fetchall()
        columns = list(result.keys())
        
        print(f"\nTable: {table_name}")
        print(f"Columns: {columns}")
        print(f"Rows fetched: {len(rows)}")
        print("Sample data:")
        for i, row in enumerate(rows):
            print(f"  Row {i+1}: {dict(zip(columns, row))}")
        
        return rows, columns

if __name__ == "__main__":
    try:
        engine = connect_to_db()
        print("Connected to database successfully!")
        
        # Inspect database
        tables = inspect_database(engine)
        
        # Preview each table
        for table in tables[:3]:  # Preview first 3 tables
            preview_table(engine, table)
            print("-" * 50)
            
    except Exception as e:
        print(f"Error connecting to database: {e}")