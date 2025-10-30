#!/bin/bash

# Database setup script for PostgreSQL
# This script creates the database if it doesn't exist

echo "Setting up PostgreSQL database..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if PostgreSQL is running
if ! command -v psql &> /dev/null; then
    echo "ERROR: psql command not found. Please install PostgreSQL first."
    exit 1
fi

# Create database if it doesn't exist
echo "Creating database '${DB_DATABASE}' if it doesn't exist..."
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USERNAME}" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_DATABASE}'" | grep -q 1 || \
PGPASSWORD="${DB_PASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USERNAME}" -d postgres -c "CREATE DATABASE ${DB_DATABASE};"

if [ $? -eq 0 ]; then
    echo "Database '${DB_DATABASE}' ready!"

    # Initialize tables
    echo "Initializing database tables..."
    python3 database.py

    if [ $? -eq 0 ]; then
        echo "✓ Database setup complete!"
        exit 0
    else
        echo "ERROR: Failed to initialize tables"
        exit 1
    fi
else
    echo "ERROR: Failed to create database"
    exit 1
fi
