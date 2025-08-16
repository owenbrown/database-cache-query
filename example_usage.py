#!/usr/bin/env python3
"""
Example usage of the database cache query system.

This script demonstrates:
1. Basic usage with mock data
2. JSON column parsing with dot notation
3. Batch processing with progress tracking
4. Cache hit/miss scenarios
5. Error handling
"""

import json
import random
from typing import List, Dict, Any
from main import get_data, DataNotFoundError, ColumnNotFoundError


def create_mock_database() -> Dict[str, Dict[int, Dict[str, Any]]]:
    """Create a mock database with sample data."""

    # Generate sample user data
    users_data = {}
    cities = ["NYC", "LA", "Chicago", "Boston", "Seattle", "Austin", "Denver", "Miami"]
    states = ["NY", "CA", "IL", "MA", "WA", "TX", "CO", "FL"]

    for i in range(1, 1001):  # 1000 users
        city = random.choice(cities)
        state = random.choice(states)

        users_data[i] = {
            "id": i,
            "name": f"User{i}",
            "email": f"user{i}@example.com",
            "age": random.randint(18, 80),
            "profile": json.dumps(
                {
                    "address": {
                        "city": city,
                        "state": state,
                        "zip": f"{random.randint(10000, 99999)}",
                    },
                    "preferences": {
                        "theme": random.choice(["light", "dark"]),
                        "notifications": random.choice([True, False]),
                    },
                    "stats": {
                        "login_count": random.randint(1, 1000),
                        "last_active": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                    },
                }
            ),
        }

    # Generate sample order data
    orders_data = {}
    for i in range(1, 501):  # 500 orders
        user_id = random.randint(1, 1000)
        orders_data[i] = {
            "id": i,
            "user_id": user_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(["pending", "completed", "cancelled"]),
            "metadata": json.dumps(
                {
                    "payment": {
                        "method": random.choice(
                            ["credit_card", "paypal", "bank_transfer"]
                        ),
                        "currency": "USD",
                    },
                    "shipping": {
                        "method": random.choice(["standard", "express", "overnight"]),
                        "tracking": f"TRK{random.randint(100000, 999999)}",
                    },
                }
            ),
        }

    return {"public.users": users_data, "public.orders": orders_data}


def create_mock_fetcher(mock_db: Dict[str, Dict[int, Dict[str, Any]]]):
    """Create a mock fetcher function that simulates database queries."""

    def mock_fetcher(ids: List[int], table_name: str) -> List[Dict[str, Any]]:
        """
        Mock fetcher function that simulates database queries.

        Args:
            ids: List of IDs to fetch
            table_name: Table name in format "schema.table"

        Returns:
            List of dictionaries containing the requested data
        """
        print(f"  🔍 Fetching {len(ids)} IDs from {table_name}")

        if table_name not in mock_db:
            raise Exception(f"Table {table_name} not found")

        table_data = mock_db[table_name]
        result = []

        for id in ids:
            if id in table_data:
                result.append(table_data[id])

        print(f"  ✅ Found {len(result)} records")
        return result

    return mock_fetcher


def example_basic_usage():
    """Demonstrate basic usage of get_data function."""
    print("\n" + "=" * 60)
    print("🚀 EXAMPLE 1: Basic Usage")
    print("=" * 60)

    # Create mock database and fetcher
    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # Request basic user data
    print("\n📋 Requesting basic user data for IDs 1-5...")
    result = get_data(
        ids=[1, 2, 3, 4, 5],
        columns=["name", "email", "age"],
        table_name="public.users",
        fetcher=fetcher,
    )

    print(f"\n📊 Results: {result.height} rows, {len(result.columns)} columns")
    print(result)


def example_json_parsing():
    """Demonstrate JSON field parsing with dot notation."""
    print("\n" + "=" * 60)
    print("🔍 EXAMPLE 2: JSON Field Parsing")
    print("=" * 60)

    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # Request data with JSON field extraction
    print("\n📋 Requesting user data with JSON field extraction...")
    result = get_data(
        ids=[1, 2, 3, 4, 5],
        columns=[
            "name",
            "profile.address.city",
            "profile.address.state",
            "profile.preferences.theme",
            "profile.stats.login_count",
        ],
        table_name="public.users",
        fetcher=fetcher,
    )

    print(
        f"\n📊 Results with JSON fields: {result.height} rows, {len(result.columns)} columns"
    )
    print(result)


def example_batch_processing():
    """Demonstrate batch processing with progress tracking."""
    print("\n" + "=" * 60)
    print("⚡ EXAMPLE 3: Batch Processing")
    print("=" * 60)

    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # Request a large number of records to trigger batching
    print("\n📋 Requesting 250 user records (will trigger batching)...")
    large_ids = list(range(1, 251))

    result = get_data(
        ids=large_ids,
        columns=["name", "email", "profile.address.city"],
        table_name="public.users",
        fetcher=fetcher,
    )

    print(f"\n📊 Batch processing results: {result.height} rows")
    print("Sample data:")
    print(result.head(10))


def example_cache_behavior():
    """Demonstrate cache hit/miss behavior."""
    print("\n" + "=" * 60)
    print("💾 EXAMPLE 4: Cache Behavior")
    print("=" * 60)

    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # First request - cache miss
    print("\n📋 First request (cache miss)...")
    result1 = get_data(
        ids=[10, 11, 12],
        columns=["name", "email"],
        table_name="public.users",
        fetcher=fetcher,
    )
    print(f"First request: {result1.height} rows")

    # Second request - cache hit
    print("\n📋 Second request for same data (cache hit)...")
    result2 = get_data(
        ids=[10, 11, 12],
        columns=["name", "email"],
        table_name="public.users",
        fetcher=fetcher,
    )
    print(f"Second request: {result2.height} rows (should be from cache)")

    # Third request - partial cache hit
    print("\n📋 Third request with some new IDs (partial cache hit)...")
    result3 = get_data(
        ids=[11, 12, 13, 14],  # 11, 12 in cache; 13, 14 need fetching
        columns=["name", "email"],
        table_name="public.users",
        fetcher=fetcher,
    )
    print(f"Third request: {result3.height} rows")


def example_error_handling():
    """Demonstrate error handling scenarios."""
    print("\n" + "=" * 60)
    print("⚠️  EXAMPLE 5: Error Handling")
    print("=" * 60)

    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # Test 1: Missing IDs
    print("\n🔍 Testing missing IDs error...")
    try:
        get_data(
            ids=[9999, 10000],  # IDs that don't exist
            columns=["name", "email"],
            table_name="public.users",
            fetcher=fetcher,
        )
    except DataNotFoundError as e:
        print(f"✅ Caught expected error: {e}")

    # Test 2: Missing columns
    print("\n🔍 Testing missing columns error...")

    def limited_fetcher(ids: List[int], table_name: str) -> List[Dict[str, Any]]:
        # Return data without some requested columns
        return [{"id": id, "name": f"User{id}"} for id in ids]

    try:
        get_data(
            ids=[1, 2],
            columns=["name", "nonexistent_column"],
            table_name="public.users",
            fetcher=limited_fetcher,
        )
    except ColumnNotFoundError as e:
        print(f"✅ Caught expected error: {e}")

    # Test 3: Input validation
    print("\n🔍 Testing input validation...")
    try:
        get_data(
            ids=[1, "invalid", 3],  # Invalid ID type
            columns=["name"],
            table_name="public.users",
            fetcher=fetcher,
        )
    except ValueError as e:
        print(f"✅ Caught expected error: {e}")


def example_multiple_tables():
    """Demonstrate working with multiple tables."""
    print("\n" + "=" * 60)
    print("🗂️  EXAMPLE 6: Multiple Tables")
    print("=" * 60)

    mock_db = create_mock_database()
    fetcher = create_mock_fetcher(mock_db)

    # Get user data
    print("\n📋 Getting user data...")
    users = get_data(
        ids=[1, 2, 3],
        columns=["name", "email", "profile.address.city"],
        table_name="public.users",
        fetcher=fetcher,
    )
    print(f"Users: {users.height} rows")
    print(users)

    # Get order data
    print("\n📋 Getting order data...")
    orders = get_data(
        ids=[1, 2, 3, 4, 5],
        columns=["user_id", "amount", "status", "metadata.payment.method"],
        table_name="public.orders",
        fetcher=fetcher,
    )
    print(f"Orders: {orders.height} rows")
    print(orders)


def main():
    """Run all examples."""
    print("🎯 Database Cache Query System - Example Usage")
    print("This example demonstrates the key features of the system.")

    try:
        example_basic_usage()
        example_json_parsing()
        example_batch_processing()
        example_cache_behavior()
        example_error_handling()
        example_multiple_tables()

        print("\n" + "=" * 60)
        print("🎉 All examples completed successfully!")
        print("=" * 60)
        print("\n💡 Key takeaways:")
        print("  • The system automatically handles caching and batch processing")
        print("  • JSON fields can be accessed using dot notation")
        print("  • Progress tracking shows batch processing in action")
        print("  • Cache hits avoid unnecessary database queries")
        print("  • Comprehensive error handling for various scenarios")
        print("  • Multiple tables can be cached independently")

    except Exception as e:
        print(f"\n❌ Example failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
