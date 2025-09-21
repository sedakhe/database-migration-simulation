#!/usr/bin/env python3
"""
Test script to verify the database migration simulation setup.
This script tests the Python UDFs and validates the sample data.
"""

import json
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.flink_job.pyudf_email_domain import (
    extract_email_domain,
    extract_email_username,
    is_valid_email,
    normalize_email,
    categorize_email_domain,
    extract_country_from_postal_code,
    format_full_name
)


def test_email_udfs():
    """Test the email-related UDFs."""
    print("🧪 Testing Email UDFs...")
    
    test_cases = [
        ("user@example.com", "example.com", "user", True, "user@example.com", "corporate"),
        ("test.user@company.co.uk", "company.co.uk", "test.user", True, "test.user@company.co.uk", "corporate"),
        ("student@university.edu", "university.edu", "student", True, "student@university.edu", "educational"),
        ("person@gmail.com", "gmail.com", "person", True, "person@gmail.com", "personal"),
        ("invalid-email", None, None, False, None, "unknown"),
        ("", None, None, False, None, "unknown"),
        ("  User@Example.COM  ", "example.com", "user", True, "user@example.com", "corporate"),
    ]
    
    for email, expected_domain, expected_username, expected_valid, expected_normalized, expected_category in test_cases:
        domain = extract_email_domain(email)
        username = extract_email_username(email)
        valid = is_valid_email(email)
        normalized = normalize_email(email)
        category = categorize_email_domain(domain) if domain else "unknown"
        
        print(f"  Email: {email}")
        print(f"    Domain: {domain} (expected: {expected_domain}) {'✅' if domain == expected_domain else '❌'}")
        print(f"    Username: {username} (expected: {expected_username}) {'✅' if username == expected_username else '❌'}")
        print(f"    Valid: {valid} (expected: {expected_valid}) {'✅' if valid == expected_valid else '❌'}")
        print(f"    Normalized: {normalized} (expected: {expected_normalized}) {'✅' if normalized == expected_normalized else '❌'}")
        print(f"    Category: {category} (expected: {expected_category}) {'✅' if category == expected_category else '❌'}")
        print()


def test_postal_code_udf():
    """Test the postal code UDF."""
    print("🧪 Testing Postal Code UDF...")
    
    test_cases = [
        ("10001", "US"),  # US ZIP
        ("M5V 3A8", "CA"),  # Canadian postal
        ("SW1A 1AA", "GB"),  # UK postal
        ("12345", "DE"),  # German postal
        ("invalid", None),  # Invalid
        ("", None),  # Empty
    ]
    
    for postal_code, expected_country in test_cases:
        country = extract_country_from_postal_code(postal_code)
        print(f"  Postal Code: {postal_code} -> Country: {country} (expected: {expected_country}) {'✅' if country == expected_country else '❌'}")


def test_name_udf():
    """Test the name formatting UDF."""
    print("🧪 Testing Name UDF...")
    
    test_cases = [
        ("John", "Doe", "John Doe"),
        ("", "Smith", "Smith"),
        ("Jane", "", "Jane"),
        ("", "", ""),
        ("  John  ", "  Doe  ", "John Doe"),
    ]
    
    for first, last, expected in test_cases:
        result = format_full_name(first, last)
        print(f"  {first} + {last} -> {result} (expected: {expected}) {'✅' if result == expected else '❌'}")


def test_sample_data():
    """Test the sample data file."""
    print("🧪 Testing Sample Data...")
    
    try:
        with open('data/sample_events.json', 'r') as f:
            data = json.load(f)
        
        print(f"  ✅ Loaded {len(data)} records from sample_events.json")
        
        # Validate structure
        required_fields = ['id', 'first_name', 'last_name', 'email', 'cdc_operation']
        for i, record in enumerate(data[:3]):  # Check first 3 records
            for field in required_fields:
                if field not in record:
                    print(f"  ❌ Record {i} missing field: {field}")
                    return False
            print(f"  ✅ Record {i} has all required fields")
        
        # Test email validation on sample data
        valid_emails = 0
        for record in data:
            if is_valid_email(record.get('email', '')):
                valid_emails += 1
        
        print(f"  ✅ {valid_emails}/{len(data)} emails are valid")
        
        return True
        
    except FileNotFoundError:
        print("  ❌ Sample data file not found")
        return False
    except json.JSONDecodeError as e:
        print(f"  ❌ Invalid JSON in sample data: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Database Migration Simulation - Setup Test")
    print("=" * 50)
    
    # Test UDFs
    test_email_udfs()
    test_postal_code_udf()
    test_name_udf()
    
    # Test sample data
    if test_sample_data():
        print("✅ All tests passed!")
        print("\n🎉 Your setup is ready for the database migration simulation!")
        print("\nNext steps:")
        print("1. Run: ./start.sh")
        print("2. Submit the Flink job")
        print("3. Run the CDC producer")
    else:
        print("❌ Some tests failed. Please check the setup.")
        sys.exit(1)


if __name__ == '__main__':
    main()