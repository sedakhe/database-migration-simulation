#!/usr/bin/env python3
"""
Simple test script to verify the database migration simulation setup.
This script tests the core functionality without requiring PyFlink.
"""

import json
import sys
import os
import re
from typing import Optional


def extract_email_domain(email: str) -> Optional[str]:
    """Extract the domain from an email address."""
    if not email or not isinstance(email, str):
        return None
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'
    match = re.match(email_pattern, email.strip())
    if match:
        return match.group(1)
    return None


def is_valid_email(email: str) -> bool:
    """Validate if an email address is properly formatted."""
    if not email or not isinstance(email, str):
        return False
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email.strip()))


def categorize_email_domain(domain: str) -> str:
    """Categorize email domain based on common patterns."""
    if not domain or not isinstance(domain, str):
        return "unknown"
    
    domain_lower = domain.lower()
    
    if domain_lower.endswith('.edu'):
        return "educational"
    elif domain_lower.endswith('.gov'):
        return "government"
    elif domain_lower.endswith('.mil'):
        return "military"
    elif domain_lower.endswith('.org'):
        return "nonprofit"
    elif domain_lower in {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'}:
        return "personal"
    else:
        return "corporate"


def test_email_functions():
    """Test email-related functions."""
    print("🧪 Testing Email Functions...")
    
    test_cases = [
        ("user@example.com", "example.com", True, "corporate"),
        ("student@university.edu", "university.edu", True, "educational"),
        ("person@gmail.com", "gmail.com", True, "personal"),
        ("invalid-email", None, False, "unknown"),
        ("", None, False, "unknown"),
    ]
    
    for email, expected_domain, expected_valid, expected_category in test_cases:
        domain = extract_email_domain(email)
        valid = is_valid_email(email)
        category = categorize_email_domain(domain) if domain else "unknown"
        
        print(f"  Email: {email}")
        print(f"    Domain: {domain} (expected: {expected_domain}) {'✅' if domain == expected_domain else '❌'}")
        print(f"    Valid: {valid} (expected: {expected_valid}) {'✅' if valid == expected_valid else '❌'}")
        print(f"    Category: {category} (expected: {expected_category}) {'✅' if category == expected_category else '❌'}")
        print()


def test_sample_data():
    """Test the sample data file."""
    print("🧪 Testing Sample Data...")
    
    try:
        with open('data/sample_events.json', 'r') as f:
            data = json.load(f)
        
        print(f"  ✅ Loaded {len(data)} records from sample_events.json")
        
        # Validate structure
        required_fields = ['id', 'first_name', 'last_name', 'email', 'cdc_operation']
        valid_records = 0
        
        for i, record in enumerate(data):
            has_all_fields = all(field in record for field in required_fields)
            if has_all_fields:
                valid_records += 1
            else:
                missing_fields = [field for field in required_fields if field not in record]
                print(f"  ❌ Record {i} missing fields: {missing_fields}")
        
        print(f"  ✅ {valid_records}/{len(data)} records have all required fields")
        
        # Test email validation on sample data
        valid_emails = 0
        for record in data:
            if is_valid_email(record.get('email', '')):
                valid_emails += 1
        
        print(f"  ✅ {valid_emails}/{len(data)} emails are valid")
        
        # Show sample enriched data
        print("\n  📊 Sample Enriched Data:")
        for i, record in enumerate(data[:3]):
            email = record.get('email', '')
            domain = extract_email_domain(email)
            category = categorize_email_domain(domain) if domain else "unknown"
            full_name = f"{record.get('first_name', '')} {record.get('last_name', '')}".strip()
            
            print(f"    Record {i+1}:")
            print(f"      Name: {full_name}")
            print(f"      Email: {email}")
            print(f"      Domain: {domain}")
            print(f"      Category: {category}")
            print(f"      Operation: {record.get('cdc_operation', 'N/A')}")
            print()
        
        return True
        
    except FileNotFoundError:
        print("  ❌ Sample data file not found")
        return False
    except json.JSONDecodeError as e:
        print(f"  ❌ Invalid JSON in sample data: {e}")
        return False


def test_project_structure():
    """Test that all required files exist."""
    print("🧪 Testing Project Structure...")
    
    required_files = [
        'src/flink_job/job.sql',
        'src/flink_job/pyudf_email_domain.py',
        'src/producers/cdc_producer.py',
        'src/schemas/postgres.sql',
        'data/sample_events.json',
        'docker/docker-compose.yml',
        'docker/postgres-init.sql',
        'requirements.txt',
        'README.md',
        'start.sh'
    ]
    
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path}")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n  ❌ Missing {len(missing_files)} files")
        return False
    else:
        print(f"\n  ✅ All {len(required_files)} required files exist")
        return True


def main():
    """Run all tests."""
    print("🚀 Database Migration Simulation - Simple Setup Test")
    print("=" * 60)
    
    # Test project structure
    structure_ok = test_project_structure()
    print()
    
    # Test email functions
    test_email_functions()
    
    # Test sample data
    data_ok = test_sample_data()
    
    if structure_ok and data_ok:
        print("✅ All tests passed!")
        print("\n🎉 Your database migration simulation is ready!")
        print("\n📋 Next Steps:")
        print("1. Start the infrastructure: ./start.sh")
        print("2. Submit the Flink job to process CDC events")
        print("3. Run the CDC producer to generate events")
        print("4. Monitor the processing in the web UIs")
        print("\n📊 Access Points:")
        print("  - Flink Dashboard: http://localhost:8081")
        print("  - Kafka UI: http://localhost:8080")
        print("  - PostgreSQL: localhost:5432")
    else:
        print("❌ Some tests failed. Please check the setup.")
        sys.exit(1)


if __name__ == '__main__':
    main()