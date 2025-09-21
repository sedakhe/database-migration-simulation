#!/usr/bin/env python3
"""
Python UDF for Flink SQL - Email Domain Extraction

This module provides a Python User Defined Function (UDF) for Apache Flink
that extracts the domain from an email address. This is used in the CDC
migration pipeline to enrich user data.
"""

import re
from typing import Optional
from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def extract_email_domain(email: str) -> Optional[str]:
    """
    Extract the domain from an email address.
    
    Args:
        email: Email address string
        
    Returns:
        Domain part of the email address, or None if invalid
        
    Examples:
        >>> extract_email_domain("user@example.com")
        "example.com"
        >>> extract_email_domain("test@subdomain.company.co.uk")
        "subdomain.company.co.uk"
        >>> extract_email_domain("invalid-email")
        None
    """
    if not email or not isinstance(email, str):
        return None
    
    # Basic email validation regex
    email_pattern = r'^[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'
    
    match = re.match(email_pattern, email.strip())
    if match:
        return match.group(1)
    
    return None


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def extract_email_username(email: str) -> Optional[str]:
    """
    Extract the username part from an email address.
    
    Args:
        email: Email address string
        
    Returns:
        Username part of the email address, or None if invalid
        
    Examples:
        >>> extract_email_username("user@example.com")
        "user"
        >>> extract_email_username("test.user@company.com")
        "test.user"
    """
    if not email or not isinstance(email, str):
        return None
    
    # Basic email validation regex
    email_pattern = r'^([a-zA-Z0-9._%+-]+)@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    match = re.match(email_pattern, email.strip())
    if match:
        return match.group(1)
    
    return None


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.BOOLEAN())
def is_valid_email(email: str) -> bool:
    """
    Validate if an email address is properly formatted.
    
    Args:
        email: Email address string
        
    Returns:
        True if email is valid, False otherwise
        
    Examples:
        >>> is_valid_email("user@example.com")
        True
        >>> is_valid_email("invalid-email")
        False
    """
    if not email or not isinstance(email, str):
        return False
    
    # More comprehensive email validation regex
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    return bool(re.match(email_pattern, email.strip()))


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def normalize_email(email: str) -> Optional[str]:
    """
    Normalize an email address by converting to lowercase and trimming.
    
    Args:
        email: Email address string
        
    Returns:
        Normalized email address, or None if invalid
        
    Examples:
        >>> normalize_email("  User@Example.COM  ")
        "user@example.com"
    """
    if not email or not isinstance(email, str):
        return None
    
    normalized = email.strip().lower()
    
    # Validate the normalized email
    if is_valid_email(normalized):
        return normalized
    
    return None


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def categorize_email_domain(domain: str) -> str:
    """
    Categorize email domain based on common patterns.
    
    Args:
        domain: Email domain string
        
    Returns:
        Category of the domain
        
    Examples:
        >>> categorize_email_domain("gmail.com")
        "personal"
        >>> categorize_email_domain("company.com")
        "corporate"
        >>> categorize_email_domain("university.edu")
        "educational"
    """
    if not domain or not isinstance(domain, str):
        return "unknown"
    
    domain_lower = domain.lower()
    
    # Personal email providers
    personal_domains = {
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
        'icloud.com', 'protonmail.com', 'yandex.com', 'mail.com'
    }
    
    # Educational domains
    if domain_lower.endswith('.edu'):
        return "educational"
    
    # Government domains
    if domain_lower.endswith('.gov'):
        return "government"
    
    # Military domains
    if domain_lower.endswith('.mil'):
        return "military"
    
    # Personal email providers
    if domain_lower in personal_domains:
        return "personal"
    
    # Non-profit organizations
    if domain_lower.endswith('.org'):
        return "nonprofit"
    
    # Default to corporate
    return "corporate"


# Additional utility functions for data enrichment

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def extract_country_from_postal_code(postal_code: str) -> Optional[str]:
    """
    Extract country information from postal code format.
    
    Args:
        postal_code: Postal code string
        
    Returns:
        Inferred country code, or None if cannot determine
        
    Examples:
        >>> extract_country_from_postal_code("10001")
        "US"
        >>> extract_country_from_postal_code("M5V 3A8")
        "CA"
    """
    if not postal_code or not isinstance(postal_code, str):
        return None
    
    postal_code = postal_code.strip().upper()
    
    # US ZIP codes (5 digits or 5+4 format)
    if re.match(r'^\d{5}(-\d{4})?$', postal_code):
        return "US"
    
    # Canadian postal codes
    if re.match(r'^[A-Z]\d[A-Z] \d[A-Z]\d$', postal_code):
        return "CA"
    
    # UK postal codes (simplified pattern)
    if re.match(r'^[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}$', postal_code):
        return "GB"
    
    # German postal codes
    if re.match(r'^\d{5}$', postal_code):
        return "DE"
    
    # French postal codes
    if re.match(r'^\d{5}$', postal_code):
        return "FR"
    
    return None


@udf(input_types=[DataTypes.STRING(), DataTypes.STRING()], result_type=DataTypes.STRING())
def format_full_name(first_name: str, last_name: str) -> str:
    """
    Format first and last name into a full name.
    
    Args:
        first_name: First name
        last_name: Last name
        
    Returns:
        Formatted full name
        
    Examples:
        >>> format_full_name("John", "Doe")
        "John Doe"
        >>> format_full_name("", "Smith")
        "Smith"
    """
    if not first_name and not last_name:
        return ""
    
    if not first_name:
        return last_name.strip()
    
    if not last_name:
        return first_name.strip()
    
    return f"{first_name.strip()} {last_name.strip()}"


# Register all UDFs for use in Flink SQL
def register_udfs(table_env):
    """
    Register all UDFs with the Flink table environment.
    
    Args:
        table_env: Flink table environment
    """
    table_env.create_temporary_function("extract_email_domain", extract_email_domain)
    table_env.create_temporary_function("extract_email_username", extract_email_username)
    table_env.create_temporary_function("is_valid_email", is_valid_email)
    table_env.create_temporary_function("normalize_email", normalize_email)
    table_env.create_temporary_function("categorize_email_domain", categorize_email_domain)
    table_env.create_temporary_function("extract_country_from_postal_code", extract_country_from_postal_code)
    table_env.create_temporary_function("format_full_name", format_full_name)


if __name__ == "__main__":
    # Test the UDFs
    print("Testing Email Domain UDFs:")
    
    test_emails = [
        "user@example.com",
        "test.user@company.co.uk",
        "invalid-email",
        "  User@Example.COM  ",
        "",
        None
    ]
    
    for email in test_emails:
        print(f"Email: {email}")
        print(f"  Domain: {extract_email_domain(email)}")
        print(f"  Username: {extract_email_username(email)}")
        print(f"  Valid: {is_valid_email(email)}")
        print(f"  Normalized: {normalize_email(email)}")
        if extract_email_domain(email):
            print(f"  Category: {categorize_email_domain(extract_email_domain(email))}")
        print()