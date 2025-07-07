#!/usr/bin/env python3
"""
Test script to demonstrate the suppression of INFO logs during earthaccess login.
"""

import logging
import os

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set up test environment variables (you would use your actual credentials)
# os.environ["EARTHDATA_USERNAME"] = "your_username"
# os.environ["EARTHDATA_PASSWORD"] = "your_password"

# Or use the test skip environment variable
os.environ["SKIP_EARTHDATA_LOGIN"] = "true"

try:
    # Import your login function
    from harmonized_landsat_sentinel.login import login
    
    print("Testing login function with suppressed earthaccess INFO logs...")
    auth = login()
    print(f"Login successful. Authenticated: {auth.authenticated}")
    
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure harmonized_landsat_sentinel is installed or in the Python path")
except Exception as e:
    print(f"Error during login: {e}")
