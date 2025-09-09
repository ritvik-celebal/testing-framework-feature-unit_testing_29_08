"""
Pytest Configuration for Local PySpark Testing
==============================================

Provides Spark session fixture for local testing without mocking.
"""

import pytest
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local Spark session for testing.
    
    Returns a real SparkSession configured for local testing.
    """
    print("\n🔥 Creating local Spark session...")
    
    try:
        # Configure Spark for local testing
        conf = SparkConf() \
            .setAppName("LocalUtilsTesting") \
            .setMaster("local[2]") \
            .set("spark.driver.memory", "1g") \
            .set("spark.executor.memory", "1g") \
            .set("spark.sql.shuffle.partitions", "2") \
            .set("spark.ui.enabled", "false") \
            .set("spark.sql.adaptive.enabled", "true")
        
        # Create Spark session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
        
        print(f"✅ Spark session created successfully!")
        print(f"   Version: {spark.version}")
        print(f"   Master: {spark.sparkContext.master}")
        
        yield spark
        
        # Cleanup
        print("\n🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Spark session stopped")
        
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        print("   Make sure Java is installed and JAVA_HOME is set")
        pytest.skip("Spark session could not be created")


def pytest_configure(config):
    """Configure pytest for local testing"""
    print("\n⚙️ Configuring pytest for local testing...")
    
    # Check Java availability
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        print("⚠️ WARNING: JAVA_HOME not set.")
        print("   Spark tests will be skipped if Java is not available.")
    else:
        print(f"☕ Java found: {java_home}")


def pytest_runtest_setup(item):
    """Setup for each test run"""
    # Check if test needs Spark
    if "spark_session" in item.fixturenames:
        java_home = os.environ.get('JAVA_HOME')
        if not java_home:
            # Try to find java in PATH
            import shutil
            java_path = shutil.which('java')
            if not java_path:
                pytest.skip("Java not found - skipping Spark test")


@pytest.fixture
def temp_directory():
    """
    Function-scoped temporary directory fixture for EventHub testing.
    
    Provides a clean temporary directory for each test function.
    Automatically cleaned up after test completion.
    
    Returns:
        str: Path to temporary directory
    """
    import tempfile
    temp_dir = tempfile.mkdtemp(prefix="eventhub_test_")
    
    try:
        yield temp_dir
    finally:
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.fixture
def sample_json_data():
    """
    Fixture providing sample JSON data for EventHub testing.
    
    Returns:
        list: Sample JSON strings for validation testing
    """
    return [
        '{"OrderNo": "12345", "status": "active"}',
        '{"OrderNo": "67890", "product": "apple", "quantity": 10}',
        '{"OrderNo": "11111", "customer": "John Doe"}',
        '{"invalid": "no_order_number"}',  # Missing required field
        '{"OrderNo": "22222", "amount": 99.99}',
        'invalid_json_string',  # Malformed JSON
        '{"OrderNo": "33333", "timestamp": "2025-01-25T10:00:00Z"}',
    ]


@pytest.fixture
def sample_hubs():
    """
    Fixture providing sample hub names for EventHub testing.
    
    Returns:
        list: Sample hub names
    """
    return ["order_hub", "product_hub", "customer_hub", "payment_hub"]


@pytest.fixture
def mock_validators():
    """
    Fixture providing mock validators for EventHub testing.
    
    Returns:
        dict: Mock validator objects for each hub
    """
    class MockValidator:
        def __init__(self, required_fields=None):
            self.required_fields = required_fields or ["OrderNo"]
        
        def validate_instance(self, obj):
            if not isinstance(obj, dict):
                raise ValueError("Invalid JSON object")
            
            for field in self.required_fields:
                if field not in obj:
                    raise ValueError(f"Missing required field: {field}")
            
            return True
    
    return {
        "order_hub": MockValidator(["OrderNo"]),
        "product_hub": MockValidator(["OrderNo", "product"]),
        "customer_hub": MockValidator(["OrderNo", "customer"]),
        "payment_hub": MockValidator(["OrderNo", "amount"]),
    }
