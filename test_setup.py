#!/usr/bin/env python3
"""Test script to validate the STT E2E Insights pipeline setup."""

import sys
import os
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def test_imports():
    """Test that all modules can be imported successfully."""
    print("🔍 Testing module imports...")
    
    try:
        # Change to src directory for relative imports
        import os
        original_dir = os.getcwd()
        os.chdir(src_path)
        
        # Test utility imports
        from utils.config_loader import ConfigLoader
        from utils.logger import setup_logging, get_logger
        from utils.async_helpers import AsyncTaskManager
        print("  ✅ Utilities imported successfully")
        
        # Test module imports
        from modules.gcs_handler import GCSHandler
        from modules.stt_processor import STTProcessor
        from modules.dlp_processor import DLPProcessor
        from modules.ccai_formatter import CCAIFormatter
        from modules.ccai_uploader import CCAIUploader
        print("  ✅ Modules imported successfully")
        
        # Test main import
        from main import STTInsightsPipeline
        print("  ✅ Main pipeline imported successfully")
        
        # Change back to original directory
        os.chdir(original_dir)
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Import failed: {e}")
        # Make sure to change back to original directory
        import os
        os.chdir(original_dir)
        return False

def test_config_loading():
    """Test configuration loading."""
    print("\n🔧 Testing configuration loading...")
    
    try:
        from utils.config_loader import ConfigLoader
        
        config_path = project_root / "config" / "config.yaml"
        config_loader = ConfigLoader(str(config_path))
        config = config_loader.load_config()
        
        # Validate required sections
        required_sections = ['gcp', 'gcs', 'stt', 'dlp', 'ccai', 'processing']
        missing_sections = [section for section in required_sections if section not in config]
        
        if missing_sections:
            print(f"  ❌ Missing configuration sections: {missing_sections}")
            return False
        
        print("  ✅ Configuration loaded and validated successfully")
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration test failed: {e}")
        return False

def test_logging_setup():
    """Test logging setup."""
    print("\n📝 Testing logging setup...")
    
    try:
        from utils.logger import setup_logging
        
        # Override logging config for testing
        test_log_config = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file_path': 'logs/test_stt_insights.log'
        }
        
        logger = setup_logging(test_log_config)
        logger.info("Test log message")
        
        print("  ✅ Logging setup successful")
        return True
        
    except Exception as e:
        print(f"  ❌ Logging test failed: {e}")
        return False

def test_dependencies():
    """Test that all required dependencies are available."""
    print("\n📦 Testing dependencies...")
    
    required_packages = [
        'google.cloud.storage',
        'google.cloud.speech',
        'google.cloud.dlp_v2',
        'google.cloud.contact_center_insights_v1',
        'yaml',
        'structlog',
        'tenacity',
        'aiofiles'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - NOT FOUND")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {missing_packages}")
        print("Please install missing dependencies with: pip install -r requirements.txt")
        return False
    
    print("\n✅ All dependencies are available")
    return True

def test_file_structure():
    """Test that all required files and directories exist."""
    print("\n📁 Testing file structure...")
    
    required_paths = [
        "src/main.py",
        "src/modules/__init__.py",
        "src/modules/gcs_handler.py",
        "src/modules/stt_processor.py",
        "src/modules/dlp_processor.py",
        "src/modules/ccai_formatter.py",
        "src/modules/ccai_uploader.py",
        "src/utils/__init__.py",
        "src/utils/config_loader.py",
        "src/utils/logger.py",
        "src/utils/async_helpers.py",
        "config/config.yaml",
        "requirements.txt",
        "README.md"
    ]
    
    missing_files = []
    
    for path in required_paths:
        file_path = project_root / path
        if file_path.exists():
            print(f"  ✅ {path}")
        else:
            print(f"  ❌ {path} - NOT FOUND")
            missing_files.append(path)
    
    if missing_files:
        print(f"\n❌ Missing files: {missing_files}")
        return False
    
    print("\n✅ All required files are present")
    return True

def main():
    """Run all tests."""
    print("🚀 STT E2E Insights Pipeline - Setup Validation")
    print("=" * 50)
    
    tests = [
        ("File Structure", test_file_structure),
        ("Dependencies", test_dependencies),
        ("Module Imports", test_imports),
        ("Configuration Loading", test_config_loading),
        ("Logging Setup", test_logging_setup),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! The pipeline is ready to use.")
        print("\nNext steps:")
        print("1. Update config/config.yaml with your GCP project settings")
        print("2. Set up Google Cloud authentication")
        print("3. Create DLP templates in your GCP project")
        print("4. Run: python src/main.py --validate-only")
        return 0
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed. Please address the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())