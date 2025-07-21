#!/usr/bin/env python3
"""Simple integration test to validate the STT E2E Insights pipeline functionality."""

import sys
import os
import subprocess
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / "src"

def test_python_package_structure():
    """Test if the project can be run as a Python package."""
    print("🔧 Testing Python package execution...")
    
    try:
        # Test running as a module with help
        result = subprocess.run([
            sys.executable, "-m", "src.main", "--help"
        ], 
        cwd=project_root, 
        capture_output=True, 
        text=True, 
        timeout=30
        )
        
        if result.returncode == 0:
            print("  ✅ Package structure works correctly")
            return True
        else:
            print(f"  ❌ Package execution failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("  ❌ Package execution timed out")
        return False
    except Exception as e:
        print(f"  ❌ Package execution error: {e}")
        return False

def test_setup_py_install():
    """Test if the package can be installed via setup.py."""
    print("📦 Testing setup.py installation...")
    
    try:
        # Test setup.py develop install
        result = subprocess.run([
            sys.executable, "setup.py", "develop", "--user"
        ], 
        cwd=project_root, 
        capture_output=True, 
        text=True, 
        timeout=60
        )
        
        if result.returncode == 0:
            print("  ✅ Package installed successfully")
            
            # Test if we can import the package now
            try:
                import src.main
                print("  ✅ Package imports successfully after installation")
                return True
            except ImportError as e:
                print(f"  ❌ Package import failed after installation: {e}")
                return False
        else:
            print(f"  ❌ Package installation failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("  ❌ Package installation timed out")
        return False
    except Exception as e:
        print(f"  ❌ Package installation error: {e}")
        return False

def test_config_validation():
    """Test configuration validation."""
    print("⚙️ Testing configuration structure...")
    
    config_file = project_root / "config" / "config.yaml"
    
    try:
        import yaml
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        required_sections = ['gcp', 'gcs', 'stt', 'dlp', 'ccai', 'processing', 'logging']
        
        missing_sections = []
        for section in required_sections:
            if section not in config:
                missing_sections.append(section)
        
        if missing_sections:
            print(f"  ❌ Missing configuration sections: {missing_sections}")
            return False
        
        print("  ✅ All required configuration sections present")
        
        # Test specific required fields
        gcp_fields = ['project_id']
        gcs_fields = ['input_bucket', 'output_bucket']
        stt_fields = ['language_code', 'sample_rate_hertz', 'encoding']
        dlp_fields = ['location']
        ccai_fields = ['location']
        
        field_checks = [
            ('gcp', gcp_fields),
            ('gcs', gcs_fields),
            ('stt', stt_fields),
            ('dlp', dlp_fields),
            ('ccai', ccai_fields)
        ]
        
        for section_name, fields in field_checks:
            section = config[section_name]
            missing_fields = [field for field in fields if field not in section]
            if missing_fields:
                print(f"  ❌ Missing fields in {section_name}: {missing_fields}")
                return False
        
        print("  ✅ All required configuration fields present")
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration validation failed: {e}")
        return False

def test_dependencies_comprehensive():
    """Comprehensive dependency test."""
    print("📚 Testing comprehensive dependencies...")
    
    # Core Google Cloud libraries
    google_cloud_deps = [
        'google.cloud.storage',
        'google.cloud.speech',
        'google.cloud.dlp_v2', 
        'google.cloud.contact_center_insights_v1'
    ]
    
    # Utility libraries
    utility_deps = [
        'yaml',
        'structlog',
        'tenacity',
        'aiofiles',
        'asyncio'
    ]
    
    # Google auth and core libraries
    google_core_deps = [
        'google.auth',
        'google.api_core',
        'google.protobuf'
    ]
    
    all_deps = google_cloud_deps + utility_deps + google_core_deps
    missing_deps = []
    
    for dep in all_deps:
        try:
            __import__(dep)
            print(f"  ✅ {dep}")
        except ImportError:
            print(f"  ❌ {dep} - NOT FOUND")
            missing_deps.append(dep)
    
    if missing_deps:
        print(f"\n❌ Missing dependencies: {missing_deps}")
        return False
    
    print("\n✅ All dependencies are available")
    return True

def test_project_structure():
    """Test comprehensive project structure."""
    print("📂 Testing comprehensive project structure...")
    
    required_structure = {
        'src/': True,
        'src/main.py': True,
        'src/__init__.py': True,
        'src/modules/': True,
        'src/modules/__init__.py': True,
        'src/modules/gcs_handler.py': True,
        'src/modules/stt_processor.py': True,
        'src/modules/dlp_processor.py': True,
        'src/modules/ccai_formatter.py': True,
        'src/modules/ccai_uploader.py': True,
        'src/utils/': True,
        'src/utils/__init__.py': True,
        'src/utils/config_loader.py': True,
        'src/utils/logger.py': True,
        'src/utils/async_helpers.py': True,
        'config/': True,
        'config/config.yaml': True,
        'requirements.txt': True,
        'setup.py': True,
        'README.md': True,
        'logs/': True,
        '.gitignore': True
    }
    
    missing_items = []
    
    for item_path, is_required in required_structure.items():
        full_path = project_root / item_path
        exists = full_path.exists()
        
        if exists:
            print(f"  ✅ {item_path}")
        else:
            if is_required:
                print(f"  ❌ {item_path} - MISSING (required)")
                missing_items.append(item_path)
            else:
                print(f"  ⚠️  {item_path} - MISSING (optional)")
    
    if missing_items:
        print(f"\n❌ Missing required items: {missing_items}")
        return False
    
    print("\n✅ Project structure is complete")
    return True

def main():
    """Run comprehensive validation tests."""
    print("🚀 STT E2E Insights Pipeline - Comprehensive Validation")
    print("=" * 60)
    
    tests = [
        ("Project Structure", test_project_structure),
        ("Dependencies", test_dependencies_comprehensive),
        ("Configuration", test_config_validation),
        ("Setup.py Installation", test_setup_py_install),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 40)
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 COMPREHENSIVE TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! The pipeline implementation is complete.")
        print("\n📋 Next steps for deployment:")
        print("1. Update config/config.yaml with your actual GCP project settings")
        print("2. Set up Google Cloud authentication:")
        print("   - gcloud auth application-default login")
        print("   - OR set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("3. Create DLP templates in your GCP project")
        print("4. Create GCS buckets for input and output")
        print("5. Enable required APIs in your GCP project:")
        print("   - Cloud Storage API")
        print("   - Speech-to-Text API")
        print("   - Data Loss Prevention API")
        print("   - Contact Center AI Insights API")
        print("6. Test with: python -m src.main --validate-only")
        return 0
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed. Please address the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())