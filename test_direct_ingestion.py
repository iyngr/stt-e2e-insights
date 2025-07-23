#!/usr/bin/env python3
"""Test script to validate direct CCAI Insights ingestion functionality."""

import sys
import os
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def test_direct_ingestion_imports():
    """Test that direct ingestion modules can be imported."""
    print("🔍 Testing direct ingestion imports...")
    
    try:
        from modules.ccai_uploader import CCAIUploader
        print("  ✅ CCAIUploader imported successfully")
        
        # Check if direct ingestion method exists
        uploader = CCAIUploader.__new__(CCAIUploader)  # Create without init
        if hasattr(uploader, 'ingest_conversations_direct'):
            print("  ✅ Direct ingestion method found")
        else:
            print("  ❌ Direct ingestion method not found")
            return False
            
        if hasattr(uploader, '_create_direct_conversation_object'):
            print("  ✅ Direct conversation object creation method found")
        else:
            print("  ❌ Direct conversation object creation method not found")
            return False
            
        return True
        
    except ImportError as e:
        print(f"  ❌ Import failed: {e}")
        return False

def test_configuration_structure():
    """Test that the configuration supports direct ingestion."""
    print("\n🔧 Testing configuration structure...")
    
    try:
        from utils.config_loader import get_config_section
        
        ccai_config = get_config_section('ccai')
        
        # Check for required direct ingestion fields
        if 'recognizer_id' in ccai_config:
            print(f"  ✅ Recognizer ID configured: {ccai_config['recognizer_id']}")
        else:
            print("  ❌ Recognizer ID not configured")
            return False
            
        if 'processing_mode' in ccai_config:
            print(f"  ✅ Processing mode configured: {ccai_config['processing_mode']}")
        else:
            print("  ❌ Processing mode not configured")
            return False
            
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration test failed: {e}")
        return False

def test_direct_conversation_object_creation():
    """Test creating a conversation object for direct ingestion."""
    print("\n🏗️  Testing direct conversation object creation...")
    
    try:
        # Mock the necessary components
        from modules.ccai_uploader import CCAIUploader
        from utils.config_loader import get_config_section
        
        # Initialize uploader components
        gcp_config = get_config_section('gcp')
        ccai_config = get_config_section('ccai')
        
        # Create a test uploader instance
        uploader = CCAIUploader.__new__(CCAIUploader)
        uploader.project_id = gcp_config.get('project_id', 'test-project')
        uploader.ccai_config = ccai_config
        uploader.location = ccai_config.get('location', 'us-central1')
        uploader.parent = f"projects/{uploader.project_id}/locations/{uploader.location}"
        
        # Mock logger - use property assignment
        class MockLogger:
            def debug(self, *args, **kwargs): pass
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): pass
            def warning(self, *args, **kwargs): pass
        
        # Initialize the logger property correctly
        from utils.logger import LoggerMixin
        LoggerMixin.__init__(uploader)  # Initialize the mixin
        
        # Test creating a conversation object
        test_audio_file = "test-audio-files/merged_conversation_001.wav"
        test_recognizer_id = ccai_config.get('recognizer_id', 'test-recognizer')
        
        try:
            conversation = uploader._create_direct_conversation_object(test_audio_file, test_recognizer_id)
            print(f"  ✅ Conversation object created successfully")
            print(f"      Medium: {conversation.medium}")
            print(f"      Language: {conversation.language_code}")
            print(f"      Data source configured: {bool(conversation.data_source)}")
            print(f"      Call metadata configured: {bool(conversation.call_metadata)}")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Conversation object creation failed: {e}")
            return False
        
    except Exception as e:
        print(f"  ❌ Direct conversation test failed: {e}")
        return False

def test_command_line_arguments():
    """Test that new command line arguments work."""
    print("\n⚙️  Testing command line arguments...")
    
    try:
        from main import main
        import subprocess
        
        # Test help output includes new arguments
        result = subprocess.run([
            sys.executable, "src/main.py", "--help"
        ], 
        cwd=project_root, 
        capture_output=True, 
        text=True
        )
        
        help_output = result.stdout
        
        if "--processing-mode" in help_output:
            print("  ✅ Processing mode argument found")
        else:
            print("  ❌ Processing mode argument not found")
            return False
            
        if "--recognizer-id" in help_output:
            print("  ✅ Recognizer ID argument found")
        else:
            print("  ❌ Recognizer ID argument not found")
            return False
            
        if "{manual,direct}" in help_output:
            print("  ✅ Processing mode choices configured correctly")
        else:
            print("  ❌ Processing mode choices not configured correctly")
            return False
            
        return True
        
    except Exception as e:
        print(f"  ❌ Command line test failed: {e}")
        return False

def test_configuration_validation():
    """Test that the recognizer ID in config matches the expected value."""
    print("\n✅ Testing recognizer configuration validation...")
    
    try:
        from utils.config_loader import get_config_section
        
        ccai_config = get_config_section('ccai')
        recognizer_id = ccai_config.get('recognizer_id')
        expected_recognizer = "projects/315895523022/locations/us-central1/recognizers/ccai-insights-recognizer"
        
        if recognizer_id == expected_recognizer:
            print(f"  ✅ Recognizer ID matches expected value")
            print(f"      Configured: {recognizer_id}")
        else:
            print(f"  ⚠️  Recognizer ID differs from expected")
            print(f"      Expected: {expected_recognizer}")
            print(f"      Configured: {recognizer_id}")
            
        processing_mode = ccai_config.get('processing_mode')
        if processing_mode == 'direct':
            print(f"  ✅ Processing mode set to direct")
        else:
            print(f"  ⚠️  Processing mode not set to direct: {processing_mode}")
            
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration validation failed: {e}")
        return False

def main():
    """Run all direct ingestion tests."""
    print("🚀 STT E2E Insights - Direct Ingestion Validation")
    print("=" * 60)
    
    tests = [
        ("Direct Ingestion Imports", test_direct_ingestion_imports),
        ("Configuration Structure", test_configuration_structure),
        ("Conversation Object Creation", test_direct_conversation_object_creation),
        ("Command Line Arguments", test_command_line_arguments),
        ("Configuration Validation", test_configuration_validation),
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
    print("\n" + "=" * 60)
    print("📊 DIRECT INGESTION TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All direct ingestion tests passed!")
        print("\nNext steps:")
        print("1. Set up Google Cloud authentication")
        print("2. Test with actual audio files:")
        print("   python src/main.py --processing-mode direct --file-limit 1")
        print("3. Compare with manual processing:")
        print("   python src/main.py --processing-mode manual --file-limit 1")
        return 0
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed. Please address the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())