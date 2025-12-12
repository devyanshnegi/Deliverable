"""
Setup Verification Script

Run this script to verify that your environment is correctly configured.
"""
import sys
import os
import json


def check_python_version():
    """Check Python version"""
    print("\n📋 Checking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"  ✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"  ✗ Python {version.major}.{version.minor} (Need Python 3.8+)")
        return False


def check_dependencies():
    """Check if all required packages are installed"""
    print("\n📦 Checking required packages...")
    
    required_packages = [
        'pandas',
        'googletrans',
        'tqdm',
        'msal',
        'requests',
        'pypac',
        'openpyxl'
    ]
    
    all_installed = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} - NOT INSTALLED")
            all_installed = False
    
    return all_installed


def check_directory_structure():
    """Check if all required directories and files exist"""
    print("\n📁 Checking directory structure...")
    
    required_items = [
        ('config', 'dir'),
        ('config/settings.json', 'file'),
        ('config/part_failure_data.json', 'file'),
        ('config/prompts.json', 'file'),
        ('src', 'dir'),
        ('src/utils.py', 'file'),
        ('src/translator.py', 'file'),
        ('src/classifier.py', 'file'),
        ('src/pipeline.py', 'file'),
        ('main.py', 'file'),
        ('README.md', 'file')
    ]
    
    all_exist = True
    for item, item_type in required_items:
        if item_type == 'dir':
            exists = os.path.isdir(item)
            symbol = "📁" if exists else "✗"
        else:
            exists = os.path.isfile(item)
            symbol = "📄" if exists else "✗"
        
        status = "✓" if exists else "✗"
        print(f"  {status} {symbol} {item}")
        
        if not exists:
            all_exist = False
    
    return all_exist


def check_config_files():
    """Validate configuration files"""
    print("\n⚙️  Validating configuration files...")
    
    config_files = [
        'config/settings.json',
        'config/part_failure_data.json',
        'config/prompts.json'
    ]
    
    all_valid = True
    for config_file in config_files:
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                json.load(f)
            print(f"  ✓ {config_file} - Valid JSON")
        except FileNotFoundError:
            print(f"  ✗ {config_file} - NOT FOUND")
            all_valid = False
        except json.JSONDecodeError as e:
            print(f"  ✗ {config_file} - INVALID JSON: {e}")
            all_valid = False
    
    return all_valid


def check_settings_content():
    """Check if settings.json has required keys"""
    print("\n🔍 Checking settings configuration...")
    
    try:
        with open('config/settings.json', 'r') as f:
            settings = json.load(f)
        
        required_sections = ['translation', 'classification', 'api', 'output']
        all_present = True
        
        for section in required_sections:
            if section in settings:
                print(f"  ✓ {section} section")
            else:
                print(f"  ✗ {section} section - MISSING")
                all_present = False
        
        return all_present
    
    except Exception as e:
        print(f"  ✗ Error reading settings: {e}")
        return False


def print_summary(results):
    """Print summary of checks"""
    print("\n" + "="*80)
    print("SETUP VERIFICATION SUMMARY")
    print("="*80)
    
    all_passed = all(results.values())
    
    for check_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {check_name}")
    
    print("="*80)
    
    if all_passed:
        print("\n✅ All checks passed! You're ready to run the pipeline.")
        print("\nTo start processing:")
        print("  python main.py")
    else:
        print("\n⚠️  Some checks failed. Please fix the issues above.")
        print("\nCommon fixes:")
        print("  - Install missing packages: pip install -r requirements.txt")
        print("  - Check that all files are in the correct locations")
        print("  - Verify JSON files are properly formatted")
    
    print()


def main():
    """Run all verification checks"""
    print("="*80)
    print("SERVICE CALL PIPELINE - SETUP VERIFICATION")
    print("="*80)
    
    results = {
        "Python Version": check_python_version(),
        "Dependencies": check_dependencies(),
        "Directory Structure": check_directory_structure(),
        "Configuration Files": check_config_files(),
        "Settings Content": check_settings_content()
    }
    
    print_summary(results)


if __name__ == "__main__":
    main()
