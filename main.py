"""
Simple command-line interface for Service Call Processing Pipeline

This script provides an easy-to-use interface for non-technical users.
"""
import os
import sys
from src.pipeline import ServiceCallPipeline


def print_banner():
    """Print welcome banner"""
    print("\n" + "="*80)
    print(" " * 20 + "SERVICE CALL PROCESSING PIPELINE")
    print(" " * 30 + "Version 1.0")
    print("="*80)


def get_user_choice():
    """Get processing mode from user"""
    print("\nPlease select a processing mode:")
    print("\n  1. Full Processing")
    print("     Process a new file completely (translate + classify)")
    print("\n  2. Incremental Processing")
    print("     Only process new service orders not in existing file")
    print("\n  3. Exit")
    
    while True:
        choice = input("\nEnter your choice (1, 2, or 3): ").strip()
        if choice in ['1', '2', '3']:
            return choice
        print("  ⚠ Invalid choice. Please enter 1, 2, or 3.")


def get_file_path(prompt):
    """Get and validate file path from user"""
    while True:
        filepath = input(f"\n{prompt}: ").strip()
        
        # Remove quotes if user added them
        filepath = filepath.strip('"').strip("'")
        
        if os.path.exists(filepath):
            return filepath
        
        print(f"  ✗ File not found: {filepath}")
        print(f"  Please check the path and try again.")
        retry = input("  Try again? (y/n): ").strip().lower()
        if retry != 'y':
            return None


def get_output_name():
    """Get output base name from user"""
    default_name = "processed_service_calls"
    name = input(f"\nOutput file base name (press Enter for '{default_name}'): ").strip()
    
    if not name:
        return default_name
    
    # Remove any file extensions user might have added
    name = name.replace('.xlsx', '').replace('.xls', '')
    return name


def run_full_processing(pipeline):
    """Run full processing mode"""
    print("\n" + "="*80)
    print("FULL PROCESSING MODE")
    print("="*80)
    print("\nThis mode will:")
    print("  1. Translate service call data to English")
    print("  2. Classify all service calls (parts, failures, fixes)")
    print("  3. Generate output files with results")
    
    # Get input file
    input_file = get_file_path("Enter path to input Excel file")
    if not input_file:
        print("\n⚠ Processing cancelled.")
        return
    
    # Get output name
    output_name = get_output_name()
    
    # Confirm
    print("\n" + "-"*80)
    print("Ready to process:")
    print(f"  Input file: {input_file}")
    print(f"  Output base name: {output_name}")
    print("-"*80)
    
    confirm = input("\nProceed? (y/n): ").strip().lower()
    if confirm != 'y':
        print("\n⚠ Processing cancelled.")
        return
    
    # Run pipeline
    try:
        main_df, problems_df = pipeline.run_full_pipeline(input_file, output_name)
        
        if main_df is not None:
            print("\n✓ Processing completed successfully!")
        else:
            print("\n✗ Processing failed. Please check error messages above.")
    
    except Exception as e:
        print(f"\n✗ Error during processing: {e}")
        import traceback
        traceback.print_exc()


def run_incremental_processing(pipeline):
    """Run incremental processing mode"""
    print("\n" + "="*80)
    print("INCREMENTAL PROCESSING MODE")
    print("="*80)
    print("\nThis mode will:")
    print("  1. Compare two files by SERVICE_ORDER")
    print("  2. Find new service orders in the second file")
    print("  3. Only process new orders (translate + classify)")
    print("  4. Merge results with existing processed data")
    
    # Get files
    print("\n📁 File 1: Already processed file (e.g., data up to November)")
    processed_file = get_file_path("Enter path to processed file")
    if not processed_file:
        print("\n⚠ Processing cancelled.")
        return
    
    print("\n📁 File 2: New file with additional data (e.g., data up to December)")
    new_file = get_file_path("Enter path to new file")
    if not new_file:
        print("\n⚠ Processing cancelled.")
        return
    
    # Get output name
    output_name = get_output_name()
    
    # Confirm
    print("\n" + "-"*80)
    print("Ready to process:")
    print(f"  Existing processed file: {processed_file}")
    print(f"  New data file: {new_file}")
    print(f"  Output base name: {output_name}")
    print("-"*80)
    
    confirm = input("\nProceed? (y/n): ").strip().lower()
    if confirm != 'y':
        print("\n⚠ Processing cancelled.")
        return
    
    # Run pipeline
    try:
        combined_df, combined_problems_df = pipeline.run_incremental_pipeline(
            new_file, processed_file, output_name
        )
        
        if combined_df is not None:
            print("\n✓ Incremental processing completed successfully!")
        else:
            print("\n✗ Processing failed. Please check error messages above.")
    
    except Exception as e:
        print(f"\n✗ Error during processing: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point"""
    print_banner()
    
    try:
        # Initialize pipeline
        pipeline = ServiceCallPipeline(config_dir='config')
        
        # Setup authentication
        pipeline.setup_authentication()
        
        # Initialize components
        pipeline.initialize_components()
        
        # Main loop
        while True:
            choice = get_user_choice()
            
            if choice == '1':
                run_full_processing(pipeline)
            
            elif choice == '2':
                run_incremental_processing(pipeline)
            
            elif choice == '3':
                print("\n👋 Goodbye!")
                sys.exit(0)
            
            # Ask if user wants to continue
            print("\n" + "="*80)
            continue_choice = input("\nProcess another file? (y/n): ").strip().lower()
            if continue_choice != 'y':
                print("\n👋 Goodbye!")
                break
    
    except KeyboardInterrupt:
        print("\n\n⚠ Process interrupted by user.")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
