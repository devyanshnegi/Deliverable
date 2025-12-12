"""
Main pipeline orchestrator for service call processing
"""
import time
from datetime import datetime
from src.utils import (
    load_json, load_excel, save_excel, 
    find_new_service_orders, merge_processed_data,
    validate_dataframe, print_summary
)
from src.translator import ServiceCallTranslator, show_translation_sample
from src.classifier import ServiceCallClassifier


class ServiceCallPipeline:
    """Main pipeline for translating and classifying service calls"""
    
    def __init__(self, config_dir='config'):
        """
        Initialize pipeline
        
        Args:
            config_dir: Directory containing configuration files
        """
        print("\n" + "="*80)
        print("SERVICE CALL PROCESSING PIPELINE")
        print("="*80)
        
        # Load configurations
        print("\n📂 Loading configurations...")
        self.settings = load_json(f'{config_dir}/settings.json')
        self.part_failure_data = load_json(f'{config_dir}/part_failure_data.json')
        self.prompts = load_json(f'{config_dir}/prompts.json')
        
        print("  ✓ Settings loaded")
        print("  ✓ Part failure data loaded")
        print("  ✓ Prompts loaded")
        
        self.translator = None
        self.classifier = None
        self.token = None
    
    def setup_authentication(self):
        """Setup MSAL authentication and get token"""
        import msal
        
        print("\n🔐 Setting up authentication...")
        api_config = self.settings['api']
        
        _msal_auth = msal.PublicClientApplication(
            client_id=api_config['client_id'],
            authority=api_config['authority'],
        )
        
        token_meta = _msal_auth.acquire_token_interactive(scopes=[api_config['scope']])
        self.token = token_meta["access_token"]
        
        print("  ✓ Authentication successful")
    
    def initialize_components(self):
        """Initialize translator and classifier"""
        print("\n⚙️  Initializing components...")
        
        # Initialize translator
        self.translator = ServiceCallTranslator(self.settings['translation'])
        print("  ✓ Translator initialized")
        
        # Initialize classifier
        self.classifier = ServiceCallClassifier(
            self.part_failure_data,
            self.prompts,
            self.settings['api'],
            self.token
        )
        print("  ✓ Classifier initialized")
    
    def run_full_pipeline(self, input_file, output_base_name="processed_service_calls"):
        """
        Run complete pipeline: Translation + Classification
        
        Args:
            input_file: Path to input Excel file
            output_base_name: Base name for output files
        
        Returns:
            Tuple (main_df, problems_df)
        """
        start_time = time.time()
        
        # Load data
        print(f"\n📂 Loading input file: {input_file}")
        df = load_excel(input_file)
        
        if df is None:
            print("✗ Failed to load input file!")
            return None, None
        
        print_summary(df, "Input Data")
        
        # Validate required columns
        required_cols = ['SERVICE_ORDER'] + self.settings['translation']['columns_to_translate']
        is_valid, missing = validate_dataframe(df, required_cols)
        
        if not is_valid:
            print(f"\n⚠ Missing required columns. Cannot proceed.")
            return None, None
        
        # Step 1: Translation
        print(f"\n{'='*80}")
        print("STEP 1: TRANSLATION")
        print(f"{'='*80}")
        
        df_translated = self.translator.translate(df)
        
        # Show samples
        original_cols = [c for c in self.settings['translation']['columns_to_translate'] if c in df.columns]
        translated_cols = [f"{c}_EN" for c in original_cols]
        show_translation_sample(df_translated, original_cols, translated_cols)
        
        # Save translated file
        translated_file = save_excel(
            df_translated, 
            f"{output_base_name}_translated.xlsx",
            create_backup=self.settings['output']['create_backup']
        )
        
        # Step 2: Classification
        print(f"\n{'='*80}")
        print("STEP 2: CLASSIFICATION")
        print(f"{'='*80}")
        
        df_classified, problems_df = self.classifier.process_dataframe(
            df_translated,
            max_workers=self.settings['classification']['max_workers']
        )
        
        # Save results
        print(f"\n{'='*80}")
        print("SAVING RESULTS")
        print(f"{'='*80}")
        
        main_file = save_excel(
            df_classified,
            f"{output_base_name}_final.xlsx",
            create_backup=self.settings['output']['create_backup']
        )
        
        problems_file = save_excel(
            problems_df,
            f"{output_base_name}_problems_normalized.xlsx",
            create_backup=self.settings['output']['create_backup']
        )
        
        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*80}")
        print("PIPELINE COMPLETE")
        print(f"{'='*80}")
        print(f"Time taken: {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")
        print(f"\nOutput files:")
        print(f"  📄 Translated: {translated_file}")
        print(f"  📄 Main results: {main_file}")
        print(f"  📄 Normalized problems: {problems_file}")
        print(f"{'='*80}")
        
        return df_classified, problems_df
    
    def run_incremental_pipeline(self, new_file, processed_file, 
                                output_base_name="processed_service_calls_incremental"):
        """
        Run incremental pipeline: Only process new SERVICE_ORDERs
        
        Args:
            new_file: Path to file with all service calls (including new ones)
            processed_file: Path to already processed file
            output_base_name: Base name for output files
        
        Returns:
            Tuple (combined_main_df, combined_problems_df)
        """
        start_time = time.time()
        
        print(f"\n{'='*80}")
        print("INCREMENTAL PROCESSING MODE")
        print(f"{'='*80}")
        
        # Load files
        print(f"\n📂 Loading files...")
        new_df = load_excel(new_file)
        processed_df = load_excel(processed_file)
        
        if new_df is None:
            print("✗ Failed to load new file!")
            return None, None
        
        # Find new SERVICE_ORDERs
        records_to_process = find_new_service_orders(new_df, processed_df)
        
        if records_to_process.empty:
            print("\n✓ No new records to process!")
            return processed_df, None
        
        print_summary(records_to_process, "Records to Process")
        
        # Run translation on new records only
        print(f"\n{'='*80}")
        print("STEP 1: TRANSLATING NEW RECORDS")
        print(f"{'='*80}")
        
        new_translated = self.translator.translate(records_to_process)
        
        # Run classification on new records
        print(f"\n{'='*80}")
        print("STEP 2: CLASSIFYING NEW RECORDS")
        print(f"{'='*80}")
        
        new_classified, new_problems_df = self.classifier.process_dataframe(
            new_translated,
            max_workers=self.settings['classification']['max_workers']
        )
        
        # Merge with existing processed data
        print(f"\n{'='*80}")
        print("STEP 3: MERGING WITH EXISTING DATA")
        print(f"{'='*80}")
        
        combined_df = merge_processed_data(processed_df, new_classified)
        
        # Load existing problems if available
        problems_file_path = processed_file.replace('_final.xlsx', '_problems_normalized.xlsx')
        try:
            existing_problems_df = load_excel(problems_file_path)
            combined_problems_df = merge_processed_data(existing_problems_df, new_problems_df)
        except:
            print("  ℹ No existing problems file found, using new problems only")
            combined_problems_df = new_problems_df
        
        # Save results
        print(f"\n{'='*80}")
        print("SAVING RESULTS")
        print(f"{'='*80}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        main_file = save_excel(
            combined_df,
            f"{output_base_name}_{timestamp}.xlsx",
            create_backup=False  # Don't backup incremental files
        )
        
        problems_file = save_excel(
            combined_problems_df,
            f"{output_base_name}_problems_{timestamp}.xlsx",
            create_backup=False
        )
        
        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*80}")
        print("INCREMENTAL PIPELINE COMPLETE")
        print(f"{'='*80}")
        print(f"New records processed: {len(new_classified):,}")
        print(f"Total records: {len(combined_df):,}")
        print(f"Time taken: {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")
        print(f"\nOutput files:")
        print(f"  📄 Combined results: {main_file}")
        print(f"  📄 Combined problems: {problems_file}")
        print(f"{'='*80}")
        
        return combined_df, combined_problems_df
