"""
Translation module for service call data
"""
import asyncio
import pandas as pd
from googletrans import Translator
from tqdm import tqdm


class ServiceCallTranslator:
    """Handles translation of service call data"""
    
    def __init__(self, config):
        """
        Initialize translator
        
        Args:
            config: Dictionary with translation settings
        """
        self.batch_size = config.get('batch_size', 10)
        self.batch_delay = config.get('batch_delay', 0.5)
        self.target_language = config.get('target_language', 'en')
        self.columns_to_translate = config.get('columns_to_translate', [])
    
    async def translate_text(self, text: str) -> str:
        """Translate a single text string"""
        async with Translator() as translator:
            result = await translator.translate(text, dest=self.target_language)
            return result.text
    
    async def translate_unique_values(self, unique_values, column_name):
        """
        Translate unique values with batching and progress tracking
        
        Args:
            unique_values: Array of unique values to translate
            column_name: Name of column being translated (for progress display)
        
        Returns:
            Dictionary mapping original text to translated text
        """
        translation_map = {}
        total = len(unique_values)
        
        print(f"\n🔄 Translating {total:,} unique values for {column_name}...")
        
        with tqdm(total=total, desc=f"  {column_name}", ncols=100) as pbar:
            for i in range(0, total, self.batch_size):
                batch = unique_values[i:i + self.batch_size]
                
                # Prepare translation tasks
                tasks = []
                valid_texts = []
                
                for text in batch:
                    if pd.isna(text) or str(text).strip() == '':
                        translation_map[text] = ''
                    else:
                        tasks.append(self.translate_text(str(text)))
                        valid_texts.append(text)
                
                # Execute batch concurrently
                if tasks:
                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Map results back to original texts
                        for idx, text in enumerate(valid_texts):
                            result = results[idx]
                            if isinstance(result, Exception):
                                print(f"\n  ⚠ Error: {str(text)[:50]}... - {result}")
                                translation_map[text] = str(text)  # Keep original on error
                            else:
                                translation_map[text] = result
                    
                    except Exception as e:
                        print(f"\n  ⚠ Batch error: {e}")
                        # Fallback: translate one by one
                        for text in valid_texts:
                            try:
                                translation_map[text] = await self.translate_text(str(text))
                            except Exception as e2:
                                print(f"  ⚠ Error: {str(text)[:30]}... - {e2}")
                                translation_map[text] = str(text)
                
                pbar.update(len(batch))
                
                # Delay between batches
                if i + self.batch_size < total:
                    await asyncio.sleep(self.batch_delay)
        
        return translation_map
    
    async def translate_dataframe_async(self, df, output_path=None):
        """
        Translate multiple columns in DataFrame
        
        Args:
            df: DataFrame to translate
            output_path: Optional path to save progress
        
        Returns:
            DataFrame with translated columns
        """
        df_result = df.copy()
        
        # Filter to only columns that exist
        columns_to_process = [
            col for col in self.columns_to_translate 
            if col in df.columns
        ]
        
        if not columns_to_process:
            print("\n⚠ No matching columns found for translation!")
            return df_result
        
        print(f"\n{'='*80}")
        print(f"TRANSLATION - Processing {len(columns_to_process)} columns")
        print(f"{'='*80}")
        
        for col in columns_to_process:
            print(f"\n📝 Column: {col}")
            
            # Get unique non-null values
            unique_values = df[col].dropna().unique()
            total_unique = len(unique_values)
            total_cells = df[col].notna().sum()
            
            if total_unique == 0:
                print(f"  ℹ No data to translate")
                continue
            
            print(f"  Unique values: {total_unique:,}")
            print(f"  Total cells: {total_cells:,}")
            print(f"  Efficiency: {total_cells / total_unique:.1f}x reduction")
            
            # Translate unique values
            translation_map = await self.translate_unique_values(unique_values, col)
            
            # Apply translations to dataframe
            new_col_name = f'{col}_EN'
            df_result[new_col_name] = df[col].map(translation_map)
            
            # Fill NaN values with original
            df_result[new_col_name].fillna(df[col], inplace=True)
            
            print(f"  ✓ Completed! Created column: {new_col_name}")
            
            # Save progress if path provided
            if output_path:
                try:
                    progress_file = output_path.replace('.xlsx', '_progress.xlsx')
                    df_result.to_excel(progress_file, index=False)
                    print(f"  💾 Progress saved")
                except Exception as e:
                    print(f"  ⚠ Could not save progress: {e}")
        
        return df_result
    
    def translate(self, df, output_path=None):
        """
        Synchronous wrapper for translation
        
        Args:
            df: DataFrame to translate
            output_path: Optional path to save progress
        
        Returns:
            Translated DataFrame
        """
        return asyncio.run(self.translate_dataframe_async(df, output_path))


def show_translation_sample(df, original_cols, translated_cols):
    """
    Show sample translations for verification
    
    Args:
        df: DataFrame with translations
        original_cols: List of original column names
        translated_cols: List of translated column names
    """
    print(f"\n{'='*80}")
    print("SAMPLE TRANSLATIONS")
    print(f"{'='*80}")
    
    for orig, trans in zip(original_cols, translated_cols):
        if orig not in df.columns or trans not in df.columns:
            continue
        
        # Find first valid value
        sample_idx = df[orig].first_valid_index()
        if sample_idx is not None:
            original = str(df.loc[sample_idx, orig])
            translated = str(df.loc[sample_idx, trans])
            
            print(f"\n{orig}:")
            print(f"  Original:   {original[:100]}{'...' if len(original) > 100 else ''}")
            print(f"  Translated: {translated[:100]}{'...' if len(translated) > 100 else ''}")
    
    print(f"\n{'='*80}")
