"""
Classification module for service call data
"""
import json
import requests
import threading
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pypac import PACSession, get_pac


class ServiceCallClassifier:
    """Handles classification of service call data"""
    
    def __init__(self, part_failure_data, prompts, api_config, token):
        """
        Initialize classifier
        
        Args:
            part_failure_data: Dictionary of parts, failure modes, and fixes
            prompts: Dictionary with prompt templates
            api_config: API configuration dictionary
            token: Authentication token
        """
        self.part_failure_data = part_failure_data
        self.prompts = prompts
        self.api_config = api_config
        self.token = token
        self.lock = threading.Lock()
        
        # Setup session with proxy if needed
        pac_url = api_config.get('pac_url')
        if pac_url:
            try:
                pac = get_pac(url=pac_url)
                self.session = PACSession(pac=pac)
                print(f"✓ Configured PAC proxy")
            except Exception as e:
                print(f"⚠ PAC proxy failed: {e}. Using regular session.")
                self.session = requests.Session()
        else:
            self.session = requests.Session()
    
    def generate_classification_prompt(self):
        """Generate the classification prompt from current part_failure_data"""
        # Build rules from part_failure_data
        rules = ""
        for part, data in self.part_failure_data.items():
            failure_modes_str = "', '".join(data["failure_modes"])
            fixes_str = "', '".join(data["fixes"])
            rules += f"If Part = '{part}' → Failure Mode ∈ {{'{failure_modes_str}'}}; Fix ∈ {{'{fixes_str}'}}\n"
        
        # Get template and insert rules
        template = self.prompts.get('classification_prompt_template', '')
        return template.format(rules=rules)
    
    def classify_service_call(self, reason_for_service, special_notes, 
                            service_performed, issue_reported):
        """
        Classify a single service call using the API
        
        Args:
            reason_for_service: Service reason text
            special_notes: Special notes text
            service_performed: Service performed text
            issue_reported: Issue reported text
        
        Returns:
            Dictionary with classification results
        """
        url = self.api_config['endpoint']
        system_prompt = self.generate_classification_prompt()
        
        # Format user message
        user_template = self.prompts.get('classification_user_message_template', '')
        user_message = user_template.format(
            reason_for_service=reason_for_service,
            special_notes=special_notes,
            service_performed=service_performed,
            issue_reported=issue_reported
        )
        
        payload = {
            "model": self.api_config.get('model', 'gpt-4o'),
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            "temperature": self.api_config.get('temperature', 0.1)
        }
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        response = self.session.post(url, json=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            # Clean JSON markers if present
            if content.startswith('```json'):
                content = content.replace('```json\n', '').replace('\n```', '').replace('```', '')
            
            return json.loads(content)
        else:
            raise RuntimeError(f"API Error: {response.status_code}, {response.text}")
    
    def process_single_call(self, row_index, row_data, max_attempts=3):
        """
        Process a single service call with retry logic
        
        Args:
            row_index: Index of the row
            row_data: Dictionary with service call data
            max_attempts: Maximum retry attempts
        
        Returns:
            Tuple (row_index, result_dict)
        """
        attempts = 0
        while attempts < max_attempts:
            try:
                result = self.classify_service_call(
                    row_data['REASON_FOR_SERVICE'],
                    row_data['SPECIAL_NOTES'],
                    row_data['SERVICE_PERFORMED'],
                    row_data['ISSUE_REPORTED']
                )
                return row_index, result
            
            except Exception as e:
                attempts += 1
                if attempts == max_attempts:
                    print(f"\n⚠ Row {row_index} failed after {max_attempts} attempts: {e}")
                    # Return empty result
                    return row_index, {
                        "analysis": {"total_problems_found": 0, "confidence_level": "low"},
                        "problems": []
                    }
        
        return row_index, None
    
    def process_dataframe(self, df, max_workers=10):
        """
        Process entire DataFrame with multi-threading
        
        Args:
            df: DataFrame to process
            max_workers: Number of concurrent threads
        
        Returns:
            DataFrame with classification results
        """
        print(f"\n{'='*80}")
        print(f"CLASSIFICATION - Processing {len(df):,} service calls")
        print(f"{'='*80}")
        print(f"Workers: {max_workers}")
        
        # Prepare data
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {}
            for idx in df.index:
                row_data = {
                    'REASON_FOR_SERVICE': df.loc[idx, 'REASON_FOR_SERVICE'],
                    'SPECIAL_NOTES': df.loc[idx, 'SPECIAL_NOTES'],
                    'SERVICE_PERFORMED': df.loc[idx, 'SERVICE_PERFORMED'],
                    'ISSUE_REPORTED': df.loc[idx, 'ISSUE_REPORTED']
                }
                future = executor.submit(self.process_single_call, idx, row_data)
                futures[future] = idx
            
            # Process completed tasks with progress bar
            with tqdm(total=len(futures), desc="  Classifying", ncols=100) as pbar:
                for future in as_completed(futures):
                    try:
                        row_idx, result = future.result()
                        results[row_idx] = result
                    except Exception as e:
                        print(f"\n⚠ Thread error: {e}")
                    pbar.update(1)
        
        # Add results to DataFrame
        df_result = self._add_results_to_dataframe(df, results)
        
        print(f"\n✓ Classification complete!")
        return df_result
    
    def _add_results_to_dataframe(self, df, results):
        """
        Add classification results to DataFrame using all 4 approaches
        
        Args:
            df: Original DataFrame
            results: Dictionary of results keyed by row index
        
        Returns:
            DataFrame with added columns
        """
        df_result = df.copy()
        approach4_records = []
        
        # Initialize new columns
        df_result['Total_Problems'] = 0
        df_result['Overall_Confidence'] = ''
        df_result['Part_Assembly_Concat'] = ''
        df_result['Failure_Mode_Concat'] = ''
        df_result['Fix_Concat'] = ''
        df_result['Confidence_Concat'] = ''
        df_result['Primary_Part'] = ''
        df_result['Primary_Failure'] = ''
        df_result['Primary_Fix'] = ''
        df_result['Primary_Confidence'] = ''
        df_result['All_Problems_JSON'] = ''
        
        # Add individual problem columns (max 3 problems)
        MAX_PROBLEMS = 3
        for i in range(1, MAX_PROBLEMS + 1):
            df_result[f'Part_{i}'] = ''
            df_result[f'Failure_Mode_{i}'] = ''
            df_result[f'Fix_{i}'] = ''
            df_result[f'Confidence_{i}'] = ''
        
        # Process each result
        for idx, result in results.items():
            if result is None:
                continue
            
            problems = result.get("problems", [])
            total_problems = result.get("analysis", {}).get("total_problems_found", 0)
            confidence_level = result.get("analysis", {}).get("confidence_level", "low")
            service_order = df.loc[idx, "SERVICE_ORDER"]
            
            # Store basic info
            df_result.at[idx, 'Total_Problems'] = total_problems
            df_result.at[idx, 'Overall_Confidence'] = confidence_level
            
            # Approach 1: Concatenated
            if problems:
                parts = " | ".join([p.get("part", "") for p in problems])
                failures = " | ".join([p.get("failure_mode", "") for p in problems])
                fixes = " | ".join([p.get("fix", "") for p in problems])
                confidences = " | ".join([p.get("confidence", "") for p in problems])
                
                df_result.at[idx, 'Part_Assembly_Concat'] = parts
                df_result.at[idx, 'Failure_Mode_Concat'] = failures
                df_result.at[idx, 'Fix_Concat'] = fixes
                df_result.at[idx, 'Confidence_Concat'] = confidences
                
                # Primary problem
                df_result.at[idx, 'Primary_Part'] = problems[0].get("part", "")
                df_result.at[idx, 'Primary_Failure'] = problems[0].get("failure_mode", "")
                df_result.at[idx, 'Primary_Fix'] = problems[0].get("fix", "")
                df_result.at[idx, 'Primary_Confidence'] = problems[0].get("confidence", "")
            
            # Approach 2: Separate columns
            for i, problem in enumerate(problems[:MAX_PROBLEMS]):
                df_result.at[idx, f'Part_{i+1}'] = problem.get("part", "")
                df_result.at[idx, f'Failure_Mode_{i+1}'] = problem.get("failure_mode", "")
                df_result.at[idx, f'Fix_{i+1}'] = problem.get("fix", "")
                df_result.at[idx, f'Confidence_{i+1}'] = problem.get("confidence", "")
            
            # Approach 3: JSON
            df_result.at[idx, 'All_Problems_JSON'] = json.dumps(problems)
            
            # Approach 4: Normalized records
            for problem_num, problem in enumerate(problems, 1):
                approach4_records.append({
                    "SERVICE_ORDER": service_order,
                    "Row_Index": idx,
                    "Problem_Number": problem_num,
                    "Part": problem.get("part", ""),
                    "Failure_Mode": problem.get("failure_mode", ""),
                    "Fix": problem.get("fix", ""),
                    "Confidence": problem.get("confidence", ""),
                    "Supporting_Text": problem.get("supporting_text", "")
                })
            
            # If no problems, add unknown record
            if not problems:
                approach4_records.append({
                    "SERVICE_ORDER": service_order,
                    "Row_Index": idx,
                    "Problem_Number": 1,
                    "Part": "Unknown",
                    "Failure_Mode": "Unknown",
                    "Fix": "Unknown",
                    "Confidence": "low",
                    "Supporting_Text": ""
                })
        
        # Create normalized dataframe
        problems_df = pd.DataFrame(approach4_records)
        
        return df_result, problems_df
