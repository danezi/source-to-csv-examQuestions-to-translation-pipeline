import pandas as pd
import random
import os
import re

def generate_output_name(input_files):
    """Generates a combined output name from multiple input files."""
    if not input_files:
        return "Combined_Output_Randomized.csv"
    
    prefixes = []
    for f in input_files:
        # Get filename and remove extensions and common suffixes
        base = os.path.basename(f)
        # Remove common extensions and suffixes to get a clean prefix
        name = re.sub(r'(_ocred_questions)?\.(csv|xlsx|xls)$', '', base, flags=re.IGNORECASE)
        # Take a shorter part of the name if it's too long (e.g., first 15 chars or first word)
        clean_name = name.split('_')[0] if '_' in name else name[:15]
        prefixes.append(clean_name)
    
    # Remove duplicates and join
    unique_prefixes = []
    for p in prefixes:
        if p not in unique_prefixes:
            unique_prefixes.append(p)
            
    combined_name = "_".join(unique_prefixes)
    if len(combined_name) > 50:
        combined_name = combined_name[:50] + "_etc"
        
    return f"Combined_{combined_name}_Randomized.csv"

def random_answers(input_files):
    """Reads multiple files, randomizes answers, and saves to a single combined file."""
    if isinstance(input_files, str):
        input_files = [input_files]
        
    all_dfs = []
    
    for file_path in input_files:
        print(f"Reading file: {file_path}")
        try:
            if file_path.lower().endswith('.csv'):
                # Detect separator and encoding
                header_line = None
                detected_encoding = 'latin-1'
                for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                    try:
                        with open(file_path, 'r', encoding=enc) as f:
                            header_line = f.readline()
                        detected_encoding = enc
                        break
                    except UnicodeDecodeError:
                        continue
                
                if header_line is None:
                    print(f"Could not decode {file_path}. Skipping.")
                    continue

                sep = ';' if ';' in header_line else ','
                header_line = header_line.strip()
                num_cols = len(header_line.split(sep))
                
                try:
                    df = pd.read_csv(file_path, sep=sep, encoding=detected_encoding, usecols=range(num_cols), on_bad_lines='warn')
                except Exception as e:
                    print(f"Pandas read failed for {file_path}: {e}")
                    continue
            else:
                df = pd.read_excel(file_path)
            
            all_dfs.append(df)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    if not all_dfs:
        print("No valid data found to process.")
        return

    # Concatenate all dataframes
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows to process: {len(df)}")

    # Check required columns
    required_columns = ['Frage', 'A', 'B', 'C', 'D', 'Richtig']
    for col in required_columns:
        if col not in df.columns:
            print(f"Missing required column: {col}")
            return

    new_rows = []
    
    mapping_idx_to_letter = {0: 'A', 1: 'B', 2: 'C', 3: 'D'}
    mapping_letter_to_idx = {'A': 0, 'B': 1, 'C': 2, 'D': 3}

    stats_old = {'A':0, 'B':0, 'C':0, 'D':0}
    stats_new = {'A':0, 'B':0, 'C':0, 'D':0}

    for index, row in df.iterrows():
        correct_letter = str(row['Richtig']).strip().upper()
        
        if correct_letter not in mapping_letter_to_idx:
            print(f"Row {index}: Invalid correct answer '{correct_letter}'. Skipping shuffle.")
            new_rows.append(row)
            continue
        
        stats_old[correct_letter] += 1

        answers = [
            str(row['A']),
            str(row['B']),
            str(row['C']),
            str(row['D'])
        ]
        
        correct_answer_idx = mapping_letter_to_idx[correct_letter]
        correct_answer_text = answers[correct_answer_idx]

        shuffled_answers = answers.copy()
        random.shuffle(shuffled_answers)

        new_correct_idx = shuffled_answers.index(correct_answer_text)
        new_correct_letter = mapping_idx_to_letter[new_correct_idx]
        
        stats_new[new_correct_letter] += 1

        row['A'] = shuffled_answers[0]
        row['B'] = shuffled_answers[1]
        row['C'] = shuffled_answers[2]
        row['D'] = shuffled_answers[3]
        row['Richtig'] = new_correct_letter
        row['Richtig_Text'] = correct_answer_text
        
        new_rows.append(row)

    new_df = pd.DataFrame(new_rows)
    
    print("\nStats (Distribution of correct answers):")
    print("OLD:", stats_old)
    print("NEW:", stats_new)

    output_path = generate_output_name(input_files)
    
    try:
        if output_path.lower().endswith('.csv'):
            new_df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        else:
            new_df.to_excel(output_path, index=False)
        print(f"\nSuccessfully saved to: {output_path}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    # You can now provide a list of files here
    input_files = [
        'MFA_Behandlungsassistenz_questions.csv' #, 
        #'Pflegias_Generalistische_Pflege_2_gedruckt_questions.csv'
        #'Büromanagement_3_260104_ocred_questions.csv'
    ]
    
    existing_files = [f for f in input_files if os.path.exists(f)]
    
    if not existing_files:
        print("No input files were found.")
        # Fallback to current dir if no paths match to show example
        print(f"Current directory: {os.getcwd()}")
    else:
        if len(existing_files) < len(input_files):
            missing = set(input_files) - set(existing_files)
            print(f"Warning: Some files were not found: {missing}")
            
        random_answers(existing_files)
