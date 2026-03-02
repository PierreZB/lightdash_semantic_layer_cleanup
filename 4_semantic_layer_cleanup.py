# --- Python Script for Semantic Layer Cleanup ---
#
# This script automates the process of identifying and commenting out unused fields
# in dbt YAML files based on their usage in Lightdash charts.
#
# Pre-requisites:
# 1. Ensure you have a CSV export from the '3_semantic_layer_objects_usage.csv' script.
# 2. You must have the `python-dotenv` library installed. If not, run:
#    pip install python-dotenv
# 3. Create a `.env` file in the same directory as this script. You can rename the
#    `.env.example` file and configure the VALIDATION_COMMAND variable.
#
# ---
import csv
import os
import re
import subprocess
import sys
import datetime
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
BASE_DIR = os.path.dirname(__file__)
CSV_PATH = os.path.join(BASE_DIR, 'outputs', '3_semantic_layer_objects_usage.csv')
DBT_DIR = os.path.abspath(os.path.join(BASE_DIR, '..'))

# The validation command is now configurable via the .env file
QUICK_VALIDATION_COMMAND = os.getenv('QUICK_VALIDATION_COMMAND')

def format_time(seconds):
    """Converts seconds into a human-readable string like '1m 25s'."""
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"

def run_validation():
    if not QUICK_VALIDATION_COMMAND:
        print("🚨 ERROR: QUICK_VALIDATION_COMMAND is not set in your .env file.")
        print("Please configure it to run Lightdash validation (e.g., conda run -n dbt lightdash validate...).")
        sys.exit(1)

    print(f"  -> Running validation command: {QUICK_VALIDATION_COMMAND}")
    # Run command in the dbt root directory
    result = subprocess.run(
        QUICK_VALIDATION_COMMAND, 
        shell=True, 
        cwd=DBT_DIR, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True,
        # Using /bin/bash is not portable; shell=True will use the system's default shell.
        # executable='/bin/bash' 
    )
    return result.returncode == 0, result.stdout, result.stderr

def find_file(filename):
    for root, dirs, files in os.walk(DBT_DIR):
        if filename in files:
            return os.path.join(root, filename)
    return None

def comment_out_field(filepath, model_name, field_name, field_type):
    """
    Comments out a field in a dbt YAML file. If the field is the last active
    item under a parent key (e.g., 'metrics'), it comments out the parent key as well.
    """
    with open(filepath, 'r') as f:
        lines = f.readlines()

    # --- Stage 1: Analysis - Find all relevant line indices and properties ---
    model_re = re.compile(r"^(\s*)-\s*name:\s*['\"]?" + re.escape(model_name) + r"['\"]?\s*(?:#.*)?$")
    if field_type.lower() == 'dimension':
        field_re = re.compile(r"^(\s*)-\s*name:\s*['\"]?" + re.escape(field_name) + r"['\"]?\s*(?:#.*)?$")
    else:
        field_re = re.compile(r"^(\s*)['\"]?" + re.escape(field_name) + r"['\"]?:\s*(?:#.*)?$")

    model_start_idx, model_end_idx, model_indent = -1, len(lines), -1
    field_idx, field_indent = -1, -1
    parent_idx, parent_indent = -1, -1

    # Find model boundaries
    for i, line in enumerate(lines):
        if model_start_idx == -1:
            m = model_re.match(line)
            if m:
                model_start_idx = i
                model_indent = len(m.group(1))
        elif i > model_start_idx:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= model_indent and line.lstrip().startswith('-'):
                    model_end_idx = i
                    break
    if model_start_idx == -1: return False

    # Find field within model
    for i in range(model_start_idx, model_end_idx):
        fm = field_re.match(lines[i])
        if fm:
            field_idx = i
            field_indent = len(fm.group(1))
            break
    if field_idx == -1: return False

    # Find parent of the field
    for i in range(field_idx - 1, model_start_idx - 1, -1):
        line = lines[i]
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            current_indent = len(line) - len(line.lstrip())
            if current_indent < field_indent:
                parent_idx = i
                parent_indent = current_indent
                break
    
    # --- Stage 2: Sibling Analysis ---
    is_last_child = False
    if parent_idx != -1:
        sibling_count = 0
        parent_block_end_idx = model_end_idx
        for i in range(parent_idx + 1, model_end_idx):
            line = lines[i]
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= parent_indent:
                    parent_block_end_idx = i
                    break
        
        for i in range(parent_idx + 1, parent_block_end_idx):
            line = lines[i]
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_indent = len(line) - len(line.lstrip())
                if current_indent == field_indent:
                    sibling_count += 1
        
        if sibling_count <= 1:
            is_last_child = True

    # --- Stage 3: Modification ---
    new_lines = []
    commenting = False
    modified = False

    for i, line in enumerate(lines):
        # If this is the parent of a last child, comment it out.
        if is_last_child and i == parent_idx:
            if not line.lstrip().startswith('#'):
                new_lines.append('# ' + line)
            else:
                new_lines.append(line) # Already commented
            continue

        # If we are currently commenting out a block
        if commenting:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= field_indent:
                    commenting = False # Stop commenting, process this line normally
                else:
                    new_lines.append('# ' + line) # It's a property of the field
                    continue
            else:
                # Comment out empty lines/comments within the block
                new_lines.append('# ' + line)
                continue
        
        # If we are at the field to be commented
        if i == field_idx:
            field_indent = len(line) - len(line.lstrip())
            commenting = True
            modified = True
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_lines.append(f"{' ' * field_indent}# Auto Cleanup {timestamp}\n")
            new_lines.append('# ' + line)
            continue
        
        new_lines.append(line)

    if modified:
        with open(filepath, 'w') as f:
            f.writelines(new_lines)
            
    return modified

def add_breaking_change_comment(filepath, model_name, field_name, field_type, breaking_change_tag):
    with open(filepath, 'r') as f:
        lines = f.readlines()
        
    in_model = False
    model_indent = -1
    new_lines = []
    modified = False
    
    model_re = re.compile(r"^(\s*)-\s*name:\s*['\"]?" + re.escape(model_name) + r"['\"]?\s*(?:#.*)?$")
    
    if field_type.lower() == 'dimension':
        field_re = re.compile(r"^(\s*)-\s*name:\s*['\"]?" + re.escape(field_name) + r"['\"]?\s*(?:#.*)?$")
    else:
        field_re = re.compile(r"^(\s*)['\"]?" + re.escape(field_name) + r"['\"]?:\s*(?:#.*)?$")

    for line in lines:
        if not in_model:
            m = model_re.match(line)
            if m:
                in_model = True
                model_indent = len(m.group(1))
            new_lines.append(line)
            continue
            
        if in_model:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= model_indent and line.lstrip().startswith('-'):
                    if re.match(r'^\s*-\s*name:', line):
                        in_model = False
            
            if in_model and not modified:
                fm = field_re.match(line)
                if fm:
                    modified = True
                    new_lines.append(f"{fm.group(1)}{breaking_change_tag}\n")
            
            new_lines.append(line)

    if modified:
        with open(filepath, 'w') as f:
            f.writelines(new_lines)
            
    return modified

def main():
    targets = []
    # Read CSV and filter targets
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('Model Used in Charts') == 'Yes' and row.get('Field Used in Chart') == 'Not used':
                dependency_level_str = row.get('Dependency Level')
                if dependency_level_str and dependency_level_str.isdigit():
                    row['Dependency Level'] = int(dependency_level_str)
                else:
                    row['Dependency Level'] = 0
                targets.append(row)

    # Sort targets by Dependency Level in descending order
    targets.sort(key=lambda x: x.get('Dependency Level', 0), reverse=True)
                
    total_targets = len(targets)
    print(f"Found {total_targets} targets to process.")

    if not targets:
        print("No targets to process. Exiting.")
        return

    # Ask for user confirmation
    user_input = input(f"Found {total_targets} fields to comment out.\nAre you sure you want to proceed? (yes/no): ").lower()
    
    if user_input not in ['yes', 'y']:
        print("Operation cancelled by user.")
        sys.exit(0)
    
    start_time = time.time()
    breaking_changes = []

    for i, target in enumerate(targets):
        filename = target.get('File Name')
        model_name = target.get('Model Name')
        field_type = target.get('Field Type')
        field_name = target.get('Internal Name')
        
        filepath = find_file(filename)
        if not filepath:
            print(f"Warning: File {filename} not found.")
            continue

        # Store original content before modification
        with open(filepath, 'r') as f:
            original_content = f.read()
            
        print(f"\n[{i+1}/{total_targets}] Processing '{model_name}.{field_name}' in {filename}...")
        modified = comment_out_field(filepath, model_name, field_name, field_type)
        
        if modified:
            print("  -> Field commented successfully. Running Lightdash validation...")
            success, stdout, stderr = run_validation()
            
            if not success:
                print(f"\n🚨 ERROR: Validation failed after commenting out {field_name} (Type: {field_type}) in {filename}")
                print("  -> Reverting changes and marking as a breaking change.")
                
                # Revert the file to its original state
                with open(filepath, 'w') as f:
                    f.write(original_content)
                
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                breaking_change_tag = f"# BREAKING CHANGE - Auto Cleanup {timestamp}"
                
                # Add the breaking change comment
                add_breaking_change_comment(filepath, model_name, field_name, field_type, breaking_change_tag)
                
                breaking_changes.append({'model': model_name, 'field': field_name})

                print("Lightdash Validate Output:")
                print(stdout)
                print(stderr)
                
                print(f"\n🚨 IMPORTANT: The semantic object '{model_name}.{field_name}' broke one or more Lightdash asset(s).")
                print(f"The change to '{field_name}' in file '{filename}' has been reverted.")
                print(f"The object has been flagged in its YML file '{filepath}' with the tag: '{breaking_change_tag}'")
                
                user_choice = input("\nDo you want to continue with the next object? (yes/no): ").lower()
                if user_choice not in ['yes', 'y']:
                    print("\nStopping process as requested by user.")
                    break
            else:
                print("  -> Validation passed!")
        else:
            print(f"  -> Field {field_name} not found or already commented out.")
        
        # Timing information
        elapsed_time = time.time() - start_time
        processed_count = i + 1
        avg_time_per_object = elapsed_time / processed_count
        remaining_count = total_targets - processed_count
        estimated_time_remaining = avg_time_per_object * remaining_count
        
        print(f"  -> Status: Elapsed: {format_time(elapsed_time)} | Avg: {format_time(avg_time_per_object)}/obj | ETA: {format_time(estimated_time_remaining)}")

    # --- Final Summary ---
    print("\n" + "="*50)
    print("Script finished processing all targets.")
    
    if breaking_changes:
        print("\n🚨 Summary of Breaking Changes Found:")
        print("The following objects caused validation errors and have been flagged:")
        for bc in breaking_changes:
            print(f"  - Model: {bc['model']}, Field: {bc['field']}")
    else:
        print("\n✅ No breaking changes were found during the process.")

    print("="*50)
    print("Script has finished.")

if __name__ == '__main__':
    main()