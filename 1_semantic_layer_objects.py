import yaml
import re
import sys
import argparse
import os
import csv
import subprocess
from pathlib import Path
from dotenv import load_dotenv


def extract_dependencies(sql_string):
    """Extracts field dependencies from a SQL string using ${field_name} syntax."""
    if not sql_string:
        return []
    # Find all ${...} references
    matches = re.findall(r'\$\{([^}]+)\}', str(sql_string))
    # Exclude TABLE and return unique dependencies
    return list(set(m for m in matches if m != 'TABLE'))

def process_yml(file_path):
    """
    Parses a dbt YAML file, extracts semantic objects, and identifies both
    internal and external join dependencies.
    Returns a tuple of (rows, external_dependencies).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not read or parse {file_path}: {e}", file=sys.stderr)
        return [], []

    if not isinstance(data, dict):
        return [], []

    rows = []
    external_join_deps = set()
    file_name = os.path.basename(file_path)
    
    for model in data.get('models', []):
        if not isinstance(model, dict):
            continue
            
        model_name = model.get('name', 'Unknown')
        
        # Extract join dependencies
        join_dependent_fields = set() # Fields in the current model used in joins
        model_meta = model.get('meta', {})
        if not model_meta and 'config' in model and isinstance(model['config'], dict):
            model_meta = model['config'].get('meta', {})
            
        if isinstance(model_meta, dict):
            for join_def in model_meta.get('joins', []):
                if isinstance(join_def, dict):
                    sql_on = join_def.get('sql_on', '')
                    # Find all ${...} references
                    matches = re.findall(r'\$\{([^}]+)\}', str(sql_on))
                    for match in set(matches):
                        parts = match.split('.')
                        if len(parts) == 2:
                            ref_model, ref_field = parts
                            if ref_model == model_name or ref_model == 'TABLE':
                                join_dependent_fields.add(ref_field)
                            else:
                                external_join_deps.add(match) # e.g., 'map_content.step_id'
                        elif len(parts) == 1 and parts[0] != 'TABLE':
                            join_dependent_fields.add(parts[0])

        for column in model.get('columns', []):
            if not isinstance(column, dict):
                continue
                
            col_name = column.get('name')
            col_desc = column.get('description', '')
            
            meta = column.get('meta', {})
            if not meta and 'config' in column and isinstance(column['config'], dict):
                meta = column['config'].get('meta', {})
                
            if not isinstance(meta, dict):
                meta = {} # Ensure meta is a dict

            is_semantic_object = 'dimension' in meta or 'metrics' in meta or 'additional_dimensions' in meta

            # Common logic for adding dependencies
            def get_dependencies(name, base_deps=None):
                deps = list(base_deps) if base_deps else []
                if name in join_dependent_fields:
                    deps.append('join sql_on')
                return ', '.join(deps) if deps else 'None'

            if 'dimension' in meta and isinstance(meta['dimension'], dict):
                dim = meta['dimension']
                rows.append({
                    'File Name': file_name, 'Model Name': model_name,
                    'Field Type': 'Dimension', 'Internal Name': col_name,
                    'Label': dim.get('label', col_name),
                    'Description': dim.get('description', col_desc),
                    'Data Type': dim.get('type', '(implicit)'),
                    'SQL / Logic': dim.get('sql', '(implicit)'),
                    'Groups': ', '.join(dim.get('groups', [])) if dim.get('groups') else '(none)',
                    'Dependencies': get_dependencies(col_name)
                })
            
            if 'additional_dimensions' in meta and isinstance(meta['additional_dimensions'], dict):
                for add_dim_name, add_dim in meta['additional_dimensions'].items():
                    if isinstance(add_dim, dict):
                        sql = add_dim.get('sql', '(implicit)')
                        base_deps = extract_dependencies(sql)
                        rows.append({
                            'File Name': file_name, 'Model Name': model_name,
                            'Field Type': 'Add. Dimension', 'Internal Name': add_dim_name,
                            'Label': add_dim.get('label', add_dim_name),
                            'Description': add_dim.get('description', ''),
                            'Data Type': add_dim.get('type', '(implicit)'),
                            'SQL / Logic': sql,
                            'Groups': ', '.join(add_dim.get('groups', [])) if add_dim.get('groups') else '(none)',
                            'Dependencies': get_dependencies(add_dim_name, base_deps)
                        })
            
            if 'metrics' in meta and isinstance(meta['metrics'], dict):
                for metric_name, metric in meta['metrics'].items():
                    if isinstance(metric, dict):
                        sql = metric.get('sql', f'(implicit on {col_name})')
                        base_deps = extract_dependencies(metric.get('sql', ''))
                        if not metric.get('sql'):
                            base_deps = [col_name]
                        rows.append({
                            'File Name': file_name, 'Model Name': model_name,
                            'Field Type': 'Metric', 'Internal Name': metric_name,
                            'Label': metric.get('label', metric_name),
                            'Description': metric.get('description', ''),
                            'Data Type': metric.get('type', '(implicit)'),
                            'SQL / Logic': sql,
                            'Groups': ', '.join(metric.get('groups', [])) if metric.get('groups') else '(none)',
                            'Dependencies': get_dependencies(metric_name, base_deps)
                        })

            if not is_semantic_object:
                rows.append({
                    'File Name': file_name, 'Model Name': model_name,
                    'Field Type': 'Column', 'Internal Name': col_name,
                    'Label': col_name, 'Description': col_desc,
                    'Data Type': '(implicit)', 'SQL / Logic': '(implicit)',
                    'Groups': '(none)',
                    'Dependencies': get_dependencies(col_name)
                })

    return rows, list(external_join_deps)

def write_csv(rows, output_file):
    """Outputs the extracted rows as a CSV file."""
    if not rows:
        print("No fields found.")
        return
        
    headers = ['File Name', 'Model Name', 'Field Type', 'Internal Name', 'Label', 'Description', 'Data Type', 'SQL / Logic', 'Groups', 'Dependencies']
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            # Sort rows for consistent output
            sorted_rows = sorted(rows, key=lambda x: (x['File Name'], x['Model Name'], x['Internal Name']))
            for row in sorted_rows:
                writer.writerow(row)
        print(f"Successfully exported data to {output_file}")
    except Exception as e:
        print(f"Error writing CSV: {e}", file=sys.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract Lightdash semantic layer information from YAML files.')
    parser.add_argument('--path', '-p', default=os.path.join(os.path.dirname(__file__), '..', 'models'), help='Path to a Lightdash YAML file or a directory containing YAML files')
    parser.add_argument('--output', '-o', default=os.path.join(os.path.dirname(__file__), 'outputs', '1_semantic_layer_objects.csv'), help='Path to the output CSV file')
    args = parser.parse_args()

    
    try:
        import yaml
    except ImportError:
        print("Error: The 'PyYAML' package is required but not installed.", file=sys.stderr)
        print("Please install it by running: pip install pyyaml", file=sys.stderr)
        sys.exit(1)

    all_rows = []
    all_external_deps = set()
    target_path = Path(args.path)
    
    # Pass 1: Collect all data and external dependencies
    files_to_process = []
    if target_path.is_file():
        if target_path.suffix in ['.yml', '.yaml']:
            files_to_process.append(target_path)
        else:
            print(f"Error: {target_path} is not a .yml or .yaml file.", file=sys.stderr)
            sys.exit(1)
    elif target_path.is_dir():
        files_to_process.extend(target_path.rglob('*.yml'))
        files_to_process.extend(target_path.rglob('*.yaml'))
    else:
        print(f"Error: Path {target_path} does not exist.", file=sys.stderr)
        sys.exit(1)

    for file_path in files_to_process:
        rows, external_deps = process_yml(str(file_path))
        all_rows.extend(rows)
        all_external_deps.update(external_deps)

    # Pass 2: Enrich dependencies based on external references
    for row in all_rows:
        full_name = f"{row['Model Name']}.{row['Internal Name']}"
        if full_name in all_external_deps:
            if row['Dependencies'] == 'None':
                row['Dependencies'] = 'join sql_on'
            elif 'join sql_on' not in row['Dependencies']:
                # Append to existing dependencies
                dep_list = row['Dependencies'].split(', ')
                dep_list.append('join sql_on')
                row['Dependencies'] = ', '.join(dep_list)
        
    write_csv(all_rows, args.output)
