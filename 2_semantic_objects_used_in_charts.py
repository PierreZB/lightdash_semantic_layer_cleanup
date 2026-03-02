import os
import yaml
import glob
import pandas as pd
import argparse
import sys
import subprocess
from dotenv import load_dotenv

def extract_filter_fields(filter_node):
    """Recursively extract target fieldIds from a filter node."""
    fields = []
    if isinstance(filter_node, dict):
        if 'target' in filter_node and 'fieldId' in filter_node['target']:
            fields.append(filter_node['target']['fieldId'])
        if 'and' in filter_node:
            for item in filter_node['and']:
                fields.extend(extract_filter_fields(item))
        if 'or' in filter_node:
            for item in filter_node['or']:
                fields.extend(extract_filter_fields(item))
    elif isinstance(filter_node, list):
        for item in filter_node:
            fields.extend(extract_filter_fields(item))
    return fields

def run_lightdash_download():
    """
    Asks for user confirmation and then executes the lightdash download command
    configured in the .env file. Skips the download if the user says no.
    """
    load_dotenv()
    download_command = os.getenv('DOWNLOAD_COMMAND')
    dbt_project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    if not download_command:
        print("🚨 WARNING: DOWNLOAD_COMMAND is not set in your .env file.", file=sys.stderr)
        print("Skipping 'lightdash download' step. The script will proceed with local files.", file=sys.stderr)
        return

    # Ask for user confirmation
    user_input = input("Do you want to run 'lightdash download' to refresh project files? (yes/no): ").lower()
    
    if user_input not in ['yes', 'y']:
        print("Skipping 'lightdash download'. Continuing with local files.")
        return

    print("Running 'lightdash download' to refresh project files...")
    print(f"Executing command: {download_command}")
    
    try:
        result = subprocess.run(
            download_command,
            shell=True,
            cwd=dbt_project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True  # This will raise a CalledProcessError if the command returns a non-zero exit code
        )
        print("✅ 'lightdash download' completed successfully.")
        print(result.stdout)
    except FileNotFoundError:
        print(f"🚨 ERROR: Command not found. Ensure the command '{download_command.split()[0]}' is correct and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print("🚨 ERROR: 'lightdash download' failed.", file=sys.stderr)
        print("Return Code:", e.returncode, file=sys.stderr)
        print("\n--- STDOUT ---", file=sys.stderr)
        print(e.stdout, file=sys.stderr)
        print("\n--- STDERR ---", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def process_yaml(file_path, mapping=None):
    if mapping is None:
        mapping = {}
        
    """Process a single Lightdash chart YAML file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = yaml.safe_load(f)
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
            return []
            
    if not data or 'metricQuery' not in data:
        return []
        
    chart_slug = os.path.basename(file_path).replace('.yml', '').replace('.yaml', '')
    chart_name = data.get('name', '')
    chart_description = data.get('description', '')
    space_slug = data.get('spaceSlug', '')
    updated_at = data.get('updatedAt', '')
    metric_query = data.get('metricQuery', {})
    explore_name = metric_query.get('exploreName', '')
    
    # 1. Gather custom definitions
    custom_dims = {}
    for cd in metric_query.get('customDimensions', []):
        sql = cd.get('sql', '')
        info = {'sql': sql, 'baseDimensionName': '', 'type': cd.get('dimensionType', cd.get('type', '')), 'table': cd.get('table', '')}
        if 'id' in cd:
            custom_dims[cd['id']] = info
        elif 'name' in cd:
            custom_dims[cd['name']] = info
            
    custom_metrics = {}
    for cm in metric_query.get('additionalMetrics', []):
        sql = cm.get('sql', '')
        info = {'sql': sql, 'baseDimensionName': cm.get('baseDimensionName', ''), 'type': cm.get('type', ''), 'table': cm.get('table', '')}
        if 'name' in cm:
            # Custom metrics usually appear as {exploreName}_{name} in the field list
            if explore_name:
                custom_metrics[f"{explore_name}_{cm['name']}"] = info
            custom_metrics[cm['name']] = info # Fallback just in case
            
    table_calcs = {}
    for tc in metric_query.get('tableCalculations', []):
        sql = tc.get('sql', '')
        info = {'sql': sql, 'baseDimensionName': '', 'type': tc.get('type', ''), 'table': ''}
        if 'name' in tc:
            table_calcs[tc['name']] = info
            
    # Keep track of fields and their usage/type
    fields_info = {}
    
    def add_field(name, usage, default_type):
        if name not in fields_info:
            fields_info[name] = {'usages': set(), 'type': default_type, 'sql': '', 'baseDimensionName': '', 'additionalType': '', 'table': ''}
        fields_info[name]['usages'].add(usage)
        
        # Override the type if it matches any of the custom definitions
        if name in custom_dims:
            fields_info[name]['type'] = 'Custom Dimension'
            fields_info[name]['sql'] = custom_dims[name]['sql']
            fields_info[name]['additionalType'] = custom_dims[name]['type']
            fields_info[name]['table'] = custom_dims[name]['table']
        elif name in custom_metrics:
            fields_info[name]['type'] = 'Custom Metric'
            fields_info[name]['sql'] = custom_metrics[name]['sql']
            fields_info[name]['baseDimensionName'] = custom_metrics[name]['baseDimensionName']
            fields_info[name]['additionalType'] = custom_metrics[name]['type']
            fields_info[name]['table'] = custom_metrics[name]['table']
        elif name in table_calcs:
            fields_info[name]['type'] = 'Table Calculation'
            fields_info[name]['sql'] = table_calcs[name]['sql']
            fields_info[name]['additionalType'] = table_calcs[name]['type']
            fields_info[name]['table'] = table_calcs[name]['table']
            
    # Process Dimensions
    for dim in metric_query.get('dimensions', []):
        add_field(dim, 'Dimension', 'Raw Dimension')
        
    # Process Metrics
    for metric in metric_query.get('metrics', []):
        add_field(metric, 'Metric', 'Raw Metric')
        
    # Process Filters
    filters = metric_query.get('filters', {})
    dim_filters = filters.get('dimensions', {})
    for f in extract_filter_fields(dim_filters):
        add_field(f, 'Filter', 'Raw Dimension')
        
    metric_filters = filters.get('metrics', {})
    for f in extract_filter_fields(metric_filters):
        add_field(f, 'Filter', 'Raw Metric')
        
    # Process Table Calculations
    for tc in table_calcs:
        add_field(tc, 'Table Calculation', 'Table Calculation')
        
    # Common suffixes added by Lightdash for dimension groups
    ld_suffixes = ['_day', '_month', '_year', '_week', '_quarter', '_hour', '_minute', '_second', '_date', '_timestamp', '_raw', '_millisecond']
    
    results = []
    for field_name, info in fields_info.items():
        table_name = info.get('table', '')
        original_field_name = field_name
        
        # 1. Try exact mapping match
        match = mapping.get(field_name)
        
        # 2. Try matching after removing common Lightdash suffixes
        if not match:
            for suffix in ld_suffixes:
                if field_name.endswith(suffix):
                    base_name = field_name[:-len(suffix)]
                    if base_name in mapping:
                        match = mapping[base_name]
                        break
        
        # Parse logic to extract original field name and table name
        if info['type'] == 'Table Calculation':
            table_name = ''
            original_field_name = field_name
        elif match:
            table_name = match['Table Name']
            original_field_name = match['Original Field Name']
        elif info['type'] in ('Custom Dimension', 'Custom Metric'):
            if table_name and field_name.startswith(f"{table_name}_"):
                original_field_name = field_name[len(table_name)+1:]
            elif explore_name and field_name.startswith(f"{explore_name}_"):
                table_name = explore_name
                original_field_name = field_name[len(explore_name)+1:]
        else:
            # Raw Dimension, Metric, Filter
            if explore_name and field_name.startswith(f"{explore_name}_"):
                table_name = explore_name
                original_field_name = field_name[len(explore_name)+1:]
            else:
                # Might be from a joined table. We leave table_name empty (or as-is) 
                # because we don't definitively know where the table name ends and field begins.
                pass
                
        results.append({
            'Chart Slug': chart_slug,
            'Chart Display Name': chart_name,
            'Chart Description': chart_description,
            'Space Slug': space_slug,
            'Updated At': updated_at,
            'Field Name': field_name,
            'Table Name': table_name,
            'Original Field Name': original_field_name,
            'Usage Context': ', '.join(sorted(list(info['usages']))),
            'Field Type': info['type'],
            'SQL': info['sql'],
            'Base Dimension Name': info['baseDimensionName'],
            'Additional Type': info['additionalType']
        })
        
    return results

def main():
    parser = argparse.ArgumentParser(description="Extract fields from Lightdash chart YAML files.")
    parser.add_argument('--directory', '-d', default=os.path.join(os.path.dirname(__file__), '..', 'lightdash', 'charts'), help="Directory containing .yml files")
    parser.add_argument('--output', '-o', default=os.path.join(os.path.dirname(__file__), 'outputs', '2_semantic_objects_used_in_charts.csv'), help="Output CSV file path")
    parser.add_argument('--sl-mapping', '-m', default=os.path.join(os.path.dirname(__file__), 'outputs', '1_semantic_layer_objects.csv'), help="Path to semantic layer CSV mapping")
    
    args = parser.parse_args()

    # Run the lightdash download command first
    run_lightdash_download()

    # Ask user if they want to proceed with analysis
    user_input = input("\nDo you want to parse chart files to identify used semantic objects? (yes/no): ").lower()
    if user_input not in ['yes', 'y']:
        print("⏭️ Skipping chart analysis.")
        return

    print("✅ Proceeding with chart analysis...")
    
    mapping = {}
    if os.path.exists(args.sl_mapping):
        try:
            sl_df = pd.read_csv(args.sl_mapping)
            for _, row in sl_df.iterrows():
                model_name = str(row.get('Model Name', ''))
                internal_name = str(row.get('Internal Name', ''))
                if model_name and internal_name and model_name != 'nan' and internal_name != 'nan':
                    # Lightdash fieldId is typically modelName_fieldName
                    field_key = f"{model_name}_{internal_name}"
                    mapping[field_key] = {'Table Name': model_name, 'Original Field Name': internal_name}
            print(f"✅ Loaded {len(mapping)} fields from semantic layer mapping.")
        except Exception as e:
            print(f"Error loading {args.sl_mapping}: {e}")
            
    all_results = []
    # Find all .yml and .yaml files in the target directory
    yaml_files = glob.glob(os.path.join(args.directory, '*.yml')) + glob.glob(os.path.join(args.directory, '*.yaml'))
    
    if not yaml_files:
        print(f"No YAML files found in {args.directory}")
        return

    for file_path in yaml_files:
        all_results.extend(process_yaml(file_path, mapping))
        
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(args.output, index=False)
        print(f"✅ Extracted {len(all_results)} fields from {len(yaml_files)} charts.")
        print(f"📁 Results saved to: {args.output}")
    else:
        print("No fields found in the provided YAML files.")

if __name__ == '__main__':
    main()