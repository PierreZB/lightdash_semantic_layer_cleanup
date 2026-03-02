import csv
import re
import os
import glob
from collections import defaultdict, Counter

def get_dependency_level(field, dependencies_map, levels_cache, visited_path):
    """Recursively calculate the dependency level of a field."""
    if field in levels_cache:
        return levels_cache[field]
    if field in visited_path:
        # Cycle detected
        return float('inf')

    visited_path.add(field)

    deps = dependencies_map.get(field, set())
    max_dep_level = -1

    if not deps:
        level = 0
    else:
        for dep in deps:
            dep_level = get_dependency_level(dep, dependencies_map, levels_cache, visited_path)
            if dep_level > max_dep_level:
                max_dep_level = dep_level
        
        if max_dep_level == float('inf'):
            level = float('inf')
        elif max_dep_level == -1: # All deps had no level (e.g., not in map)
            level = 0
        else:
            level = 1 + max_dep_level

    visited_path.remove(field)
    levels_cache[field] = level
    return level

def main():
    base_dir = os.path.dirname(__file__)
    sl_objects_path = os.path.join(base_dir, 'outputs', '1_semantic_layer_objects.csv')
    sl_charts_path = os.path.join(base_dir, 'outputs', '2_semantic_objects_used_in_charts.csv')
    output_path = os.path.join(base_dir, 'outputs', '3_semantic_layer_objects_usage.csv')
    charts_dir = os.path.join(base_dir, '..', 'lightdash', 'charts')

    print("Building dependency graph...")
    # 1. Parse sl_objects to build dependency graph
    dependencies_map = {}
    all_sl_fields = []
    
    try:
        with open(sl_objects_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # Read all rows into memory so we can append new columns later
            for row in reader:
                all_sl_fields.append(row)
                
                model_name = row.get('Model Name', '').strip()
                internal_name = row.get('Internal Name', '').strip()
                if not model_name or not internal_name:
                    continue
                    
                deps = set()
                
                # Extract from SQL / Logic using regex
                sql_logic = row.get('SQL / Logic', '')
                refs = re.findall(r'\$\{([^}]+)\}', sql_logic)
                for ref in refs:
                    ref = ref.strip()
                    if ref == 'TABLE' or ref.startswith('TABLE.'):
                        continue
                    deps.add(ref)
                    
                # Extract from Dependencies column
                dep_col = row.get('Dependencies', '')
                if dep_col and dep_col.lower() != 'none':
                    for d in dep_col.split(','):
                        d = d.strip()
                        if d:
                            deps.add(d)
                            
                # Resolve dependencies to (model, field) tuples
                resolved_deps = set()
                for dep in deps:
                    if '.' in dep:
                        parts = dep.split('.', 1)
                        resolved_deps.add((parts[0].strip(), parts[1].strip()))
                    else:
                        resolved_deps.add((model_name, dep))
                        
                dependencies_map[(model_name, internal_name)] = resolved_deps
    except FileNotFoundError:
        print(f"Error: Could not find {sl_objects_path}")
        return

    print("Calculating dependency levels...")
    dependency_levels = {}
    all_fields_keys = list(dependencies_map.keys())
    for field_key in all_fields_keys:
        if field_key not in dependency_levels:
            get_dependency_level(field_key, dependencies_map, dependency_levels, set())

    # Replace inf with a high number for fields in cycles
    for field, level in dependency_levels.items():
        if level == float('inf'):
            print(f"Warning: Cycle detected involving {field}. Assigning a high dependency level.")
            dependency_levels[field] = 999

    print("Reading charts data...")
    # 2. Read sl_charts to find initial used fields
    used_fields = set()
    used_models = set()
    field_usages = defaultdict(Counter)
    
    # New structures for tracking chart slugs and display names
    field_slugs = defaultdict(set)
    field_display_names = defaultdict(set)
    
    try:
        with open(sl_charts_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                table_name = row.get('Table Name', '').strip()
                if table_name:
                    used_models.add(table_name)
                
                usage_context = row.get('Usage Context', '').strip()
                if usage_context and usage_context != 'Table Calculation':
                    field_name = row.get('Original Field Name', '').strip()
                    if table_name and field_name:
                        field_key = (table_name, field_name)
                        used_fields.add(field_key)
                        
                        # Track the chart slugs and display names directly
                        chart_slug = row.get('Chart Slug', '').strip()
                        chart_display = row.get('Chart Display Name', '').strip()
                        if chart_slug:
                            field_slugs[field_key].add(chart_slug)
                        if chart_display:
                            field_display_names[field_key].add(chart_display)
                        
                        # Parse comma-separated usages and count them
                        for context in usage_context.split(','):
                            context = context.strip()
                            if context:
                                field_usages[field_key][context] += 1
    except FileNotFoundError:
        print(f"Error: Could not find {sl_charts_path}")
        return

    print("Resolving indirect dependencies and propagating chart usage...")
    # 3. Traverse dependency graph to find all indirectly used fields
    indirect_deps = set()
    visited = set()
    queue = []
    
    # Push directly used fields to the queue initially
    for field in used_fields:
        visited.add(field)
        queue.append(field)
        
    # Iterative relaxation to propagate charts backwards through the dependency graph
    while queue:
        current_field = queue.pop(0)
        current_slugs = field_slugs[current_field]
        current_displays = field_display_names[current_field]
        
        # Look up what 'current_field' depends on
        deps = dependencies_map.get(current_field, set())
        for dep in deps:
            indirect_deps.add(dep)
            
            old_len_slugs = len(field_slugs[dep])
            
            # Propagate charts to the dependency
            field_slugs[dep].update(current_slugs)
            field_display_names[dep].update(current_displays)
            
            # Re-queue if we discovered this dependency for the first time
            # OR if we added new charts to it (so it can propagate them further down)
            if dep not in visited or len(field_slugs[dep]) > old_len_slugs:
                visited.add(dep)
                queue.append(dep)

    print("Starting second pass QA...")
    # 4. Second pass QA: Scan raw chart files for additional usage
    print(f"Scanning directory for chart usage: {charts_dir}")
    yml_files = glob.glob(os.path.join(charts_dir, '**', '*.yml'), recursive=True)
    
    file_contents = {}
    for file_path in yml_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_contents[file_path] = f.read()
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            
    print(f"Loaded {len(file_contents)} .yml files for QA.")

    found_in_second_pass = set()
    for row in all_sl_fields:
        model_used = row.get('Model Name', '') in used_models
        if model_used:
            m = row.get('Model Name', '').strip()
            f_name = row.get('Internal Name', '').strip()
            if m and f_name:
                search_string = f"{m}_{f_name}"
                for content in file_contents.values():
                    if search_string in content:
                        found_in_second_pass.add((m, f_name))
                        break

    print(f"Identified {len(found_in_second_pass)} fields for update in second pass.")


    print("Writing output file...")
    # 5. Write to output file
    try:
        if not all_sl_fields:
            print("No fields found in 1_semantic_layer_objects.csv")
            return
            
        original_fieldnames = list(all_sl_fields[0].keys())
        new_columns = [
            'Model Used in Charts', 
            'Field Used in Chart', 
            'Field Usage Types',
            'Unique Charts Count',
            'List of Charts',
            'Dependency Level'
        ]
        
        # Append new columns to fieldnames while keeping original order
        fieldnames = original_fieldnames + [col for col in new_columns if col not in original_fieldnames]
        
        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in all_sl_fields:
                model_name = row.get('Model Name', '').strip()
                internal_name = row.get('Internal Name', '').strip()
                field_key = (model_name, internal_name)

                # Check for primary key in the SQL/Logic column
                is_primary_key = 'primary_key' in row.get('SQL / Logic', '').lower()

                if is_primary_key:
                    # Flag in dependencies column for the output
                    current_deps = row.get('Dependencies', '')
                    if current_deps and current_deps.lower() != 'none':
                        # Avoid adding if already present
                        if 'primary_key' not in current_deps.lower():
                            row['Dependencies'] = current_deps + ', primary_key'
                    else:
                        row['Dependencies'] = 'primary_key'
                
                is_direct = field_key in used_fields
                is_dep = field_key in indirect_deps
                
                # 1. 'Model Used in Charts'
                row['Model Used in Charts'] = 'Yes' if model_name in used_models else 'No'
                
                # 2. 'Field Used in Chart'
                dependencies_col_val = row.get('Dependencies', '')
                is_join_sql_on = "join sql_on" in dependencies_col_val.lower()

                if is_direct and is_dep:
                    usage_status = 'Direct use and dependency'
                elif is_direct:
                    usage_status = 'Direct use'
                elif is_dep:
                    usage_status = 'Dependency'
                elif is_join_sql_on:
                    usage_status = 'Used in SQL JOIN'
                else:
                    usage_status = 'Not used'

                # Second pass QA check
                if usage_status == 'Not used' and (model_name, internal_name) in found_in_second_pass:
                    usage_status = 'Used (second pass)'
                
                if is_primary_key:
                    if usage_status == 'Not used':
                        usage_status = 'Primary Key'
                    elif 'Primary Key' not in usage_status:
                        usage_status += ', Primary Key'

                row['Field Used in Chart'] = usage_status
                
                # 3. 'Field Usage Types'
                usage_counter = field_usages.get(field_key, Counter())
                usage_types = []
                if usage_counter:
                    usage_types.extend([f"{k} {v}" for k, v in usage_counter.items()])
                
                if 'Used in SQL JOIN' in usage_status and 'Used in SQL JOIN' not in usage_types:
                    usage_types.append('Used in SQL JOIN')
                
                if is_primary_key and 'Primary Key' not in usage_types:
                    usage_types.append('Primary Key')

                if usage_types:
                    usage_str = ", ".join(usage_types)
                elif is_primary_key:
                    usage_str = 'Primary Key'
                else:
                    usage_str = "None"
                row['Field Usage Types'] = usage_str
                
                # 4. 'Unique Charts Count'
                slugs = field_slugs.get(field_key, set())
                row['Unique Charts Count'] = len(slugs) if slugs else 0
                
                # 5. 'List of Charts'
                displays = field_display_names.get(field_key, set())
                if displays:
                    row['List of Charts'] = ", ".join(sorted(displays))
                else:
                    row['List of Charts'] = "None"
                
                row['Dependency Level'] = dependency_levels.get(field_key, 0)
                
                writer.writerow(row)
                
        print(f"Successfully processed files.")
        print(f"Total objects processed: {len(all_sl_fields)}")
        print(f"Output written to {output_path}")
        
    except Exception as e:
        print(f"An unexpected error occurred while writing: {e}")
        return

if __name__ == '__main__':
    main()
