def get_sources(cli_output):
    sources = []

    for line in cli_output:
        if line.startswith('source:'):
            sources.append(line[7:])
    
    return sources

def schema_exists(schema_name, source_list):
    
    for source in source_list:
        if schema_name in source.keys():
            return True
        
    return False

def get_schema(sources, project, schema_name):
    
    for element in sources[project]:
        if schema_name in element:
            return element[schema_name]
    

def profile_sources(projects_arr):
    projects = {}
    for source in projects_arr:
        [project, schema, table] = source.split('.')

        if project not in projects:
            projects[project] = [{schema: [table]}]
        elif not schema_exists(schema, projects[project]):
            projects[project].append({schema: [table]})
        elif schema_exists(schema, projects[project]):
            tables = get_schema(projects, project, schema)
            if table not in tables:
                tables.append(table)
    return projects