from rich.console import Console
from rich.progress import Progress
import mariadb
import json
import os

console = Console()

def fetch_unique_values(cursor, table_name, column_name, row_limit=None):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    if row_limit:
        query += f" LIMIT {row_limit}"
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}

def fetch_column_details(cursor, table_name):
    query = f"SHOW COLUMNS FROM {table_name}"
    cursor.execute(query)
    return [(row[0], row[1]) for row in cursor.fetchall()]

def fetch_table_names(cursor):
    cursor.execute("SHOW TABLES")
    return [row[0] for row in cursor.fetchall()]

def identify_soft_foreign_keys_in_db(db_config, threshold_percent=0.1, row_limit=None, progress_file='progress.json'):
    global_matches = {}
    progress_data = {}

    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress_data = json.load(f)

    progress_key = f"{db_config['host']}_{db_config['database']}"
    if progress_key not in progress_data:
        progress_data[progress_key] = {}

    try:
        conn = mariadb.connect(**db_config)
        cursor = conn.cursor()
        table_names = fetch_table_names(cursor)
    except mariadb.Error as err:
        console.print(f"[red]Something went wrong with the database connection: {err}[/red]")
        return

    with Progress() as progress:
        task = progress.add_task("[cyan]Processing tables...", total=len(table_names))

        for table1 in table_names:
            if table1 in progress_data[progress_key]:
                console.print(f"[yellow]Skipping {table1}, already done.[/yellow]")
                progress.update(task, advance=1)
                continue

            console.print(f"[green]Processing table {table1}...[/green]")
            table1_columns = fetch_column_details(cursor, table1)
            table1_matches = {}

            for (col1, type1) in table1_columns:
                unique_col1_values = fetch_unique_values(cursor, table1, col1, None)  # No row limit for col1
                
                for table2 in table_names:
                    if table1 == table2:
                        continue

                    table2_columns = fetch_column_details(cursor, table2)
                    potential_foreign_keys = []

                    for (col2, type2) in table2_columns:
                        if type1 == type2:
                            unique_col2_values = fetch_unique_values(cursor, table2, col2, row_limit)  # row limit for col2
                            common_values = unique_col1_values.intersection(unique_col2_values)
                            percent_match = len(common_values) / (len(unique_col1_values) + 1e-9)  # Add a small number to avoid division by zero

                            if percent_match >= threshold_percent:
                                potential_foreign_keys.append(col2)
                
                    if potential_foreign_keys:
                        table1_matches[table2] = potential_foreign_keys
            
            if table1_matches:
                global_matches[table1] = table1_matches
            
            # Mark the table as done
            progress_data[progress_key][table1] = True

            # Save progress
            with open(progress_file, 'w') as f:
                json.dump(progress_data, f)

            progress.update(task, advance=1)
        
    conn.close()
    return global_matches

def staged_identification(db_config, stages=[10, 50, None], threshold_percent=0.1, progress_file='progress.json'):
    overall_matches = {}

    for stage in stages:
        console.print(f"[blue]Running stage with row_limit = {stage}[/blue]")
        matches = identify_soft_foreign_keys_in_db(db_config, threshold_percent=threshold_percent, row_limit=stage, progress_file=progress_file)
        overall_matches.update(matches)

    return overall_matches
