from google.cloud import bigquery
import json

# Initialize a BigQuery client
client = bigquery.Client()

# Define a dictionary to store results
top_apps = {}

# List of SQL queries and corresponding marketplace names
queries = [
    ("AWS_MARKETPLACE", "SELECT * FROM `Marketplace_Dataset.AWS_MARKETPLACE` ORDER BY reviews DESC LIMIT 1;"),
    ("AZURE_MARKETPLACE", "SELECT * FROM `Marketplace_Dataset.AZURE_MARKETPLACE` ORDER BY reviews DESC LIMIT 1;"),
    ("FigmaPlugins", "SELECT * FROM `Marketplace_Dataset.FigmaPlugins` ORDER BY usage_number DESC LIMIT 1;"),
    ("GITHUB_TOPICS", "SELECT * FROM `Marketplace_Dataset.GITHUB_TOPICS` ORDER BY stars DESC LIMIT 1;"),
    ("HerokuSpider", "SELECT * FROM `Marketplace_Dataset.HerokuSpider` ORDER BY stars DESC LIMIT 1;"),
    ("VISUAL_STUDIO_MARKETPLACE", "SELECT * FROM `Marketplace_Dataset.VISUAL_STUDIO_MARKETPLACE` ORDER BY installs DESC LIMIT 1;")
    # Add more queries and marketplace names here...
]

# Loop through each query and execute it
for marketplace, query in queries:
    # Execute the query
    query_job = client.query(query)

    # Fetch the result
    result = query_job.result()

    # Access the result row
    row = next(result)

    # Store the result in the dictionary
    top_apps[marketplace] = {
        "app_name": row.app_name,
        "marketplace_name": marketplace
    }

# Convert the dictionary to JSON format
json_result = json.dumps({"top-apps": top_apps}, indent=4)


#Steps to load json into table for BigQuery
# Define the dataset and table information
dataset_id = 'your_project_id.your_dataset_id'
table_id = 'your_table_id'

#table = client.get_table(f"{dataset_id}.{table_id}")
#rows_to_insert = [(json_result,)]
#client.insert_rows(table, rows_to_insert)

# Print the JSON result
#print(json_result)
