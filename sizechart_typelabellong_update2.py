# This script adds priority relationships between type_label_long Type node and SizeCharts

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from utils import load_db_connections

_, driver = load_db_connections()

# CSV File path
file_path = "sizechart_typelabellong_update4.csv"  # Update this to your CSV file path


def create_relationships(tx, type_label_long, type_label_short, priority):
    query = """
    // Match the Type node based on type_label_long
    MATCH (t:Type {type_label_long: $type_label_long})
    // Find all SizeChart nodes that match the type_label_short
    MATCH (sc:SizeChart {type_label_short: $type_label_short})
    // Create a relationship with priority
    MERGE (t)-[r:HAS_SIZECHART]->(sc)
    ON CREATE SET r.priority = $priority
    ON MATCH SET r.priority = $priority
    RETURN t.type_label_long, sc.type_label_short, r.priority
    """
    results = tx.run(
        query,
        type_label_long=type_label_long,
        type_label_short=type_label_short,
        priority=priority,
    )
    return [result for result in results]


def load_data(file_path):
    """
    Load data from CSV, update or create nodes and relationships in Neo4j.
    """
    try:
        # Read CSV using pandas
        df = pd.read_csv(file_path)

        # Connect to Neo4j and start a session
        with driver.session() as session:
            # Iterate over dataframe rows
            for index, row in df.iterrows():
                type_label_long = row["type_label_long"]
                type_label_short = row["type_label_short"]
                priority = row["priority"]

                # Create relationships in the Neo4j database
                results = session.execute_write(create_relationships, type_label_long, type_label_short, priority)
                for result in results:
                    print(
                        f"Relationship created: Type({result[0]}) -> SizeChart({result[1]}) with priority {result[2]}"
                    )
    except ServiceUnavailable as e:
        print(f"ServiceUnavailable error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Close the driver connection when done
def close():
    driver.close()


if __name__ == "__main__":
    load_data(file_path)
    close()
