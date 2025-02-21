import json

from utils import load_db_connections

db, driver = load_db_connections()
def create_size_chart(tx, size_chart):
    """
    Creates a SizeChart node with dynamic properties.
    """
    property_keys = ", ".join([f"{key}: ${key}" for key in size_chart.keys()])
    query = f"""
        CREATE (sc:SizeChart {{
            {property_keys}
        }})
    """
    tx.run(query, **size_chart)


if __name__ == "__main__":
    # ✅ Fetch all size chart documents from Firestore
    sizecharts_ref = db.collection("sizecharts")
    documents = sizecharts_ref.stream()  # Retrieve all documents

    with driver.session() as session:
        for doc in documents:
            data = doc.to_dict()
            document_id = doc.id  # Firestore document ID

            print(f"📌 Debugging Document: {document_id}")

            # ✅ Ensure we access `sizechart` correctly
            sizechart_data = data.get("json", {}).get("sizechart", {})

            # 🔹 Debugging: Print extracted `sizechart_data`
            print(f"📊 Extracted SizeChart Data: {json.dumps(sizechart_data, indent=2)}")

            if "column_header" not in sizechart_data or not isinstance(sizechart_data["column_header"], list):
                print(f"⚠️ Skipping {document_id}: Missing or incorrect 'column_header' field")
                continue

            if "rows" not in sizechart_data or not isinstance(sizechart_data["rows"], list):
                print(f"⚠️ Skipping {document_id}: Missing or incorrect 'rows' field")
                continue

            column_headers = sizechart_data["column_header"]
            rows = sizechart_data["rows"]

            print(f"📌 Column Headers: {column_headers}")
            print(f"📌 Rows: {rows}")

            # ✅ Extract measurement labels dynamically
            measurement_labels = [header.get("measure", "").replace(" ", "_") for header in column_headers]
            print(f"📌 Extracted Measurement Labels: {measurement_labels}")

            for row in rows:
                size_label = row.get("row_header", "").strip()  # e.g., "XS", "S", etc.
                values = row.get("values", [])

                if not isinstance(values, list) or not values:
                    print(f"⚠️ Skipping {size_label}: No valid values found in document {document_id}")
                    continue

                # ✅ Prepare a dynamic dictionary for SizeChart node
                size_chart_data = {
                    "size_chart_master_id": document_id,
                    "size_chart_unique_id": f"{document_id}_{size_label}",  # Unique ID
                    "size_label": size_label,
                }

                # ✅ Dynamically assign each measurement
                for i, measure_label in enumerate(measurement_labels):
                    if i < len(values):  # Ensure index exists in values list
                        size_chart_data[measure_label] = float(values[i])  # Store as float

                # ✅ Ensure valid data before inserting into Neo4j
                if len(size_chart_data) > 2:  # Must contain more than ID & size_label
                    session.execute_write(create_size_chart, size_chart_data)
                    print(f"✅ Added SizeChart for {size_label} in document {document_id}")

                else:
                    print(f"⚠️ Skipped {size_label}: No valid measurements in document {document_id}")

    # ✅ Close Neo4j driver
    driver.close()
