import json
from .FirebaseToNeo_products import create_product_master, create_product_translation
from .FirebaseToNeo_sizecharts import create_size_chart
from .utils import load_db_connections

db, driver = load_db_connections()


def create_size(tx, product_id, size_label):
    """
    Creates a Size node and links it to the ProductMaster.
    """
    query = """
        MATCH (pm:ProductMaster {size_chart_master_id: $product_id})
        MERGE (s:Size {size_label: $size_label})
        MERGE (pm)-[:HAS_SIZE]->(s)
    """
    tx.run(query, product_id=product_id, size_label=size_label)


def link_size_to_size_chart(tx, size_label, size_chart_id):
    """
    Links a Size node to a SizeChart node.
    """
    query = """
        MATCH (s:Size {size_label: $size_label})
        MATCH (sc:SizeChart {size_chart_unique_id: $size_chart_id})
        MERGE (s)-[:HAS_SIZECHART]->(sc)
    """
    tx.run(query, size_label=size_label, size_chart_id=size_chart_id)


if __name__ == "__main__":
    # ✅ Fetch all products from Firestore
    products_ref = db.collection("products")
    documents = products_ref.stream()

    with driver.session() as session:
        for doc in documents:
            data = doc.to_dict()

            if "popsize_category" in data:
                print(f"📌 Processing document: {doc.id}")

                # ✅ Prepare ProductMaster data
                product_master_data = {
                    "product_id": doc.id,
                    "product_images": [img["url"] for img in data.get("images", []) if "url" in img],
                    "aggregateRating": json.dumps(data.get("aggregateRating", {})),
                    "product_current_price": data.get("price", None),
                    "product_release_date": data.get("metadata", {}).get("dateDownloaded", ""),
                    "product_sku": data.get("sku", ""),
                    "currency_value": data.get("currency", ""),
                    "product_original_price": data.get("regularPrice", None),
                    "type_label_long": data.get("popsize_category", None),
                    "brand_label": data.get("brand", {}).get("name", None),
                    "size_chart_master_id": data.get("sizechart", None),
                }

                # ✅ Prepare ProductTranslation data
                product_translation_data = {
                    "product_id": doc.id,
                    "product_label": data.get("name", "Unknown"),
                    "product_url": data.get("canonicalUrl", ""),
                    "product_material": data.get("material", ""),
                    "product_features": data.get("features", []),
                    "product_description": data.get("description", ""),
                }

                # ✅ Insert ProductMaster node
                session.execute_write(create_product_master, product_master_data)

                # ✅ Insert ProductTranslation node and link to ProductMaster
                session.execute_write(create_product_translation, product_translation_data)

                print(f"✅ Added ProductMaster & ProductTranslation for {doc.id} to Neo4j!")

    # ✅ Fetch all size charts from Firestore
    sizecharts_ref = db.collection("sizecharts")
    documents = sizecharts_ref.stream()

    with driver.session() as session:
        for doc in documents:
            data = doc.to_dict()
            document_id = doc.id  # Firestore document ID

            sizechart_data = data.get("json", {}).get("sizechart", {})

            if "column_header" not in sizechart_data or not isinstance(sizechart_data["column_header"], list):
                print(f"⚠️ Skipping {document_id}: Missing or incorrect 'column_header' field")
                continue

            if "rows" not in sizechart_data or not isinstance(sizechart_data["rows"], list):
                print(f"⚠️ Skipping {document_id}: Missing or incorrect 'rows' field")
                continue

            column_headers = sizechart_data["column_header"]
            rows = sizechart_data["rows"]

            # ✅ Extract measurement labels dynamically
            measurement_labels = [header.get("measure", "").replace(" ", "_") for header in column_headers]

            for row in rows:
                size_label = row.get("row_header", "").strip()
                values = row.get("values", [])

                if not isinstance(values, list) or not values:
                    print(f"⚠️ Skipping {size_label}: No valid values found in document {document_id}")
                    continue

                # ✅ Prepare SizeChart node
                size_chart_data = {
                    "size_chart_master_id": document_id,
                    "size_chart_unique_id": f"{document_id}_{size_label}",
                    "size_label": size_label,
                    "brand_label": data.get("brand_label", ""),  # From ProductMaster
                    "type_label_long": data.get("type_label_long", ""),  # From ProductMaster
                }

                for i, measure_label in enumerate(measurement_labels):
                    if i < len(values):
                        size_chart_data[measure_label] = float(values[i])

                # ✅ Create Size node and link to ProductMaster
                session.execute_write(create_size, document_id, size_label)

                # ✅ Create SizeChart node and link to Size
                session.execute_write(create_size_chart, size_chart_data)
                session.execute_write(
                    link_size_to_size_chart,
                    size_label,
                    size_chart_data["size_chart_unique_id"],
                )

                print(f"✅ Added SizeChart for {size_label} in document {document_id}")

    # ✅ Close Neo4j driver
    driver.close()
