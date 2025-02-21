import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Load environment variables from .env file
load_dotenv()

# ✅ Initialize Firebase
cred = credentials.Certificate("popsizedb-firebase-adminsdk-wwbwa-7982b6bf06.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# ✅ Neo4j Connection
URI = os.getenv("NEO4J_URI")
AUTH_USER = os.getenv("NEO4J_USERNAME")
AUTH_PASS = os.getenv("NEO4J_PASSWORD")
AUTH = (AUTH_USER, AUTH_PASS)

# ✅ Initialize the Neo4j driver
driver = GraphDatabase.driver(URI, auth=AUTH)


def create_product_master(tx, product_master):
    """
    Creates a ProductMaster node in Neo4j.
    """
    query = """
        CREATE (pm:ProductMaster {
            product_id: $product_id,
            product_images: $product_images,
            aggregateRating: $aggregateRating,
            product_current_price: $product_current_price,
            product_release_date: $product_release_date,
            product_sku: $product_sku,
            currency_value: $currency_value,
            product_original_price: $product_original_price,
            type_label_long: $type_label_long,
            brand_label: $brand_label,
            size_chart_master_id: $size_chart_master_id
        })
    """
    tx.run(query, **product_master)


def create_product_translation(tx, product_translation):
    """
    Creates a ProductTranslation node in Neo4j and links it to ProductMaster.
    """
    query = """
        MATCH (pm:ProductMaster {product_id: $product_id})
        CREATE (pt:ProductTranslation {
            product_label: $product_label,
            product_url: $product_url,
            product_material: $product_material,
            product_features: $product_features,
            product_description: $product_description
        })
        MERGE (pm)-[:HAS_TRANSLATION]->(pt)
    """
    tx.run(query, **product_translation)


if __name__ == "__main__":
    # ✅ Fetch all products from Firestore
    products_ref = db.collection("products")
    documents = products_ref.stream()  # Retrieve all documents

    with driver.session() as session:
        for doc in documents:
            data = doc.to_dict()

            # ✅ Check if 'popsize_category' exists before processing
            if "popsize_category" in data:
                print(f"📌 Processing document: {doc.id}")

                # ✅ Prepare ProductMaster data
                product_master_data = {
                    "product_id": doc.id,  # Firestore document ID
                    "product_images": [img["url"] for img in data.get("images", []) if "url" in img],
                    "aggregateRating": json.dumps(data.get("aggregateRating", {})),  # Store as JSON string
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
                    "product_id": doc.id,  # Required to link with ProductMaster
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

                print(f"✅ Successfully added ProductMaster and ProductTranslation for {doc.id} to Neo4j!")

            else:
                print(f"❌ Skipped document {doc.id}: Missing 'popsize_category' field.")

    # ✅ Close the Neo4j driver properly
    driver.close()
