import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import firebase_admin
from firebase_admin import credentials, firestore

def load_db_connections():
    # Load environment variables from .env file
    load_dotenv()
    script_dir = os.path.dirname(os.path.abspath(__file__))


    # ✅ Initialize Firebase
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.path.join(script_dir, "popsizedb-firebase-adminsdk-wwbwa-7982b6bf06.json"))
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    # ✅ Neo4j Connection
    URI = os.getenv("NEO4J_URI")
    AUTH_USER = os.getenv("NEO4J_USERNAME")
    AUTH_PASS = os.getenv("NEO4J_PASSWORD")
    AUTH = (AUTH_USER, AUTH_PASS)

    # ✅ Initialize Neo4j driver
    driver = GraphDatabase.driver(URI, auth=AUTH)

    return db, driver