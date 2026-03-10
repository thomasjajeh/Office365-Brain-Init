import os
import requests
from dotenv import load_dotenv

load_dotenv()

SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
SEARCH_KEY = os.getenv("SEARCH_KEY")
SEARCH_INDEX = "agent-brain"
OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
EMBED_MODEL = "embedding-model"

headers = {
    "Content-Type": "application/json",
    "api-key": SEARCH_KEY
}

# Step 1: Delete existing index
print("Deleting existing index...")
r = requests.delete(
    f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX}?api-version=2024-07-01",
    headers=headers
)
print(f"  Delete: {r.status_code} ({r.reason})")

# Step 2: Create index with integrated vectorization
print("Creating index with integrated vectorization...")

index_definition = {
    "name": SEARCH_INDEX,
    "fields": [
        {"name": "id", "type": "Edm.String", "key": True, "filterable": True},
        {"name": "source", "type": "Edm.String", "filterable": True},
        {"name": "source_type", "type": "Edm.String", "filterable": True},
        {"name": "conversation_id", "type": "Edm.String", "filterable": True},
        {"name": "subject", "type": "Edm.String", "searchable": True},
        {"name": "sender", "type": "Edm.String", "filterable": True, "searchable": True},
        {"name": "date", "type": "Edm.String", "filterable": True, "sortable": True},
        {"name": "content_chunk", "type": "Edm.String", "searchable": True},
        {
            "name": "content_vector",
            "type": "Collection(Edm.Single)",
            "searchable": True,
            "dimensions": 1536,
            "vectorSearchProfile": "default-vector-profile"
        }
    ],
    "vectorSearch": {
        "algorithms": [
            {
                "name": "default-hnsw",
                "kind": "hnsw",
                "hnswParameters": {
                    "metric": "cosine",
                    "m": 4,
                    "efConstruction": 400,
                    "efSearch": 500
                }
            }
        ],
        "vectorizers": [
            {
                "name": "default-openai-vectorizer",
                "kind": "azureOpenAI",
                "azureOpenAIParameters": {
                    "resourceUri": OPENAI_ENDPOINT,
                    "deploymentId": EMBED_MODEL,
                    "apiKey": OPENAI_KEY,
                    "modelName": "text-embedding-3-small"
                }
            }
        ],
        "profiles": [
            {
                "name": "default-vector-profile",
                "algorithm": "default-hnsw",
                "vectorizer": "default-openai-vectorizer"
            }
        ]
    }
}

r = requests.put(
    f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX}?api-version=2024-07-01",
    headers=headers,
    json=index_definition
)

if r.status_code in [200, 201]:
    print("Index created successfully with integrated vectorization!")
else:
    print(f"Error creating index: {r.status_code}")
    print(r.text)
