import os
import time
import requests
import msal
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# ---------------------------
# CONFIG
# ---------------------------

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]

OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
EMBED_MODEL = "embedding-model"

SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
SEARCH_KEY = os.getenv("SEARCH_KEY")
SEARCH_INDEX = "agent-brain"

TARGET_MAILBOX = os.getenv("TARGET_MAILBOX")

BATCH_SIZE = 100
PAGE_DELAY = 2  # seconds to wait between Graph API pages

# ---------------------------
# AUTHENTICATE GRAPH
# ---------------------------

def get_graph_token():

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )

    token = app.acquire_token_for_client(scopes=GRAPH_SCOPE)

    if "access_token" not in token:
        raise Exception(f"Graph auth failed: {token}")

    return token["access_token"]


# ---------------------------
# CLEAN EMAIL HTML
# ---------------------------

def clean_html(html):

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")

    return text


# ---------------------------
# CHUNK TEXT
# ---------------------------

def chunk_text(text, chunk_size=300):

    words = text.split()
    chunks = []

    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)

    return chunks


# ---------------------------
# GENERATE EMBEDDING
# ---------------------------

def create_embedding(text):

    # Truncate to ~6000 words to stay under 8192 token limit
    words = text.split()
    if len(words) > 6000:
        text = " ".join(words[:6000])

    url = f"{OPENAI_ENDPOINT}/openai/deployments/{EMBED_MODEL}/embeddings?api-version=2024-02-01"

    headers = {
        "api-key": OPENAI_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "input": text
    }

    for attempt in range(5):
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            return response.json()["data"][0]["embedding"]

        if response.status_code == 429:
            wait = 15 * (attempt + 1)
            print(f"Rate limited, waiting {wait}s...")
            time.sleep(wait)
        else:
            raise Exception(response.text)

    raise Exception("Max retries exceeded for embedding request")


# ---------------------------
# FETCH EMAILS FROM GRAPH
# ---------------------------

def get_email_pages(token, folder="inbox"):
    """Yields one page of emails at a time from Graph API."""

    url = f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/mailFolders/{folder}/messages?$top=50&$select=id,subject,body,from,receivedDateTime,conversationId"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    page_num = 0

    while url:

        r = requests.get(url, headers=headers).json()

        page = r.get("value", [])
        page_num += 1

        if page:
            yield page

        url = r.get("@odata.nextLink")

        if url:
            time.sleep(PAGE_DELAY)


# ---------------------------
# PREPARE DOCUMENTS
# ---------------------------

def build_documents(email):

    body = clean_html(email["body"]["content"])

    chunks = chunk_text(body)

    docs = []

    for i, chunk in enumerate(chunks):

        embedding = create_embedding(chunk)

        doc = {
            "id": f"{email['id']}_{i}",
            "source": "mailbox",
            "source_type": "email",
            "conversation_id": email.get("conversationId", ""),
            "subject": email.get("subject", ""),
            "sender": email.get("from", {}).get("emailAddress", {}).get("address", ""),
            "date": email.get("receivedDateTime", ""),
            "content_chunk": chunk,
            "content_vector": embedding
        }

        docs.append(doc)

    return docs


# ---------------------------
# UPLOAD TO AZURE SEARCH
# ---------------------------

def upload_batch(documents):

    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX}/docs/index?api-version=2024-07-01"

    headers = {
        "Content-Type": "application/json",
        "api-key": SEARCH_KEY
    }

    payload = {
        "value": [
            {"@search.action": "upload", **doc} for doc in documents
        ]
    }

    r = requests.post(url, headers=headers, json=payload)

    if r.status_code not in [200, 201]:
        print("Upload error:", r.text)


# ---------------------------
# MAIN PIPELINE
# ---------------------------

def run():

    print("Authenticating to Graph...")
    token = get_graph_token()

    all_docs = []
    total_emails = 0

    for folder in ["inbox", "archive"]:
        print(f"\n--- Processing {folder} ---")
        page_num = 0

        for page in get_email_pages(token, folder=folder):
            page_num += 1
            print(f"\nPage {page_num} ({len(page)} emails)")

            for email in page:
                total_emails += 1
                print(f"  [{total_emails}] {email.get('subject', '(no subject)')}")

                docs = build_documents(email)
                all_docs.extend(docs)

                if len(all_docs) >= BATCH_SIZE:
                    upload_batch(all_docs)
                    print(f"  Uploaded {len(all_docs)} chunks")
                    all_docs = []

    if all_docs:
        upload_batch(all_docs)
        print(f"Uploaded final {len(all_docs)} chunks")

    print(f"\nFinished ingestion! Total emails processed: {total_emails}")


if __name__ == "__main__":
    run()
