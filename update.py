import os
import time
from datetime import datetime, timedelta, timezone
import requests
import msal
from bs4 import BeautifulSoup
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

LOOKBACK_MINUTES = 5

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
    return soup.get_text(separator=" ")


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
    words = text.split()
    if len(words) > 6000:
        text = " ".join(words[:6000])

    url = f"{OPENAI_ENDPOINT}/openai/deployments/{EMBED_MODEL}/embeddings?api-version=2024-02-01"
    headers = {
        "api-key": OPENAI_KEY,
        "Content-Type": "application/json"
    }
    payload = {"input": text}

    for attempt in range(5):
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()["data"][0]["embedding"]
        if response.status_code == 429:
            wait = 15 * (attempt + 1)
            print(f"Rate limited, waiting {wait}s...")
            time.sleep(wait)
        elif "maximum context length" in response.text:
            words = text.split()
            text = " ".join(words[:len(words) // 2])
            payload["input"] = text
            print(f"Chunk too large, truncating to {len(words) // 2} words...")
        else:
            raise Exception(response.text)

    raise Exception("Max retries exceeded for embedding request")


# ---------------------------
# FETCH RECENT EMAILS
# ---------------------------

def get_recent_emails(token, folder="inbox"):
    """Fetch emails received in the last LOOKBACK_MINUTES minutes."""
    since = (datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filter_query = f"receivedDateTime ge {since}"

    url = (
        f"https://graph.microsoft.com/v1.0/users/{TARGET_MAILBOX}/mailFolders/{folder}/messages"
        f"?$top=50&$filter={filter_query}"
        f"&$select=id,subject,body,from,receivedDateTime,conversationId"
        f"&$orderby=receivedDateTime desc"
    )

    headers = {"Authorization": f"Bearer {token}"}
    emails = []

    while url:
        r = requests.get(url, headers=headers).json()
        emails.extend(r.get("value", []))
        url = r.get("@odata.nextLink")

    return emails


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
        "value": [{"@search.action": "upload", **doc} for doc in documents]
    }
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code not in [200, 201]:
        print("Upload error:", r.text)


# ---------------------------
# MAIN
# ---------------------------

def run():
    print(f"Fetching emails from the last {LOOKBACK_MINUTES} minutes...")
    token = get_graph_token()

    all_docs = []
    total = 0
    skipped = 0

    for folder in ["inbox", "archive"]:
        emails = get_recent_emails(token, folder=folder)
        print(f"  {folder}: {len(emails)} new emails")

        for email in emails:
            total += 1
            subject = email.get("subject", "(no subject)")
            try:
                docs = build_documents(email)
                all_docs.extend(docs)
            except Exception as e:
                skipped += 1
                print(f"  SKIPPED: {subject} — {e}")

    if all_docs:
        upload_batch(all_docs)
        print(f"Uploaded {len(all_docs)} chunks from {total} emails")
    else:
        print("No new emails to process.")

    if skipped:
        print(f"Skipped: {skipped}")


if __name__ == "__main__":
    run()
