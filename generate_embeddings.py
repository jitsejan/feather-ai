import dlt
from sentence_transformers import SentenceTransformer
import numpy as np

PAGES_TABLE = "confluence_data.process_pages"
EMBEDDINGS_TABLE = "confluence_data.page_embeddings"


def generate_embeddings(conn, model):
    # Get all pages
    pages = conn.execute(f"SELECT id, title, content FROM {PAGES_TABLE}").fetchall()
    embeddings = []
    for page_id, title, content in pages:
        text = f"{title}\n{content}"
        embedding = model.encode(text)
        embeddings.append((page_id, embedding.tolist()))
    return embeddings

def store_embeddings(conn, embeddings):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {EMBEDDINGS_TABLE} (
        page_id VARCHAR REFERENCES {PAGES_TABLE}(id),
        embedding FLOAT[]
    )
    """)
    for page_id, emb in embeddings:
        conn.execute(f"INSERT INTO {EMBEDDINGS_TABLE} VALUES (?, ?)", (page_id, emb))

if __name__ == "__main__":
    import duckdb
    model = SentenceTransformer(dlt.config["ai.embedding_model"])
    conn = duckdb.connect(dlt.secrets["destination.duckdb.credentials"])
    embeddings = generate_embeddings(conn, model)
    store_embeddings(conn, embeddings)
    conn.close()
    print("Embeddings generated and stored.")
