import dlt
from sentence_transformers import SentenceTransformer
import numpy as np

def generate_embeddings(conn, model):
    # Get all pages
    pages = conn.execute("SELECT id, title, content FROM pages").fetchall()
    embeddings = []
    for page_id, title, content in pages:
        text = f"{title}\n{content}"
        embedding = model.encode(text)
        embeddings.append((page_id, embedding.tolist()))
    return embeddings

def store_embeddings(conn, embeddings):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS page_embeddings (
        page_id VARCHAR REFERENCES pages(id),
        embedding FLOAT[]
    )
    """)
    for page_id, emb in embeddings:
        conn.execute("INSERT INTO page_embeddings VALUES (?, ?)", (page_id, emb))

if __name__ == "__main__":
    import duckdb
    model = SentenceTransformer(dlt.config["ai.embedding_model"])
    conn = duckdb.connect(dlt.secrets["destination.duckdb.credentials"])
    embeddings = generate_embeddings(conn, model)
    store_embeddings(conn, embeddings)
    conn.close()
    print("Embeddings generated and stored.")