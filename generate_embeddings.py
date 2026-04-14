import dlt
from sentence_transformers import SentenceTransformer

CHUNKS_TABLE = "analytics.fct_confluence_chunks"
EMBEDDINGS_TABLE = "analytics.chunk_embeddings"


def generate_embeddings(conn, model):
    chunks = conn.execute(
        f"SELECT chunk_id, title, chunk_text FROM {CHUNKS_TABLE}"
    ).fetchall()
    embeddings = []
    for chunk_id, title, chunk_text in chunks:
        text = f"{title}\n{chunk_text}"
        embedding = model.encode(text)
        embeddings.append((chunk_id, embedding.tolist()))
    return embeddings

def store_embeddings(conn, embeddings):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {EMBEDDINGS_TABLE} (
        chunk_id VARCHAR REFERENCES {CHUNKS_TABLE}(chunk_id),
        embedding FLOAT[]
    )
    """)
    conn.execute(f"DELETE FROM {EMBEDDINGS_TABLE}")
    for chunk_id, emb in embeddings:
        conn.execute(f"INSERT INTO {EMBEDDINGS_TABLE} VALUES (?, ?)", (chunk_id, emb))

if __name__ == "__main__":
    import duckdb
    model = SentenceTransformer(dlt.config["ai.embedding_model"])
    conn = duckdb.connect(dlt.secrets["destination.duckdb.credentials"])
    embeddings = generate_embeddings(conn, model)
    store_embeddings(conn, embeddings)
    conn.close()
    print("Embeddings generated and stored.")
