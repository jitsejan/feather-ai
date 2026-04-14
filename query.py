import dlt
import duckdb
import numpy as np
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

CHUNKS_TABLE = "analytics.fct_confluence_chunks"
EMBEDDINGS_TABLE = "analytics.chunk_embeddings"


def load_embeddings(conn):
    embeddings = conn.execute(f"SELECT chunk_id, embedding FROM {EMBEDDINGS_TABLE}").fetchall()
    return {chunk_id: np.array(emb) for chunk_id, emb in embeddings}

def load_chunks(conn):
    chunks = conn.execute(f"SELECT chunk_id, title, chunk_text FROM {CHUNKS_TABLE}").fetchall()
    return {chunk_id: (title, chunk_text) for chunk_id, title, chunk_text in chunks}

def find_relevant_chunks(query_embedding, embeddings, top_k=5):
    similarities = {}
    for chunk_id, emb in embeddings.items():
        sim = np.dot(query_embedding, emb) / (np.linalg.norm(query_embedding) * np.linalg.norm(emb))
        similarities[chunk_id] = sim
    sorted_chunks = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    return sorted_chunks[:top_k]

def generate_answer(question, context, tokenizer, model):
    prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024)
    with torch.no_grad():
        outputs = model.generate(**inputs, max_length=512, num_return_sequences=1)
    answer = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return answer.split("Answer:")[-1].strip()

def query(question):
    model_emb = SentenceTransformer(dlt.config["ai.embedding_model"])
    query_emb = model_emb.encode(question)

    conn = duckdb.connect(dlt.secrets["destination.duckdb.credentials"])
    embeddings = load_embeddings(conn)
    chunks = load_chunks(conn)
    conn.close()

    relevant = find_relevant_chunks(query_emb, embeddings)
    context = "\n\n".join([f"Title: {chunks[chunk_id][0]}\nContent: {chunks[chunk_id][1]}" for chunk_id, _ in relevant])

    tokenizer = AutoTokenizer.from_pretrained(dlt.config["ai.generation_model"])
    model_gen = AutoModelForCausalLM.from_pretrained(dlt.config["ai.generation_model"], torch_dtype=torch.float16, device_map="auto")
    answer = generate_answer(question, context, tokenizer, model_gen)

    return answer

if __name__ == "__main__":
    question = input("Enter your question: ")
    answer = query(question)
    print("Answer:", answer)
