import dlt
import duckdb
import numpy as np
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch

PAGES_TABLE = "confluence_data.process_pages"
EMBEDDINGS_TABLE = "confluence_data.page_embeddings"


def load_embeddings(conn):
    embeddings = conn.execute(f"SELECT page_id, embedding FROM {EMBEDDINGS_TABLE}").fetchall()
    return {pid: np.array(emb) for pid, emb in embeddings}

def load_pages(conn):
    pages = conn.execute(f"SELECT id, title, content FROM {PAGES_TABLE}").fetchall()
    return {pid: (title, content) for pid, title, content in pages}

def find_relevant_pages(query_embedding, embeddings, top_k=5):
    similarities = {}
    for pid, emb in embeddings.items():
        sim = np.dot(query_embedding, emb) / (np.linalg.norm(query_embedding) * np.linalg.norm(emb))
        similarities[pid] = sim
    sorted_pages = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    return sorted_pages[:top_k]

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
    pages = load_pages(conn)
    conn.close()

    relevant = find_relevant_pages(query_emb, embeddings)
    context = "\n\n".join([f"Title: {pages[pid][0]}\nContent: {pages[pid][1]}" for pid, _ in relevant])

    tokenizer = AutoTokenizer.from_pretrained(dlt.config["ai.generation_model"])
    model_gen = AutoModelForCausalLM.from_pretrained(dlt.config["ai.generation_model"], torch_dtype=torch.float16, device_map="auto")
    answer = generate_answer(question, context, tokenizer, model_gen)

    return answer

if __name__ == "__main__":
    question = input("Enter your question: ")
    answer = query(question)
    print("Answer:", answer)
