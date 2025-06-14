import os
import json
import re
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

DISCOURSE_DIR = "downloaded_threads"
DB_PATH = "knowledge_base.db"
MARKDOWN_SOURCE_NAME = "DiscourseCopy"

def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return re.sub(r'\s+', ' ', soup.get_text(separator=' ')).strip()

def create_chunks(text, chunk_size=1000, overlap=200):
    text = re.sub(r'\s+', ' ', text.strip())
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    for i in range(0, len(text), chunk_size - overlap):
        chunk = text[i:i + chunk_size]
        chunks.append(chunk.strip())
    return chunks

def insert_into_markdown_chunks(conn, title, url, content_chunks):
    cursor = conn.cursor()
    for i, chunk in enumerate(content_chunks):
        cursor.execute("""
            INSERT INTO markdown_chunks (doc_title, original_url, downloaded_at, chunk_index, content, embedding)
            VALUES (?, ?, ?, ?, ?, NULL)
        """, (
            title,
            url,
            datetime.utcnow().isoformat(),
            i,
            chunk
        ))
    conn.commit()

def process_all_files():
    conn = sqlite3.connect(DB_PATH)
    os.makedirs(DISCOURSE_DIR, exist_ok=True)
    files = [f for f in os.listdir(DISCOURSE_DIR) if f.endswith('.json')]
    
    for file in tqdm(files, desc="Copying Discourse to Markdown"):
        path = os.path.join(DISCOURSE_DIR, file)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        topic_title = data.get("title", "Untitled Topic")
        topic_id = data.get("id")
        topic_slug = data.get("slug")
        url = f"https://discourse.onlinedegree.iitm.ac.in/t/{topic_slug}/{topic_id}"
        
        all_text = ""
        posts = data.get("post_stream", {}).get("posts", [])
        for post in posts:
            content = post.get("cooked", "")
            clean = clean_html(content)
            if clean:
                all_text += clean + "\n\n"
        
        chunks = create_chunks(all_text)
        if chunks:
            insert_into_markdown_chunks(conn, title=topic_title, url=url, content_chunks=chunks)
    
    conn.close()
    print("✅ Completed copying Discourse threads to markdown_chunks.")

if __name__ == "__main__":
    process_all_files()
