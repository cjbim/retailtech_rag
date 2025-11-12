from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from qdrant_utils import keyword_then_semantic_rerank
from vllm_utils import (
    call_vllm_generate_search_condition,
    clean_llm_keywords,
    call_vllm_summarize_article
)
import json
from datetime import datetime
from pathlib import Path
import os

# ─────────────────────────────────────────────
# ✅ FastAPI 기본 설정
# ─────────────────────────────────────────────
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ─────────────────────────────────────────────
# ✅ 로그 디렉토리 및 파일 설정
# ─────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "app_log.jsonl"

def log_to_file(entry: dict):
    """로그 데이터를 JSONL 형식으로 저장"""
    entry["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ─────────────────────────────────────────────
# ✅ 홈 페이지
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ─────────────────────────────────────────────
# ✅ 문서 검색 API (RetailTech 형식)
# ─────────────────────────────────────────────
@app.post("/search/documents")
async def document_search(request: Request):
    data = await request.json()
    user_question = data.get("question")

    if not user_question:
        return {"error": "❌ 질문이 없습니다."}

    print(f"\n📥 사용자 질문: {user_question}")

    # ✅ 1단계: LLM 키워드 생성
    raw_keywords = call_vllm_generate_search_condition(user_question)
    print(f"🔍 LLM 생성 키워드 (원본): {raw_keywords}")

    keywords = clean_llm_keywords(raw_keywords)
    print(f"✅ 정제된 키워드 리스트: {keywords}")

    # ✅ 2단계: Qdrant 검색 수행
    document_list = keyword_then_semantic_rerank(user_question, keywords, top_k=30)
    print(f"\n📄 검색 결과 개수: {len(document_list)}")

    # ✅ 3단계: RetailTech 형식으로 정리
    formatted_documents = []
    for doc in document_list:
        formatted_documents.append({
            "record_id": doc.get("record_id", ""),
            "store_name": doc.get("store_name", ""),
            "store_code": doc.get("store_code", ""),
            "date": doc.get("date", ""),
            "title": doc.get("title", ""),
            "text": doc.get("text", ""),
            "fault_major": doc.get("fault_major", ""),
            "fault_mid": doc.get("fault_mid", ""),
            "fault_minor": doc.get("fault_minor", ""),
            "urgency": doc.get("urgency", ""),
            "department_main": doc.get("department_main", ""),
            "progress": doc.get("progress", ""),
            "ocs_cause_major": doc.get("ocs_cause_major", ""),
            "ocs_cause_mid": doc.get("ocs_cause_mid", ""),
            "ocs_cause_minor": doc.get("ocs_cause_minor", ""),
            "keywords": doc.get("keywords", ""),
            "score": round(doc.get("score", 0.0), 5),
            "accuracy": f"{round(doc.get('score', 0.0) * 100, 2)}%"
        })

    # ✅ 로그 기록 (질문 + 키워드 + 검색 결과)
    log_to_file({
        "event": "search",
        "question": user_question,
        "llm_keywords": keywords,
        "result_count": len(formatted_documents),
        "top3_preview": formatted_documents[:3]
    })

    return {
        "result_count": len(formatted_documents),
        "documents": formatted_documents
    }


# ─────────────────────────────────────────────
# ✅ 요약 API (스토리로그 포함)
# ─────────────────────────────────────────────
@app.post("/summarize")
async def summarize_article(request: Request):
    data = await request.json()

    print(f"\n🧠 요약 요청 수신")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    if not data.get("content"):
        return {"error": "❌ 요약할 본문이 없습니다."}

    summary = call_vllm_summarize_article(data)

    # ✅ 요약 결과 로그 저장
    log_to_file({
        "event": "summarize",
        "store_name": data.get("store_name"),
        "date": data.get("date"),
        "fault_major": data.get("fault_major"),
        "ocs_cause_major": data.get("ocs_cause_major"),
        "urgency": data.get("urgency"),
        "input_excerpt": data.get("content")[:200],
        "summary": summary
    })

    return {"summary": summary}
