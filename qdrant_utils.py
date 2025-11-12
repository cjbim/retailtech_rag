import gc
import torch
import re
from typing import List, Tuple, Dict, Set
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import MatchValue, MatchAny, Filter, FieldCondition
from sklearn.metrics.pairwise import cosine_similarity

# ─────────────────────────────────────────────
# ✅ Qdrant 설정
# ─────────────────────────────────────────────
qdrant_client = QdrantClient(host="localhost", port=6333)
collection_name = "retailtech_test"

# ─────────────────────────────────────────────
# ✅ SentenceTransformer (KURE_v1) → CPU 강제 사용
# ─────────────────────────────────────────────
model = SentenceTransformer("nlpai-lab/KURE-v1", device="cpu")

def encode_and_clear(texts, **kwargs):
    """CPU에서만 임베딩 수행 (GPU 완전 비활성)"""
    vectors = model.encode(texts, device="cpu", **kwargs)
    gc.collect()
    return vectors


# ─────────────────────────────────────────────
# ✅ 공통 점수 보정 함수 (RetailTech 출력 포맷)
# ─────────────────────────────────────────────
def apply_keyword_bonus(results, text_keywords, top_k):
    """검색 결과에 키워드 교집합 기반 점수 보너스 적용 + RetailTech 스타일 출력"""
    reranked = []
    for i, hit in enumerate(results, 1):
        payload = hit.payload
        score = float(hit.score)
        doc_keywords = payload.get("keywords", [])

        matched_keywords = []
        for kw in text_keywords:
            if kw in (payload.get("sFileName") or "") or kw in doc_keywords:
                matched_keywords.append(kw)

        # ✅ 키워드 매칭 시 점수 보정
     #   for j, _ in enumerate(matched_keywords):
     #       score += max(0.05 - j * 0.01, 0.01)

        # 🔹 필드 추출
        record_id = payload.get("record_id", "없음")
        store_name = payload.get("store_name", "점포명 없음")
        store_code = payload.get("store_code", "코드 없음")
        title = payload.get("title", "제목 없음")
        text = payload.get("text", "내용 없음")

        fault_major = payload.get("fault_major", "-")
        fault_mid = payload.get("fault_mid", "-")
        fault_minor = payload.get("fault_minor", "-")
        urgency = payload.get("urgency", "-")
        department_main = payload.get("department_main", "-")
        progress = payload.get("progress", "-")
        elapsed_time = payload.get("elapsed_time", "0")
        ocs_major = payload.get("ocs_cause_major", "-")
        ocs_mid = payload.get("ocs_cause_mid", "-")
        ocs_minor = payload.get("ocs_cause_minor", "-")

        keywords = payload.get("keywords", [])
        keywords_str = ", ".join(keywords) if keywords else "없음"

        year = payload.get("year", "")
        month = str(payload.get("month", "")).zfill(2)
        day = str(payload.get("day", "")).zfill(2)
        date_str = f"{year}-{month}-{day}" if year and month and day else "날짜 정보 없음"

        # 🔹 콘솔 출력
        print(f"🔹 결과 {i}")
        print(f"🆔 접수번호: {record_id}")
        print(f"🏪 점포명: {store_name} ({store_code})")
        print(f"📅 날짜: {date_str}")
        print(f"🕓 경과시간: {elapsed_time}시간 / 긴급도: {urgency}")
        print(f"🧩 장애유형: {fault_major} > {fault_mid} > {fault_minor}")
        print(f"⚙️ OCS 원인: {ocs_major} > {ocs_mid} > {ocs_minor}")
        print(f"🏢 처리부서: {department_main}")
        print(f"📋 진행단계: {progress}")
        print(f"🗝 키워드: {keywords_str}")
        print(f"🧠 유사도 점수: {round(score, 5)}")
        print(f"📝 제목: {title}")
        print(f"💬 내용: {text[:250]}{'...' if len(text) > 250 else ''}")
        print("────" * 10)

        reranked.append({
            "id": hit.id,
            "record_id": record_id,
            "store_name": store_name,
            "store_code": store_code,
            "date": date_str,
            "fault_major": fault_major,
            "fault_mid": fault_mid,
            "fault_minor": fault_minor,
            "urgency": urgency,
            "department_main": department_main,
            "progress": progress,
            "ocs_cause_major": ocs_major,
            "ocs_cause_mid": ocs_mid,
            "ocs_cause_minor": ocs_minor,
            "keywords": keywords_str,
            "title": title,
            "text": text,
            "score": round(score, 5),
        })

    print("\n🎯 검색 완료. 상위 결과를 반환합니다.")
    reranked.sort(key=lambda x: x["score"], reverse=True)
    return reranked[:top_k]


# ─────────────────────────────────────────────
# ✅ 단일 키워드 검색
# ─────────────────────────────────────────────
def keyword_search_single(keyword: str, top_k: int = 30) -> Tuple[Set, Dict, str]:
    keyword_type = "none"
    query_filter = None

    if re.fullmatch(r"\d{4}", keyword):  # 연도
        keyword_type = "year"
        query_filter = Filter(must=[
            FieldCondition(key="year", match=MatchValue(value=int(keyword)))
        ])
    elif keyword.isdigit() and 1 <= int(keyword) <= 12:  # 월
        keyword_type = "month"
        query_filter = Filter(must=[
            FieldCondition(key="month", match=MatchValue(value=int(keyword)))
        ])
    elif keyword.isdigit() and 1 <= int(keyword) <= 31:  # 일
        keyword_type = "day"
        query_filter = Filter(must=[
            FieldCondition(key="day", match=MatchValue(value=int(keyword)))
        ])
    else:  # 텍스트 키워드
        keyword_type = "text"
        query_filter = Filter(should=[
            FieldCondition(key="sFileName", match=MatchValue(value=keyword)),
            FieldCondition(key="keywords", match=MatchAny(any=[keyword])),
            FieldCondition(key="store_name", match=MatchValue(value=keyword)),  # ✅ 점포명 검색
            FieldCondition(key="store_code", match=MatchValue(value=keyword)),  # ✅ 점포코드 검색
        ])

    result = qdrant_client.query_points(
        collection_name=collection_name,
        query_filter=query_filter,
        limit=top_k,
        with_payload=True,
        with_vectors=True,
    )

    ids = {p.id for p in result.points}
    payloads = {p.id: {"payload": p.payload, "vector": p.vector} for p in result.points}

    return ids, payloads, keyword_type



# ─────────────────────────────────────────────
# ✅ 병렬 키워드 검색
# ─────────────────────────────────────────────
def search_qdrant_metadata_parallel(keywords: List[str], top_k_per_keyword: int = 50) -> Tuple[Dict, Dict, Dict]:
    all_payloads = {}
    keyword_results = {}
    keyword_types = {}

    if not keywords:
        return {}, {}, {}

    with ThreadPoolExecutor(max_workers=max(1, len(keywords))) as executor:
        futures = {executor.submit(keyword_search_single, kw, top_k_per_keyword): kw for kw in keywords}
        for future in futures:
            ids, payloads, kw_type = future.result()
            kw = futures[future]
            keyword_results[kw] = ids
            keyword_types[kw] = kw_type
            all_payloads.update(payloads)

    return keyword_results, all_payloads, keyword_types


# ─────────────────────────────────────────────
# ✅ 날짜 + 키워드 결합 검색
# ─────────────────────────────────────────────
def keyword_then_semantic_rerank(question: str, keywords: List[str], top_k: int = 5):
    print("\n" + "=" * 80)
    print(f"🧩 [keyword_then_semantic_rerank] 검색 요청 시작")
    print(f"📥 질문: {question}")
    print(f"🔑 키워드 리스트: {keywords}")
    print("=" * 80)

    keyword_results, all_payloads, keyword_types = search_qdrant_metadata_parallel(keywords, top_k_per_keyword=200)
    date_keywords = [kw for kw, t in keyword_types.items() if t in ("year", "month", "day")]
    text_keywords = [kw for kw, t in keyword_types.items() if t == "text"]

    print(f"📅 날짜 키워드: {date_keywords if date_keywords else '없음'}")
    print(f"💬 텍스트 키워드: {text_keywords if text_keywords else '없음'}")

    # 날짜가 포함된 경우
    if date_keywords:
        print("\n⚡ [1단계] 날짜 + 키워드 결합 → Qdrant 검색 실행")
        query_vector = encode_and_clear([question])[0]

        must_conditions = []
        for kw in date_keywords:
            kw_type = keyword_types[kw]
            if kw_type == "year":
                must_conditions.append(FieldCondition(key="year", match=MatchValue(value=int(kw))))
            elif kw_type == "month":
                must_conditions.append(FieldCondition(key="month", match=MatchValue(value=int(kw))))
            elif kw_type == "day":
                must_conditions.append(FieldCondition(key="day", match=MatchValue(value=int(kw))))

        if text_keywords:
            should_conditions = []
            for kw in text_keywords:
                should_conditions.extend([
                    FieldCondition(key="sFileName", match=MatchValue(value=kw)),
                    FieldCondition(key="keywords", match=MatchAny(any=[kw])),
                    FieldCondition(key="keywords", match={"text": kw}),
                ])
            must_conditions.append(Filter(should=should_conditions))

        filter_query = Filter(must=must_conditions)
        results = qdrant_client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=filter_query,
            limit=top_k * 10,
            with_payload=True
        )

        if not results:
            print("⚠️ [1단계] 검색 결과 0건 → 의미검색 fallback 실행")
            return semantic_vector_search(question, top_k)

        return apply_keyword_bonus(results, text_keywords, top_k)

    # 키워드만 있을 경우
    elif text_keywords:
        print("\n🔤 [2단계] 키워드 기반 검색 실행")
        query_vector = encode_and_clear([question])[0]
        should_conditions = []
        for kw in text_keywords:
            should_conditions.extend([
                FieldCondition(key="sFileName", match=MatchValue(value=kw)),
                FieldCondition(key="keywords", match=MatchAny(any=[kw])),
                FieldCondition(key="keywords", match={"text": kw}),
            ])
        filter_query = Filter(should=should_conditions)
        results = qdrant_client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=filter_query,
            limit=top_k * 10,
            with_payload=True
        )

        if not results:
            print("⚠️ [2단계] 검색 결과 0건 → 의미검색 fallback 실행")
            return semantic_vector_search(question, top_k)

        return apply_keyword_bonus(results, text_keywords, top_k)

    # 아무것도 없을 경우 → 의미검색 fallback
    else:
        print("\n⚠️ [3단계] 필터 없음 → 전체 의미검색 fallback")
        results = qdrant_client.search(
            collection_name=collection_name,
            query_vector=encode_and_clear([question])[0],
            limit=top_k * 10,
            with_payload=True
        )
        return apply_keyword_bonus(results, keywords, top_k)


# ─────────────────────────────────────────────
# ✅ 의미검색 fallback (단순 벡터검색)
# ─────────────────────────────────────────────
def semantic_vector_search(question: str, top_k: int = 30):
    print("\n⚙️ [단순 의미검색 fallback] 실행 중...")
    query_vector = encode_and_clear([question])[0]
    results = qdrant_client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=top_k,
        with_payload=True
    )

    reranked = []
    for i, hit in enumerate(results, 1):
        payload = hit.payload
        score = round(float(hit.score), 5)

        record_id = payload.get("record_id", "없음")
        store_name = payload.get("store_name", "점포명 없음")
        store_code = payload.get("store_code", "코드 없음")
        title = payload.get("title", "제목 없음")
        text = payload.get("text", "내용 없음")

        fault_major = payload.get("fault_major", "-")
        fault_mid = payload.get("fault_mid", "-")
        fault_minor = payload.get("fault_minor", "-")
        urgency = payload.get("urgency", "-")
        department_main = payload.get("department_main", "-")
        progress = payload.get("progress", "-")
        elapsed_time = payload.get("elapsed_time", "0")
        ocs_major = payload.get("ocs_cause_major", "-")
        ocs_mid = payload.get("ocs_cause_mid", "-")
        ocs_minor = payload.get("ocs_cause_minor", "-")

        keywords = payload.get("keywords", [])
        keywords_str = ", ".join(keywords) if keywords else "없음"

        year = payload.get("year", "")
        month = str(payload.get("month", "")).zfill(2)
        day = str(payload.get("day", "")).zfill(2)
        date_str = f"{year}-{month}-{day}" if year and month and day else "날짜 정보 없음"

        # 콘솔 출력 (apply_keyword_bonus와 동일)
        print(f"🔹 결과 {i}")
        print(f"🆔 접수번호: {record_id}")
        print(f"🏪 점포명: {store_name} ({store_code})")
        print(f"📅 날짜: {date_str}")
        print(f"🕓 경과시간: {elapsed_time}시간 / 긴급도: {urgency}")
        print(f"🧩 장애유형: {fault_major} > {fault_mid} > {fault_minor}")
        print(f"⚙️ OCS 원인: {ocs_major} > {ocs_mid} > {ocs_minor}")
        print(f"🏢 처리부서: {department_main}")
        print(f"📋 진행단계: {progress}")
        print(f"🗝 키워드: {keywords_str}")
        print(f"🧠 유사도 점수: {score}")
        print(f"📝 제목: {title}")
        print(f"💬 내용: {text[:250]}{'...' if len(text) > 250 else ''}")
        print("────" * 10)

        reranked.append({
            "id": hit.id,
            "record_id": record_id,
            "store_name": store_name,
            "store_code": store_code,
            "date": date_str,
            "fault_major": fault_major,
            "fault_mid": fault_mid,
            "fault_minor": fault_minor,
            "urgency": urgency,
            "department_main": department_main,
            "progress": progress,
            "ocs_cause_major": ocs_major,
            "ocs_cause_mid": ocs_mid,
            "ocs_cause_minor": ocs_minor,
            "keywords": keywords_str,
            "title": title,
            "text": text,
            "score": score,
        })

    print("\n🎯 의미검색 완료. 상위 결과를 반환합니다.")
    return reranked
