import requests
import re

# ✅ vLLM API 서버 정보
VLLM_API_URL = "http://localhost:8000/v1/completions"
MODEL_ID = "/home/filadmin/ai-project/vllm/production-models/gemma-3-27b-it"

# ✅ 1️⃣ vLLM API 호출 함수
def call_vllm(prompt, max_tokens=256, stop=None):
    try:
        response = requests.post(
            VLLM_API_URL,
            headers={"Content-Type": "application/json"},
            json={
                "model": MODEL_ID,
                "prompt": prompt.strip(),
                "max_tokens": max_tokens,
                "temperature": 0.4,
                **({"stop": stop} if stop else {})
            },
            timeout=30
        )

        response.raise_for_status()
        result = response.json()
        print("🔍 vLLM 응답 전체:", result)

        choices = result.get("choices", [])
        if choices and "text" in choices[0]:
            return choices[0].get("text", "").strip()

        return "[⚠️ LLM 응답에 텍스트 없음]"

    except requests.RequestException as e:
        print(f"[❌ vLLM 호출 실패]: {e}")
        return "[❌ LLM 서버 연결 실패]"


# ✅ 2️⃣ 검색 키워드 생성 함수
def call_vllm_generate_search_condition(user_question):
    prompt = f"""
다음은 문서 검색용 키워드를 생성하는 작업이야.
❗️절대 설명하지 말고, 쉼표로 구분된 키워드 목록만 생성해.

규칙:
- 질문에 명시된 연도가 있을 때만 포함해. 없으면 연도는 절대 넣지 마.
- 연도는 항상 4자리 숫자 (예: '23년도' → '2023')
- 월,일이 들어가면 앞에 숫자만 추출해줘
- HTML 태그, 특수문자, 개행문자(\\n), 따옴표 등은 절대 포함하지 마
- 출력은 예: 키워드1, 키워드2, 키워드3 형식이어야 함

질문: {user_question}

키워드:"""
    return call_vllm(prompt, max_tokens=32, stop=["\n"])


# ✅ 3️⃣ 키워드 후처리 함수
def clean_llm_keywords(raw_text: str) -> list:
    first_line = raw_text.strip().split("\n")[0]  # 첫 줄만 사용
    cleaned = re.sub(r"(?i)질문\s*:.*", "", first_line)  # "질문:" 제거
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = re.sub(r"[\\\n\r\t]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return [kw.strip() for kw in cleaned.split(",") if kw.strip()]


def call_vllm_summarize_article(data: dict, user_question: str = None):
    """
    스토리텔링 요약용 LLM 호출 함수
    :param data: dict 형태로 전달된 장애 데이터 (FastAPI에서 그대로 전달됨)
    :param user_question: 선택적 사용자 질문 (기존 구조 유지)
    """

    # 🔹 데이터 정리
    content = clean_article_text(data.get("content", ""))
    store_name = data.get("store_name", "")
    date = data.get("date", "")
    fault_major = data.get("fault_major", "")
    fault_mid = data.get("fault_mid", "")
    fault_minor = data.get("fault_minor", "")
    ocs_major = data.get("ocs_cause_major", "")
    ocs_mid = data.get("ocs_cause_mid", "")
    ocs_minor = data.get("ocs_cause_minor", "")
    department = data.get("department_main", "")
    urgency = data.get("urgency", "")

    # 🔸 프롬프트 구성
    prompt = f"""
다음은 {store_name} 점포에서 발생한 장애 내역입니다.
현장 엔지니어가 상급 관리자에게 구두로 보고하듯, 자연스럽고 간결한 스토리텔링 형식으로 정리해 주세요.

조건:
- "요약"이라는 단어를 사용하지 말 것
- 세 문장 이내로 간결하게 작성
- 장애 발생 → 원인 → 조치/결과 순서로 기술
- 긴급도(A~C)는 문맥에 녹여 자연스럽게 반영할 것
- 숫자, 코드명(VKV47 등)은 정확하게 유지할 것
- 장애 원인과 처리 결과만 간결하게 2~3문장으로 정리
- 사실 근거가 없는 추론 문장은 작성하지 말 것

📅 날짜: {date}
🏪 점포명: {store_name}
⚙️ 장애유형: {fault_major} > {fault_mid} > {fault_minor}
🧩 OCS 원인:
  - 대분류: {ocs_major}
  - 중분류: {ocs_mid}
  - 소분류: {ocs_minor}
🏢 처리부서: {department}
🚨 긴급도: {urgency}

[본문]
{content}
"""

    # 🔸 vLLM 호출 (max_tokens은 상황에 맞게)
    raw_summary = call_vllm(prompt, max_tokens=1024)

    # 🔸 후처리: 의미 유지한 문장 정리
    return clean_sentences_preserve_meaning(raw_summary)




# ✅ 5️⃣ 문장 정제 함수
def clean_sentences_preserve_meaning(text: str) -> str:
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ✅ 6️⃣ 기사 본문 정제 함수
def clean_article_text(text: str) -> str:
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = text.replace('“', '"').replace('”', '"')
    text = text.replace("‘", "'").replace("’", "'")
    text = re.sub(r"\([^)]{0,30}\)", "", text)
    text = re.sub(r"[•★☆▶▲▼→※]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
