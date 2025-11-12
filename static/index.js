// ✅ 날짜 포맷 함수 (YYYY-MM-DD → YYYY년 M월 D일)
function formatDateKorean(dateStr) {
    if (!dateStr) return "";
    const parts = dateStr.split("-");
    if (parts.length !== 3) return dateStr;

    const year = parts[0];
    const month = String(parseInt(parts[1], 10));
    const day = String(parseInt(parts[2], 10));
    return `${year}년 ${month}월 ${day}일`;
}

function toSortableDateNum(dateStr) {
    if (!dateStr) return 0;
    let digits = dateStr.replace(/\D/g, "");
    if (digits.length < 8) return 0;
    return parseInt(digits.slice(0, 8), 10);
}

// ✅ 검색 함수
async function search() {
    const question = document.getElementById('questionInput').value.trim();
    const resultDiv = document.getElementById('result');
    resultDiv.innerHTML = '⏳ 검색 중...';

    if (!question) {
        resultDiv.innerHTML = '<p style="color:red;">❌ 검색어를 입력해주세요.</p>';
        return;
    }

    try {
        const response = await fetch("/search/documents", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question })
        });

        const data = await response.json();
        if (data.error) {
            resultDiv.innerHTML = `<p style="color:red;">❌ ${data.error}</p>`;
            return;
        }

        // 정확도순 정렬
        data.documents.sort((a, b) => parseFloat(b.score || b.accuracy) - parseFloat(a.score || a.accuracy));

        let html = `<p>🔎 총 ${data.result_count}건 검색됨</p>`;

        for (const [index, doc] of data.documents.entries()) {
            const safeId = `summary_${index}`;
            html += `
                <div class="result-card">
                    <div class="result-content">
                        <div class="result-title">📌 ${index + 1}. ${doc.store_name || "점포명 없음"} (${doc.store_code || "-"})</div>
                        <div class="result-meta">
                            🆔 접수번호: ${doc.record_id || "-"}<br>
                            📅 날짜: ${formatDateKorean(doc.date)}<br>
                            ⚙️ 장애유형: ${doc.fault_major || "-"} > ${doc.fault_mid || "-"} > ${doc.fault_minor || "-"}<br>
                            🧩 OCS 원인:<br>
                                - 대분류: ${doc.ocs_cause_major || "-"}<br>
                                - 중분류: ${doc.ocs_cause_mid || "-"}<br>
                                - 소분류: ${doc.ocs_cause_minor || "-"}<br>
                            🏢 처리부서: ${doc.department_main || "-"}<br>
                       
                            🚨 긴급도: ${doc.urgency || "-"}<br>
                           
                        </div>
                        <div class="result-accuracy">🎯 정확도: ${doc.accuracy || doc.score || "0"}%</div>
                        <div class="result-text">${doc.text ? doc.text.slice(0, 300) + (doc.text.length > 300 ? "..." : "") : "(본문 없음)"}</div>
                        <div class="result-buttons">
                            
                        <!--    <button
                            data-content="${encodeURIComponent(doc.text || '')}"
                            data-target="${safeId}"
                            data-store_name="${doc.store_name || ''}"
                            data-store_code="${doc.store_code || ''}"
                            data-date="${doc.date || ''}"
                            data-title="${doc.title || ''}"
                            data-fault_major="${doc.fault_major || ''}"
                            data-fault_mid="${doc.fault_mid || ''}"
                            data-fault_minor="${doc.fault_minor || ''}"
                            data-urgency="${doc.urgency || ''}"
                            data-department_main="${doc.department_main || ''}"
                            data-progress="${doc.progress || ''}"
                            data-ocs_major="${doc.ocs_cause_major || ''}"
                            data-ocs_mid="${doc.ocs_cause_mid || ''}"
                            data-ocs_minor="${doc.ocs_cause_minor || ''}"
                            data-keywords="${doc.keywords || ''}"
                            onclick="summarizeFromButton(this)">
                            요약하기
                            </button>
                            -->
                        </div>
                        <div id="${safeId}"></div>
                    </div>
                </div>
            `;
        }
        resultDiv.innerHTML = html;

    } catch (err) {
        console.error(err);
        resultDiv.innerHTML = `<p style="color:red;">❌ 오류 발생: ${err.message}</p>`;
    }
}

// ✅ 버튼에서 호출되는 함수
function summarizeFromButton(button) {
    const docData = {
        fault_major: button.dataset.fault_major || "-",
        fault_mid: button.dataset.fault_mid || "-",
        fault_minor: button.dataset.fault_minor || "-",
        ocs_cause_major: button.dataset.ocs_major || "-",
        ocs_cause_mid: button.dataset.ocs_mid || "-",
        ocs_cause_minor: button.dataset.ocs_minor || "-",
        store_name: button.dataset.store_name || "점포명 미상",
        urgency: button.dataset.urgency || "-",
        department_main: button.dataset.department_main || "-",
        date: button.dataset.date || "날짜 미상",
        content: decodeURIComponent(button.dataset.content || "")
    };
    console.log(docData);

    if (!docData.content) {
        alert("본문이 없습니다.");
        return;
    }

    summarize(docData, button.dataset.target);
}



// ✅ 요약 함수
async function summarize(doc, targetId) {
    const content = decodeURIComponent(doc.content || "");
    const targetDiv = document.getElementById(targetId);

    if (!content || content.length < 10) {
        targetDiv.innerText = "⚠️ 요약할 본문이 없습니다.";
        return;
    }

    targetDiv.className = "summary-box";
    targetDiv.innerText = "🧠 요약 중...";

    try {
        const payload = {
            content,
            fault_major: doc.fault_major || "-",
            fault_mid: doc.fault_mid || "-",
            fault_minor: doc.fault_minor || "-",
            ocs_cause_major: doc.ocs_cause_major || "-",
            ocs_cause_mid: doc.ocs_cause_mid || "-",
            ocs_cause_minor: doc.ocs_cause_minor || "-",
            department_main: doc.department_main || "-",
            urgency: doc.urgency || "-",
            date: doc.date || "-",
            store_name: doc.store_name || "-"
        };

        const response = await fetch("/summarize", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (data.summary) {
            targetDiv.innerHTML = "📄 ";
            let i = 0;
            const text = data.summary;

            function typeWriter() {
                if (i < text.length) {
                    const char = text.charAt(i);
                    targetDiv.innerHTML += (char === " " ? "&nbsp;" : char);
                    i++;
                    setTimeout(typeWriter, 15);
                }
            }
            typeWriter();
        } else {
            targetDiv.innerText = "❌ 요약 실패";
        }

    } catch (err) {
        targetDiv.innerText = `❌ 요약 중 오류: ${err.message}`;
    }
}



// ✅ HTML onclick 이벤트 등록
window.search = search;
window.summarizeFromButton = summarizeFromButton;
