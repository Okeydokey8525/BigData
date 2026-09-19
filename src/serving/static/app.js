/**
 * Flight Delay AI Platform - Frontend Logic
 * Author: Nhóm 6 - Nhập môn Big Data (HUIT)
 */

let appMetadata = null;

document.addEventListener('DOMContentLoaded', async () => {
    console.log("[*] Flight Delay AI Platform initialized.");
    await loadMetadata();
    await loadBenchmarkTable();
});

// ==========================================
// 1. Tab Switching Logic
// ==========================================
function switchTab(tabId) {
    // Hide all tabs
    document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.nav-tab').forEach(el => el.classList.remove('active'));

    // Show target tab
    const targetContent = document.getElementById(`tab-content-${tabId}`);
    const targetBtn = document.getElementById(`tab-btn-${tabId}`);

    if (targetContent) targetContent.classList.add('active');
    if (targetBtn) targetBtn.classList.add('active');
}

// ==========================================
// 2. Load Metadata & Dropdowns
// ==========================================
async function loadMetadata() {
    try {
        const response = await fetch('/api/metadata');
        if (!response.ok) throw new Error("Không thể nạp metadata.");
        appMetadata = await response.json();

        // 1. Populate Models
        const modelSelect = document.getElementById('select-model');
        modelSelect.innerHTML = '';
        appMetadata.models.forEach((m, idx) => {
            const opt = document.createElement('option');
            opt.value = m.id;
            opt.textContent = m.name;
            if (m.id === 'random_forest_cpu') opt.selected = true; // Default
            modelSelect.appendChild(opt);
        });

        // 2. Populate Carriers
        const carrierSelect = document.getElementById('select-carrier');
        carrierSelect.innerHTML = '';
        appMetadata.carriers.forEach(c => {
            const opt = document.createElement('option');
            opt.value = c.code;
            opt.textContent = c.name;
            if (c.code === 'DL') opt.selected = true; // Default Delta Air Lines
            carrierSelect.appendChild(opt);
        });

        // 3. Populate Origin & Dest Airports
        const originSelect = document.getElementById('select-origin');
        const destSelect = document.getElementById('select-dest');
        originSelect.innerHTML = '';
        destSelect.innerHTML = '';

        appMetadata.airports.forEach(a => {
            const opt1 = document.createElement('option');
            opt1.value = a.code;
            opt1.textContent = a.name;
            if (a.code === 'JFK') opt1.selected = true;
            originSelect.appendChild(opt1);

            const opt2 = document.createElement('option');
            opt2.value = a.code;
            opt2.textContent = a.name;
            if (a.code === 'LAX') opt2.selected = true;
            destSelect.appendChild(opt2);
        });

        console.log("[✓] Metadata loaded successfully:", appMetadata);
    } catch (err) {
        console.error("[!] Lỗi nạp metadata:", err);
    }
}

// ==========================================
// 3. Handle Live Prediction
// ==========================================
async function handlePrediction(event) {
    event.preventDefault();

    const btn = document.getElementById('btn-submit-predict');
    const spinner = document.getElementById('btn-spinner');
    const btnText = document.getElementById('btn-text');

    // UI Loading state
    btn.disabled = true;
    spinner.classList.remove('hidden');
    btnText.textContent = "Đang phân tích qua mạng nơ-ron/cây quyết định...";

    const modelChoice = document.getElementById('select-model').value;
    const carrier = document.getElementById('select-carrier').value;
    const origin = document.getElementById('select-origin').value;
    const dest = document.getElementById('select-dest').value;
    const flightDate = document.getElementById('input-date').value;
    const depTime = document.getElementById('input-dep-time').value;
    const duration = parseFloat(document.getElementById('input-duration').value);
    const distance = parseFloat(document.getElementById('input-distance').value);

    const payload = {
        model_name: modelChoice,
        op_unique_carrier: carrier,
        origin: origin,
        dest: dest,
        fl_date: flightDate,
        dep_time_str: depTime,
        crs_elapsed_time: duration,
        distance: distance
    };

    try {
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const errData = await response.json();
            throw new Error(errData.detail || "Lỗi khi gọi API dự đoán.");
        }

        const data = await response.json();
        renderPredictionResult(data);
    } catch (err) {
        alert("Lỗi: " + err.message);
    } finally {
        btn.disabled = false;
        spinner.classList.add('hidden');
        btnText.textContent = "🚀 Phân Tích & Dự Báo Ngay";
    }
}

function renderPredictionResult(data) {
    document.getElementById('result-placeholder').classList.add('hidden');
    const resultCard = document.getElementById('result-content');
    resultCard.classList.remove('hidden');

    // 1. Primary Diagnosis Banner
    const diagBanner = document.getElementById('primary-diagnosis');
    const diagIcon = document.getElementById('diag-icon');
    const diagModel = document.getElementById('diag-model-used');
    const diagTitle = document.getElementById('diag-title');
    const diagDesc = document.getElementById('diag-desc');

    diagBanner.className = "diagnosis-banner " + (data.badge_type || "primary");
    diagIcon.textContent = data.icon || "✈️";
    diagModel.textContent = `Thuật toán: ${data.model_used}`;
    diagTitle.textContent = data.predicted_cause_title;
    diagDesc.textContent = data.predicted_cause_desc;

    // 2. Meta Pills
    document.getElementById('val-latency').textContent = `${data.latency_ms.toFixed(2)} ms`;
    document.getElementById('val-confidence').textContent = `${data.confidence_pct.toFixed(1)}%`;

    // 3. Probabilities Breakdown
    const probContainer = document.getElementById('probabilities-container');
    probContainer.innerHTML = '';

    data.probabilities.forEach(item => {
        const div = document.createElement('div');
        div.className = 'prob-item';
        div.innerHTML = `
            <div class="prob-header">
                <span class="prob-label">${item.title}</span>
                <span class="prob-percent">${item.percent.toFixed(1)}%</span>
            </div>
            <div class="prob-track">
                <div class="prob-bar" style="width: ${item.percent}%; background-color: ${item.color};"></div>
            </div>
        `;
        probContainer.appendChild(div);
    });

    // 4. Actionable Advice
    document.getElementById('advice-text').textContent = data.advice;
}

// ==========================================
// 4. Load Benchmark Table
// ==========================================
async function loadBenchmarkTable() {
    try {
        const response = await fetch('/api/metrics');
        if (!response.ok) return;
        const data = await response.json();

        const tbody = document.getElementById('benchmark-tbody');
        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            if (row.Model.includes('Random Forest')) {
                tr.className = 'highlight-row';
            }
            tr.innerHTML = `
                <td><strong>${row.Model}</strong></td>
                <td>${row["Train Time (s)"]}s</td>
                <td>${row["Latency (ms/1k)"]} ms</td>
                <td>${row["RAM Usage (MB)"]} MB</td>
                <td><span class="badge-tag">${row["Accuracy (%)"]}%</span></td>
                <td><strong style="color: #38bdf8;">${row["Weighted F1 (%)"]}%</strong></td>
                <td><strong style="color: #34d399;">${row["Macro F1 (%)"]}%</strong></td>
            `;
            tbody.appendChild(tr);
        });
    } catch (err) {
        console.warn("[!] Không thể tải dữ liệu bảng benchmark:", err);
    }
}

// ==========================================
// 5. Update Explainability Visualizations
// ==========================================
function updateExplainabilityCharts() {
    const selectedModel = document.getElementById('select-explain-model').value;
    const cmImg = document.getElementById('img-confusion-matrix');
    const fiImg = document.getElementById('img-feature-importance');
    const fiCard = document.getElementById('card-feature-importance');

    // Update Confusion Matrix (7M)
    cmImg.src = `/api/figures/cm_${selectedModel}_7m.png`;

    // Update Feature Importance (Logistic regression doesn't have feature_importances_)
    if (selectedModel === 'logistic_regression') {
        fiCard.style.display = 'none';
    } else {
        fiCard.style.display = 'block';
        fiImg.src = `/api/figures/feat_imp_${selectedModel}_7m.png`;
    }
}
