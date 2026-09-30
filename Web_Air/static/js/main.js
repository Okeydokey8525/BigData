/**
 * Web_Air - JavaScript Client Controller
 * Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
 */

let METADATA_CACHE = null;
let FIGURES_CACHE = [];

document.addEventListener("DOMContentLoaded", () => {
    initTabs();
    initPresets();
    fetchSystemStatus();
    loadFiguresCatalog();
    loadMetricsData();
});

// ==============================================================================
// 1. TABS MANAGEMENT
// ==============================================================================
function initTabs() {
    const tabButtons = document.querySelectorAll(".tab-btn");
    const tabPanes = document.querySelectorAll(".tab-pane");

    tabButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            const targetId = btn.getAttribute("data-tab");

            tabButtons.forEach(b => b.classList.remove("active"));
            tabPanes.forEach(p => p.classList.remove("active"));

            btn.classList.add("active");
            const targetPane = document.getElementById(targetId);
            if (targetPane) {
                targetPane.classList.add("active");
            }

            // Lazy load data khi mở tab
            if (targetId === "tab-benchmark") {
                loadMetricsData();
            } else if (targetId === "tab-figures") {
                loadFiguresCatalog();
            }
        });
    });
}

// ==============================================================================
// 2. PRESETS QUICK FILL
// ==============================================================================
const PRESET_DATA = {
    "transcon": {
        "op_unique_carrier": "DL", "origin": "JFK", "dest": "LAX",
        "fl_date": "2024-07-15", "dep_time_str": "08:30",
        "crs_elapsed_time": 360, "distance": 2475
    },
    "hub_rush": {
        "op_unique_carrier": "AA", "origin": "ORD", "dest": "ATL",
        "fl_date": "2024-08-20", "dep_time_str": "17:45",
        "crs_elapsed_time": 130, "distance": 606
    },
    "storm_season": {
        "op_unique_carrier": "WN", "origin": "DFW", "dest": "MIA",
        "fl_date": "2024-09-18", "dep_time_str": "19:15",
        "crs_elapsed_time": 170, "distance": 1121
    },
    "short_hop": {
        "op_unique_carrier": "UA", "origin": "SFO", "dest": "SEA",
        "fl_date": "2024-05-10", "dep_time_str": "12:00",
        "crs_elapsed_time": 135, "distance": 679
    }
};

function initPresets() {
    // Không cần xử lý thêm nếu dùng inline onclick
}

function applyPreset(presetId) {
    const data = PRESET_DATA[presetId];
    if (!data) return;

    for (const [key, val] of Object.entries(data)) {
        const el = document.getElementById(key);
        if (el) el.value = val;
    }

    // Tự động kích hoạt dự đoán với mô hình hiện tại
    handlePredict(false);
}

// ==============================================================================
// 3. PREDICTION HANDLER (SINGLE & BATTLE)
// ==============================================================================
async function handlePredict(isBattle = false) {
    const form = document.getElementById("prediction-form");
    if (!form.checkValidity()) {
        form.reportValidity();
        return;
    }

    const payload = {
        model_id: document.getElementById("model_id").value,
        op_unique_carrier: document.getElementById("op_unique_carrier").value,
        origin: document.getElementById("origin").value.trim().toUpperCase(),
        dest: document.getElementById("dest").value.trim().toUpperCase(),
        fl_date: document.getElementById("fl_date").value,
        dep_time_str: document.getElementById("dep_time_str").value,
        crs_elapsed_time: parseFloat(document.getElementById("crs_elapsed_time").value),
        distance: parseFloat(document.getElementById("distance").value)
    };

    const placeholder = document.getElementById("result-placeholder");
    const singleDisplay = document.getElementById("result-single");
    const battleDisplay = document.getElementById("result-battle");
    const btnSingle = document.getElementById("btn-single-predict");

    // UI Loading state
    if (placeholder) placeholder.style.display = "none";
    if (singleDisplay) singleDisplay.style.display = "none";
    if (battleDisplay) battleDisplay.style.display = "none";

    const oldText = btnSingle.innerHTML;
    btnSingle.innerHTML = `<span>⏳</span> Đang tính toán 13 đặc trưng...`;
    btnSingle.disabled = true;

    try {
        if (!isBattle) {
            // Chạy 1 mô hình
            const resp = await fetch("/api/predict", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            if (!resp.ok) {
                const err = await resp.json();
                throw new Error(err.detail || "Lỗi khi gọi API dự đoán.");
            }

            const json = await resp.json();
            renderSingleResult(json.data);
            singleDisplay.style.display = "block";
        } else {
            // Chạy đối đầu cả 6 mô hình
            const resp = await fetch("/api/predict-all", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            if (!resp.ok) {
                const err = await resp.json();
                throw new Error(err.detail || "Lỗi khi gọi API đối đầu.");
            }

            const json = await resp.json();
            renderBattleResult(json);
            battleDisplay.style.display = "block";
        }
    } catch (error) {
        alert("Lỗi thực thi suy luận: " + error.message);
        if (placeholder) placeholder.style.display = "flex";
    } finally {
        btnSingle.innerHTML = oldText;
        btnSingle.disabled = false;
    }
}

function renderSingleResult(data) {
    const banner = document.getElementById("res-banner");
    const icon = document.getElementById("res-icon");
    const badge = document.getElementById("res-badge");
    const title = document.getElementById("res-title");
    const desc = document.getElementById("res-desc");
    const conf = document.getElementById("res-confidence");
    const latency = document.getElementById("res-latency");
    const model = document.getElementById("res-model");
    const advice = document.getElementById("res-advice");
    const probContainer = document.getElementById("prob-bars");

    // Áp dụng màu sắc & nội dung
    banner.style.borderColor = data.color;
    banner.style.backgroundColor = `${data.color}15`;
    icon.innerHTML = data.icon;
    badge.innerText = data.predicted_name;
    badge.style.color = data.color;
    title.innerText = data.predicted_title;
    desc.innerText = data.description;

    conf.innerText = `${data.confidence_percent}%`;
    latency.innerText = `${data.latency_ms} ms`;
    model.innerText = data.model_name;
    advice.innerText = data.advice;

    // Render thanh xác suất 6 lớp
    probContainer.innerHTML = "";
    data.probabilities.forEach(p => {
        const item = document.createElement("div");
        item.className = "prob-item";
        item.innerHTML = `
            <div class="prob-meta">
                <span>${p.icon} ${p.title}</span>
                <span style="color: ${p.color};">${p.percent}%</span>
            </div>
            <div class="prob-track">
                <div class="prob-fill" style="width: ${p.percent}%; background-color: ${p.color};"></div>
            </div>
        `;
        probContainer.appendChild(item);
    });
}

function renderBattleResult(data) {
    const title = document.getElementById("battle-consensus-title");
    const fastest = document.getElementById("battle-fastest");
    const container = document.getElementById("battle-cards");

    const con = data.consensus;
    title.innerHTML = `${con.icon} Đa số mô hình dự đoán: <strong>${con.title}</strong> (${con.agree_count}/${con.total_models} mô hình)`;
    fastest.innerHTML = `⚡ Mô hình nhanh nhất: <strong>${con.fastest_model}</strong> (${con.min_latency_ms} ms)`;

    container.innerHTML = "";
    data.models_results.forEach(m => {
        const card = document.createElement("div");
        card.className = "battle-model-card";
        card.style.borderLeft = `4px solid ${m.color}`;
        card.innerHTML = `
            <div class="battle-model-header">
                <span class="battle-model-name">${m.model_name}</span>
                <span class="battle-model-latency">${m.latency_ms} ms</span>
            </div>
            <div class="battle-model-pred">
                <span>${m.icon}</span>
                <strong style="color: ${m.color};">${m.predicted_title}</strong>
            </div>
            <div class="prob-track" style="height: 6px; margin-top: 4px;">
                <div class="prob-fill" style="width: ${m.confidence_percent}%; background-color: ${m.color};"></div>
            </div>
            <span style="font-size: 11px; color: #9CA3AF; text-align: right;">Độ tin cậy: ${m.confidence_percent}%</span>
        `;
        container.appendChild(card);
    });
}

// ==============================================================================
// 4. METRICS & PIPELINE STAGES LOADER (TAB 2)
// ==============================================================================
async function loadMetricsData() {
    const tbody = document.getElementById("metrics-tbody");
    const stagesBody = document.getElementById("stages-tbody");

    // 1. Tải bảng đối sánh chính
    try {
        const resp = await fetch("/api/metrics");
        if (resp.ok) {
            const data = await resp.json();
            if (data.length > 0) {
                tbody.innerHTML = "";
                data.forEach(row => {
                    const tr = document.createElement("tr");
                    tr.innerHTML = `
                        <td><strong>${row["Model"]}</strong></td>
                        <td>${row["Train Time (s)"]}</td>
                        <td>${row["Latency (ms/1k)"]}</td>
                        <td>${row["RAM Usage (MB)"]}</td>
                        <td>${row["Accuracy (%)"]}</td>
                        <td>${row["Weighted Precision (%)"]}</td>
                        <td>${row["Weighted Recall (%)"]}</td>
                        <td class="${row['Weighted F1 (%)'] > 70.5 ? 'highlight-best' : ''}">${row["Weighted F1 (%)"]}</td>
                        <td>${row["Macro F1 (%)"]}</td>
                    `;
                    tbody.appendChild(tr);
                });
            }
        }
    } catch (e) {
        console.error("Lỗi khi tải bảng đối sánh metrics:", e);
    }

    // 2. Tải bảng 4 giai đoạn pipeline
    try {
        const resp = await fetch("/api/pipeline-stages");
        if (resp.ok) {
            const stages = await resp.json();
            if (stages.length > 0) {
                stagesBody.innerHTML = "";
                stages.forEach(s => {
                    const tr = document.createElement("tr");
                    tr.innerHTML = `
                        <td><strong>${s["Approach"]}</strong></td>
                        <td>${s["ETL"]}</td>
                        <td>${s["Feature_Engineering"]}</td>
                        <td>${s["Training"]}</td>
                        <td>${s["Evaluation"]}</td>
                        <td><strong>${s["Total_Time"]}</strong></td>
                        <td>${s["Peak_RAM_MB"]}</td>
                    `;
                    stagesBody.appendChild(tr);
                });
            }
        }
    } catch (e) {
        console.error("Lỗi khi tải bảng phân rã pipeline:", e);
    }
}

// ==============================================================================
// 5. FIGURES GALLERY LOADER (TAB 3)
// ==============================================================================
async function loadFiguresCatalog() {
    if (FIGURES_CACHE.length > 0) return;

    try {
        const resp = await fetch("/api/figures-list");
        if (resp.ok) {
            FIGURES_CACHE = await resp.json();
            renderFiguresGrid("all");
            initFigureFilters();
        }
    } catch (e) {
        console.error("Lỗi khi tải danh mục biểu đồ:", e);
    }
}

function initFigureFilters() {
    const filterBtns = document.querySelectorAll(".filter-btn");
    filterBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            filterBtns.forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            const filter = btn.getAttribute("data-filter");
            renderFiguresGrid(filter);
        });
    });
}

function renderFiguresGrid(filter = "all") {
    const container = document.getElementById("figures-container");
    container.innerHTML = "";

    const filtered = filter === "all" 
        ? FIGURES_CACHE 
        : FIGURES_CACHE.filter(f => f.category === filter);

    filtered.forEach(fig => {
        const card = document.createElement("div");
        card.className = "figure-card";
        card.onclick = () => openModal(fig.filename, fig.title, fig.desc);
        card.innerHTML = `
            <img class="figure-thumb" src="/api/figures/${fig.filename}" alt="${fig.title}" loading="lazy">
            <div class="figure-info">
                <span class="figure-badge">${fig.category_name}</span>
                <h4>${fig.title}</h4>
                <p>${fig.desc}</p>
            </div>
        `;
        container.appendChild(card);
    });
}

// ==============================================================================
// 6. LIGHTBOX MODAL ZOOM
// ==============================================================================
function openModal(filename, title, desc) {
    const modal = document.getElementById("image-modal");
    const img = document.getElementById("modal-img");
    const titleEl = document.getElementById("modal-title");
    const descEl = document.getElementById("modal-desc");
    const dlEl = document.getElementById("modal-download");

    img.src = `/api/figures/${filename}`;
    titleEl.innerText = title;
    descEl.innerText = desc;
    dlEl.href = `/api/figures/${filename}`;
    dlEl.setAttribute("download", filename);

    modal.classList.add("show");
    document.body.style.overflow = "hidden";
}

function closeModal(event) {
    const modal = document.getElementById("image-modal");
    modal.classList.remove("show");
    document.body.style.overflow = "auto";
}

// Đóng modal khi bấm phím Escape
document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
        closeModal();
    }
});

// ==============================================================================
// 7. SYSTEM STATUS BADGE
// ==============================================================================
async function fetchSystemStatus() {
    try {
        const resp = await fetch("/api/system-status");
        if (resp.ok) {
            const data = await resp.json();
            const badge = document.getElementById("sys-summary");
            if (badge) {
                badge.innerText = `RTX 5050 | RAM: ${data.ram_available_gb}GB Trống (${data.process_ram_mb}MB App)`;
            }
        }
    } catch (e) {
        console.log("Status API: Fallback default");
    }
}
