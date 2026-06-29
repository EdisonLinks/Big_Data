function setText(id, value) {
    const node = document.getElementById(id);
    if (node) node.textContent = value;
}

function updateRefreshTime() {
    const node = document.getElementById("last-refresh");
    if (!node) return;
    node.textContent = new Date().toLocaleString("zh-CN", { hour12: false });
}

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function renderLatestReviews(rows) {
    const node = document.getElementById("latest-review-list");
    if (!node) return;
    if (!rows || rows.length === 0) {
        node.innerHTML = '<div class="empty-state">暂无实时评论。</div>';
        return;
    }
    node.innerHTML = rows.map((row) => {
        const isUserSubmit = row.source === "flask_review_form";
        const cls = isUserSubmit ? "live-review-card user-submit" : "live-review-card";
        const badge = isUserSubmit ? '<span class="source-pill">用户提交</span>' : "";
        return `
        <article class="${cls}">
            <div>
                <strong>${escapeHtml(row.meal_name || row.meal_id)}</strong>
                <small>${escapeHtml(row.user_id)} · ${escapeHtml(row.review_date || "")}</small>
            </div>
            ${badge}
            <span class="rating-chip">${escapeHtml(row.rating)}</span>
            <p>${escapeHtml(row.review || "暂无文字评论")}</p>
        </article>`;
    }).join("");
}

async function refreshDashboard() {
    try {
        const response = await fetch("/api/dashboard", { cache: "no-store" });
        if (!response.ok) return;
        const data = await response.json();
        const summary = data.summary || {};
        setText("metric-rating-count", summary.rating_count ?? "-");
        setText("metric-user-count", summary.user_count ?? "-");
        setText("metric-meal-count", summary.meal_count ?? "-");
        setText("metric-avg-rating", Number(summary.avg_rating || 0).toFixed(3));
        setText("metric-live-reviews", data.live_review_count ?? 0);
        renderLatestReviews(data.latest_reviews || []);
        updateRefreshTime();
    } catch (error) {
        console.warn("dashboard refresh failed", error);
    }
}

updateRefreshTime();
refreshDashboard();
setInterval(refreshDashboard, 2000);
