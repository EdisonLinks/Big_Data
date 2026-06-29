(function () {
    const root = document.querySelector("[data-chart-payload]");
    if (!root || !window.echarts) return;

    const payload = JSON.parse(root.dataset.chartPayload || "{}");
    const charts = [];

    function mount(id, option) {
        const node = document.getElementById(id);
        if (!node) return;
        const chart = window.echarts.init(node);
        chart.setOption(option);
        charts.push(chart);
    }

    const distribution = payload.distribution || [];
    mount("rating-distribution-chart", {
        tooltip: { trigger: "axis" },
        grid: { left: 36, right: 18, top: 24, bottom: 32 },
        xAxis: { type: "category", data: distribution.map((row) => `${Number(row.rating || 0).toFixed(0)}分`) },
        yAxis: { type: "value" },
        series: [{
            type: "bar",
            data: distribution.map((row) => row.rating_count || 0),
            itemStyle: { color: "#c95f35", borderRadius: [4, 4, 0, 0] }
        }]
    });

    const popular = payload.popular_meals || [];
    mount("popular-meals-chart", {
        tooltip: { trigger: "axis" },
        grid: { left: 80, right: 24, top: 16, bottom: 24 },
        xAxis: { type: "value" },
        yAxis: { type: "category", inverse: true, data: popular.map((row) => row.meal_name || row.meal_id) },
        series: [{
            type: "bar",
            data: popular.map((row) => row.avg_rating || 0),
            itemStyle: { color: "#39745a", borderRadius: [0, 4, 4, 0] }
        }]
    });

    const activeUsers = payload.active_users || [];
    mount("active-users-chart", {
        tooltip: { trigger: "axis" },
        grid: { left: 92, right: 18, top: 18, bottom: 28 },
        xAxis: { type: "value" },
        yAxis: { type: "category", inverse: true, data: activeUsers.map((row) => row.user_id) },
        series: [{
            type: "bar",
            data: activeUsers.map((row) => row.rating_count || 0),
            itemStyle: { color: "#d99a2b", borderRadius: [0, 4, 4, 0] }
        }]
    });

    const realtimeTopMeals = payload.realtime_top_meals || [];
    mount("realtime-window-chart", {
        tooltip: { trigger: "axis" },
        grid: { left: 86, right: 24, top: 18, bottom: 30 },
        xAxis: { type: "value" },
        yAxis: {
            type: "category",
            inverse: true,
            data: realtimeTopMeals.map((row) => row.meal_name || row.meal_id)
        },
        series: [{
            name: "实时热度",
            type: "bar",
            data: realtimeTopMeals.map((row) => row.hot_score_2s || 0),
            itemStyle: { color: "#1f5946", borderRadius: [0, 4, 4, 0] }
        }]
    });

    const metrics = payload.model_metrics || [];
    mount("model-metrics-chart", {
        tooltip: { trigger: "axis" },
        legend: { top: 0 },
        grid: { left: 42, right: 18, top: 42, bottom: 32 },
        xAxis: { type: "category", data: metrics.map((row) => String(row.model_type || "").toUpperCase()) },
        yAxis: { type: "value" },
        series: [
            { name: "RMSE", type: "bar", data: metrics.map((row) => row.rmse || 0), itemStyle: { color: "#c95f35" } },
            { name: "覆盖率", type: "bar", data: metrics.map((row) => row.prediction_coverage || 0), itemStyle: { color: "#2d4050" } },
            { name: "MAE", type: "bar", data: metrics.map((row) => row.mae || 0), itemStyle: { color: "#39745a" } }
        ]
    });

    window.addEventListener("resize", () => {
        charts.forEach((chart) => chart.resize());
    });
}());
