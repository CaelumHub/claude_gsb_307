/* ============================================================
   公共脚本：导航注入、API 封装、工具函数
   ============================================================ */

const PAGES = [
  { file: "projects.html",    name: "项目管理",     desc: "项目总览" },
  { file: "cases.html",       name: "测试用例",     desc: "步骤与断言" },
  { file: "suites.html",      name: "套件与分组",   desc: "组织用例" },
  { file: "monitor.html",     name: "执行监控",     desc: "实时日志状态" },
  { file: "reports.html",     name: "测试报告",     desc: "通过率耗时" },
  { file: "coverage.html",    name: "代码覆盖率",   desc: "覆盖率分析" },
  { file: "defects.html",     name: "缺陷跟踪",     desc: "缺陷闭环" },
  { file: "environments.html",name: "环境管理",     desc: "配置与依赖" },
  { file: "schedules.html",   name: "定时任务",     desc: "计划与触发" },
  { file: "notifications.html", name: "通知与集成", desc: "Webhook 等" },
];

const PAGE_NAMES = {
  projects: "项目管理", cases: "测试用例", suites: "测试套件与分组",
  monitor: "执行监控", reports: "测试报告", coverage: "代码覆盖率",
  defects: "缺陷跟踪", environments: "环境管理", schedules: "定时任务与触发",
  notifications: "通知与集成",
};

const STATUS_LABELS = {
  pending: "等待中", running: "运行中", passed: "通过", failed: "失败",
  cancelled: "已取消", error: "错误", skipped: "跳过", timeout: "超时",
};

const PRIORITY_LABELS = { P0: "P0 · 最高", P1: "P1 · 高", P2: "P2 · 中", P3: "P3 · 低" };

/* ---------- 导航注入 ---------- */
function renderNav(activeFile) {
  const host = document.getElementById("site-sidebar");
  if (!host) return;
  let links = PAGES.map((p, i) => {
    const cls = p.file === activeFile ? ' class="active"' : "";
    return `<a href="/page/${p.file}"${cls}><span class="idx">${String(i + 1).padStart(2, "0")}</span>${p.name}</a>`;
  }).join("");
  host.innerHTML = `
    <div class="brand">测试与CI平台<small>自动化测试 · 持续集成</small></div>
    <nav>${links}</nav>`;
}

/* ---------- API 封装 ---------- */
async function api(path, options = {}) {
  const opts = { headers: {}, ...options };
  if (opts.body && typeof opts.body === "object" && !(opts.body instanceof FormData)) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(opts.body);
  }
  const resp = await fetch(path, opts);
  let data = null;
  try { data = await resp.json(); } catch (_) { /* ignore */ }
  if (!resp.ok) {
    const msg = (data && data.error) || `请求失败 (${resp.status})`;
    throw new Error(msg);
  }
  return data;
}

/* ---------- 工具 ---------- */
function $(sel, root = document) { return root.querySelector(sel); }
function $$(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function toast(msg, type = "ok") {
  let el = $(".toast");
  if (!el) {
    el = document.createElement("div");
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.className = `toast ${type}`;
  requestAnimationFrame(() => el.classList.add("show"));
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove("show"), 3000);
}

function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function fmtDur(s) {
  if (s == null) return "—";
  return s >= 1 ? s.toFixed(2) + "s" : Math.round(s * 1000) + "ms";
}

function setLoading(btn, on, text = "处理中…") {
  if (!btn) return;
  if (on) {
    btn.dataset.label = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner"></span>${text}`;
  } else {
    btn.disabled = false;
    btn.innerHTML = btn.dataset.label || btn.innerHTML;
  }
}

function badge(status, label) {
  const s = status || "pending";
  return `<span class="badge ${s}">${esc(label || STATUS_LABELS[s] || s)}</span>`;
}

function statusBadge(status) {
  return badge(status, STATUS_LABELS[status] || status);
}

function priorityBadge(p) {
  return `<span class="tag">${esc(PRIORITY_LABELS[p] || p || "P2")}</span>`;
}

function tagsHtml(tags) {
  return (tags || []).map(t => `<span class="tag">${esc(t)}</span>`).join("");
}

/* 把编辑器里的字符串解析成合适的 JSON 字面量：
   "14" -> 14, "true" -> true, "[10,20]" -> [10,20], 其余保持字符串 */
function parseLiteral(s) {
  if (typeof s !== "string") return s;
  const t = s.trim();
  if (t === "true") return true;
  if (t === "false") return false;
  if (t === "null") return null;
  if (/^-?\d+(\.\d+)?$/.test(t)) return Number(t);
  if ((t.startsWith("[") && t.endsWith("]")) || (t.startsWith("{") && t.endsWith("}"))) {
    try { return JSON.parse(t); } catch (_) { return s; }
  }
  return s;
}

/* 页面元数据自动填充标题 */
function renderPageTitle(key, desc) {
  const t = $(".page-title");
  if (t && PAGE_NAMES[key]) t.textContent = PAGE_NAMES[key];
  const d = $(".page-desc");
  if (d && desc) d.textContent = desc;
}

/* 项目下拉选择器（多页面共用）：填入项目，返回选中的 project_id */
async function fillProjectSelect(sel, selectedId) {
  if (!sel) return null;
  const data = await api("/api/projects");
  const projects = data.projects || [];
  if (!projects.length) {
    // 还没有任何项目：清空下拉，并把页面里所有「加载中」占位替换为引导提示，
    // 避免页面停在「加载中…」。
    sel.innerHTML = '<option value="">— 选择项目 —</option>';
    document.querySelectorAll(".empty").forEach((el) => {
      if (el.textContent.trim() === "加载中…") {
        el.innerHTML = '暂无项目。请先到 <a href="/page/projects.html">项目管理</a> 页生成演示项目或新建项目。';
      }
    });
    return null;
  }
  sel.innerHTML = '<option value="">— 选择项目 —</option>' +
    projects.map(p => `<option value="${p.id}" ${p.id === selectedId ? "selected" : ""}>${esc(p.name)}</option>`).join("");
  const cur = sel.value || (projects[0] && projects[0].id) || "";
  if (projects[0] && !selectedId) sel.value = projects[0].id;
  return sel.value || cur;
}

/* 进度条 HTML */
function progressBar(ratio, cls = "") {
  const pct = Math.max(0, Math.min(100, Math.round(ratio * 100)));
  return `<div class="bar ${cls}"><i style="width:${pct}%"></i></div>`;
}
