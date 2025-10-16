function encodeNameForC5(name) {
  try {
    return encodeURIComponent(name || "");
  } catch (e) {
    return "";
  }
}

function base64utf8(s) {
  try {
    return btoa(unescape(encodeURIComponent(s || "")));
  } catch (e) {
    return "";
  }
}

function zbtIconUrl(marketHashName) {
  const b64 = base64utf8(marketHashName || "");
  if (!b64) return null;
  return `https://img.zbt.com/e/steam/item/730/${b64}.png`;
}

function linkForPlatform(platformName, itemId, name, marketHashName) {
  const p = (platformName || "").toUpperCase();
  if (p === "BUFF") {
    if (!itemId) return null;
    return `https://buff.163.com/goods/${itemId}`;
  }
  if (p === "C5" || p === "C5GAME") {
    if (!itemId) return null;
    const enc = encodeNameForC5(name || marketHashName);
    return `https://www.c5game.com/csgo/${itemId}/${enc}/sell`;
  }
  if (p === "YOUPIN" || p === "YOUPIN898") {
    if (!itemId) return null;
    return `https://www.youpin898.com/market/goods-list?listType=10&templateId=${itemId}&gameId=730`;
  }
  if (p === "HALO" || p === "HALOSKINS") {
    if (!itemId) return null;
    return `https://www.haloskins.com/market/${itemId}`;
  }
  if (p === "STEAM") {
    const enc = encodeURIComponent(marketHashName || name || "");
    return `https://steamcommunity.com/market/listings/730/${enc}`;
  }
  if (p === "SKINPORT") {
    const enc = encodeURIComponent(marketHashName || name || "");
    return `https://skinport.com/zh/market?search=${enc}&sort=price&order=asc`;
  }
  if (p === "DMARKET") {
    const enc = encodeURIComponent(marketHashName || name || "");
    return `https://dmarket.com/zh/ingame-items/item-list/csgo-skins?ref=fDuhF5X91i&title=${enc}`;
  }
  return null;
}

async function fetchJSON(url, opts) {
  const resp = await fetch(url, opts);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || `HTTP ${resp.status}`);
  return data;
}

async function refreshBase() {
  const btn = document.getElementById("btnFetchBase");
  btn.disabled = true;
  try {
    const data = await fetchJSON("/api/base/fetch", { method: "POST" });
    alert(`下载完成，数量=${data.count}`);
  } catch (e) {
    alert(`刷新失败: ${e.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function loadBase() {
  const q = document.getElementById("filterQuery").value.trim();
  const platform = document.getElementById("filterPlatform").value.trim();
  try {
    const data = await fetchJSON(`/api/base?q=${encodeURIComponent(q)}&platform=${encodeURIComponent(platform)}`);
    const tbody = document.getElementById("baseTbody");
    tbody.innerHTML = "";
    (data.data || []).forEach(item => {
      const tr = document.createElement("tr");
      const td1 = document.createElement("td");
      const td2 = document.createElement("td");
      const td3 = document.createElement("td");
      // icon + name
      const icon = zbtIconUrl(item.marketHashName);
      if (icon) {
        const img = document.createElement("img");
        img.src = icon;
        img.alt = item.name || item.marketHashName || "";
        img.width = 24;
        img.height = 24;
        img.loading = "lazy";
        img.style.verticalAlign = "middle";
        img.style.marginRight = "6px";
        img.onerror = () => { img.style.display = "none"; };
        td1.appendChild(img);
      }
      const nameSpan = document.createElement("span");
      nameSpan.textContent = item.name || "";
      td1.appendChild(nameSpan);
      td2.textContent = item.marketHashName || "";
      const plats = item.platformList || [];
      const links = [];
      plats.forEach(p => {
        const href = linkForPlatform(p.name, p.itemId, item.name, item.marketHashName);
        if (href) {
          const a = document.createElement("a");
          a.href = href;
          a.target = "_blank";
          a.rel = "noopener noreferrer";
          a.textContent = p.name;
          links.push(a);
        }
      });
      if (links.length === 0) {
        td3.textContent = "无平台信息";
      } else {
        links.forEach((a, idx) => {
          td3.appendChild(a);
          if (idx < links.length - 1) td3.appendChild(document.createTextNode(" | "));
        });
      }
      tr.appendChild(td1);
      tr.appendChild(td2);
      tr.appendChild(td3);
      tbody.appendChild(tr);
    });
  } catch (e) {
    alert(`加载失败: ${e.message}`);
  }
}

async function exportCSV() {
  const q = document.getElementById("filterQuery").value.trim();
  const platform = document.getElementById("filterPlatform").value.trim();
  const url = `/api/base/export/csv?q=${encodeURIComponent(q)}&platform=${encodeURIComponent(platform)}`;
  window.open(url, "_blank");
}

async function exportJSON() {
  const q = document.getElementById("filterQuery").value.trim();
  const platform = document.getElementById("filterPlatform").value.trim();
  const url = `/api/base/export/json?q=${encodeURIComponent(q)}&platform=${encodeURIComponent(platform)}`;
  window.open(url, "_blank");
}

async function importLocalToDB() {
  // 弹出文件选择框，选择本地 JSON 文件
  const input = document.createElement("input");
  input.type = "file";
  input.accept = ".json,application/json";
  input.onchange = async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    try {
      const text = await file.text();
      let payload;
      try {
        payload = JSON.parse(text);
      } catch (e) {
        alert("解析 JSON 失败，请检查文件格式");
        return;
      }
      const resp = await fetchJSON(`/api/base/import_payload`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      alert(`导入完成：新增 ${resp.inserted} 条，跳过 ${resp.skipped} 条，新增平台 ${resp.platforms_inserted} 条`);
    } catch (e) {
      alert(`导入失败: ${e.message}`);
    }
  };
  input.click();
}

async function doSingle() {
  const name = document.getElementById("singleName").value.trim();
  const pre = document.getElementById("singleResult");
  pre.textContent = "查询中...";
  try {
    const data = await fetchJSON(`/api/price/single?marketHashName=${encodeURIComponent(name)}`);
    pre.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    pre.textContent = `失败: ${e.message}`;
  }
}

async function doBatch() {
  const lines = document.getElementById("batchNames").value.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  const pre = document.getElementById("batchResult");
  pre.textContent = "查询中...";
  try {
    const data = await fetchJSON(`/api/price/batch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ marketHashNames: lines })
    });
    pre.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    pre.textContent = `失败: ${e.message}`;
  }
}

async function doBatchByIdRange() {
  const rng = document.getElementById("homeBatchIdRange").value.trim();
  if (!rng) { alert("请填写ID范围，例如 1-100"); return; }
  const pre = document.getElementById("homeBatchIdRangeResult");
  pre.textContent = "执行中...";
  try {
    const data = await fetchJSON('/api/admin/price/batch_by_id', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ idRange: rng })
    });
    pre.textContent = JSON.stringify(data, null, 2);
    const processed = data.processedNames || 0;
    const inserted = data.insertedRows || 0;
    const saved = data.saved || '';
    alert(`批量完成：处理 ${processed} 个名称，入库 ${inserted} 行，导出文件：${saved}`);
  } catch (e) {
    pre.textContent = '失败: ' + e.message;
  }
}

async function doAvg() {
  const name = document.getElementById("avgName").value.trim();
  const pre = document.getElementById("avgResult");
  pre.textContent = "查询中...";
  try {
    const data = await fetchJSON(`/api/price/avg?marketHashName=${encodeURIComponent(name)}`);
    pre.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    pre.textContent = `失败: ${e.message}`;
  }
}

window.addEventListener("DOMContentLoaded", () => {
  document.getElementById("btnFetchBase").addEventListener("click", refreshBase);
  document.getElementById("btnLoadBase").addEventListener("click", loadBase);
  document.getElementById("btnExportCSV").addEventListener("click", exportCSV);
  document.getElementById("btnExportJSON").addEventListener("click", exportJSON);
  const importBtn = document.getElementById("btnImportLocal");
  if (importBtn) importBtn.addEventListener("click", importLocalToDB);
  const importBtn2 = document.getElementById("btnImportLocal2");
  if (importBtn2) importBtn2.addEventListener("click", importLocalPricesToDB);
  document.getElementById("btnSingle").addEventListener("click", doSingle);
  document.getElementById("btnBatch").addEventListener("click", doBatch);
  const btnRange = document.getElementById("btnBatchIdRange");
  if (btnRange) btnRange.addEventListener("click", doBatchByIdRange);
  document.getElementById("btnAvg").addEventListener("click", doAvg);

  // 任务控制按钮事件
  const btnStart = document.getElementById("btnJobStart");
  const btnPause = document.getElementById("btnJobPause");
  const btnResume = document.getElementById("btnJobResume");
  const btnStop = document.getElementById("btnJobStop");
  if (btnStart) btnStart.addEventListener("click", jobStart);
  if (btnPause) btnPause.addEventListener("click", jobPause);
  if (btnResume) btnResume.addEventListener("click", jobResume);
  if (btnStop) btnStop.addEventListener("click", jobStop);

  // 初始拉取一次状态，并每秒轮询
  refreshJobStatus();
  setInterval(refreshJobStatus, 1000);
});

// 导入价格到数据库（覆盖旧数据）
async function importLocalPricesToDB() {
  const input = document.createElement("input");
  input.type = "file";
  input.accept = ".json,application/json";
  input.onchange = async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    try {
      const text = await file.text();
      let payload;
      try {
        payload = JSON.parse(text);
      } catch (e) {
        alert("解析 JSON 失败，请检查文件格式");
        return;
      }
      const resp = await fetch(`/api/admin/price/import_payload`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok || !data.success) {
        alert("导入价格失败：" + (data.error || resp.statusText));
        return;
      }
      alert(
        `导入完成\n覆盖旧记录：${data.overwritten}\n新写入：${data.inserted}\n处理饰品：${data.itemsProcessed} 平台：${data.platformsProcessed}`
      );
    } catch (e) {
      alert(`导入失败: ${e.message}`);
    }
  };
  input.click();
}

// 自动任务控制与状态轮询
function renderJobUI(state) {
  const bar = document.getElementById("jobProgressBar");
  const stateEl = document.getElementById("jobState");
  const nextRangeEl = document.getElementById("jobNextRange");
  const countdownEl = document.getElementById("jobCountdown");
  const progTextEl = document.getElementById("jobProgText");
  const rangeEl = document.getElementById("jobRange");

  if (!state || !bar) return;
  const percent = typeof state.percent === "number" ? state.percent : 0;
  bar.style.width = `${percent}%`;
  if (stateEl) stateEl.textContent = state.state || "-";
  if (nextRangeEl) nextRangeEl.textContent = `${state.currentStartId || 0}-${state.currentEndIdNext || 0}`;
  if (countdownEl) {
    if (state.paused) countdownEl.textContent = "暂停中";
    else if (typeof state.nextRunSeconds === "number") countdownEl.textContent = `${state.nextRunSeconds}s`;
    else countdownEl.textContent = "-";
  }
  const total = state.maxId || 0;
  const completed = state.completedCount || 0;
  if (progTextEl) progTextEl.textContent = `${completed}/${total} (${percent}%)`;
  const rng = state.lastProcessedRange ? `${state.lastProcessedRange[0]}-${state.lastProcessedRange[1]}` : "-";
  if (rangeEl) rangeEl.textContent = rng;

  // 控制按钮可用性
  const btnStart = document.getElementById("btnJobStart");
  const btnPause = document.getElementById("btnJobPause");
  const btnResume = document.getElementById("btnJobResume");
  const btnStop = document.getElementById("btnJobStop");
  const running = !!state.running;
  const paused = !!state.paused;
  if (btnStart) btnStart.disabled = running; // 已在运行则禁用开始
  if (btnPause) btnPause.disabled = !running || paused;
  if (btnResume) btnResume.disabled = !running || !paused;
  if (btnStop) btnStop.disabled = !running;
}

async function refreshJobStatus() {
  try {
    const data = await fetchJSON('/api/admin/job/status');
    renderJobUI(data);
  } catch (e) {
    // 可忽略
  }
}

async function jobStart() {
  try {
    const input = document.getElementById('jobStartId');
    let startId = 1;
    if (input && input.value) {
      const v = parseInt(String(input.value).trim(), 10);
      if (Number.isFinite(v) && v > 0) startId = v;
    }
    const payload = { startId, batchSize: 100 };
    const data = await fetchJSON('/api/admin/job/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    renderJobUI(data);
  } catch (e) {
    alert('启动失败: ' + e.message);
  }
}

async function jobPause() {
  try {
    const data = await fetchJSON('/api/admin/job/pause', { method: 'POST' });
    renderJobUI(data);
  } catch (e) {
    alert('暂停失败: ' + e.message);
  }
}

async function jobResume() {
  try {
    const data = await fetchJSON('/api/admin/job/resume', { method: 'POST' });
    renderJobUI(data);
  } catch (e) {
    alert('继续失败: ' + e.message);
  }
}

async function jobStop() {
  try {
    const data = await fetchJSON('/api/admin/job/stop', { method: 'POST' });
    renderJobUI(data);
  } catch (e) {
    alert('结束失败: ' + e.message);
  }
}