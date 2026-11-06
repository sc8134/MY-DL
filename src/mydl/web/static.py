"""Embedded dashboard HTML/JS for web UI."""

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>mydl - Download Manager</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f5f5;
      color: #333;
      line-height: 1.5;
    }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    header {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid #ddd;
    }
    h1 { font-size: 1.5rem; font-weight: 600; }
    .connection-status {
      display: flex; align-items: center; gap: 8px; font-size: 0.875rem;
      padding: 4px 12px; border-radius: 12px; background: #e0e0e0; color: #666;
    }
    .connection-status.connected { background: #e8f5e9; color: #2e7d32; }
    .connection-status .dot { width: 8px; height: 8px; border-radius: 50%; background: #999; }
    .connection-status.connected .dot { background: #4caf50; }

    .panel { background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }
    .panel-header { padding: 16px 20px; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; }
    .panel-title { font-weight: 600; font-size: 1rem; }
    .panel-body { padding: 0; }

    /* Add URL form */
    .add-form { padding: 20px; display: grid; gap: 16px; }
    .form-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
    .form-group { display: flex; flex-direction: column; gap: 4px; }
    label { font-size: 0.875rem; font-weight: 500; color: #555; }
    input, select {
      padding: 10px 12px; border: 1px solid #ddd; border-radius: 6px;
      font-size: 0.9375rem; outline: none; transition: border-color 0.2s;
    }
    input:focus, select:focus { border-color: #1976d2; }
    .btn {
      padding: 10px 20px; border: none; border-radius: 6px; font-size: 0.9375rem;
      font-weight: 500; cursor: pointer; transition: background 0.2s;
    }
    .btn-primary { background: #1976d2; color: white; }
    .btn-primary:hover { background: #1565c0; }
    .btn-secondary { background: #e0e0e0; color: #333; }
    .btn-secondary:hover { background: #d5d5d5; }
    .btn-danger { background: #f44336; color: white; }
    .btn-danger:hover { background: #e53935; }
    .btn-small { padding: 6px 12px; font-size: 0.8125rem; }

    /* Task table */
    .task-table { width: 100%; border-collapse: collapse; }
    .task-table th { text-align: left; padding: 12px 16px; font-weight: 600; font-size: 0.8125rem; text-transform: uppercase; color: #666; background: #fafafa; border-bottom: 1px solid #eee; }
    .task-table td { padding: 12px 16px; border-bottom: 1px solid #f0f0f0; font-size: 0.875rem; }
    .task-table tr:hover { background: #fafafa; }
    .task-table tr.paused { opacity: 0.6; }
    .task-table tr.failed { background: #fff3e0; }
    .task-table tr.completed { background: #f1f8e9; }

    .url-cell { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .progress-bar { width: 100%; height: 6px; background: #e0e0e0; border-radius: 3px; overflow: hidden; }
    .progress-bar-fill { height: 100%; background: #1976d2; border-radius: 3px; transition: width 0.3s; }
    .progress-bar.completed .progress-bar-fill { background: #4caf50; }
    .progress-bar.failed .progress-bar-fill { background: #f44336; }
    .progress-text { font-size: 0.75rem; color: #666; margin-top: 4px; }
    .speed-eta { font-size: 0.75rem; color: #888; }

    .status-badge {
      display: inline-block; padding: 2px 8px; border-radius: 10px;
      font-size: 0.6875rem; font-weight: 600; text-transform: uppercase;
    }
    .status-pending { background: #e3f2fd; color: #1565c0; }
    .status-queued { background: #fff3e0; color: #ef6c00; }
    .status-running { background: #e8f5e9; color: #2e7d32; }
    .status-paused { background: #f5f5f5; color: #757575; }
    .status-completed { background: #f1f8e9; color: #33691e; }
    .status-failed { background: #fbe9e7; color: #c62828; }
    .status-cancelled { background: #f5f5f5; color: #757575; }

    .actions { display: flex; gap: 4px; }
    .empty-state { padding: 40px; text-align: center; color: #999; }

    /* Settings panel */
    .settings-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; padding: 20px; }
    .setting-item { display: flex; flex-direction: column; gap: 4px; }
    .setting-item label { font-size: 0.8125rem; }
    .setting-item input[type="number"] { width: 100px; }
    .setting-item input[type="checkbox"] { width: auto; }

    footer { text-align: center; padding: 20px; color: #999; font-size: 0.8125rem; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>mydl Web UI</h1>
      <div id="conn-status" class="connection-status">
        <span class="dot"></span>
        <span id="conn-text">Connecting...</span>
      </div>
    </header>

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Add Download</span>
      </div>
      <div class="panel-body">
        <form id="add-form" class="add-form">
          <div class="form-row">
            <div class="form-group">
              <label for="url">URL *</label>
              <input type="url" id="url" placeholder="https://example.com/file.zip" required>
            </div>
            <div class="form-group">
              <label for="mirrors">Mirrors (comma-separated)</label>
              <input type="text" id="mirrors" placeholder="https://mirror1/file.zip, https://mirror2/file.zip">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label for="mirror-strategy">Mirror Strategy</label>
              <select id="mirror-strategy">
                <option value="fallback">Fallback (sequential)</option>
                <option value="parallel">Parallel (race)</option>
                <option value="fastest">Fastest (probe first)</option>
              </select>
            </div>
            <div class="form-group">
              <label for="connections">Connections</label>
              <input type="number" id="connections" value="1" min="1" max="16">
            </div>
            <div class="form-group">
              <label for="speed-limit">Speed Limit</label>
              <input type="text" id="speed-limit" placeholder="e.g. 2M, 512K">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label for="output-dir">Output Directory</label>
              <input type="text" id="output-dir" placeholder="downloads">
            </div>
            <div class="form-group">
              <label for="output-template">Name Template</label>
              <input type="text" id="output-template" placeholder="{index}-{filename}">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label for="sha256">SHA256 (verify)</label>
              <input type="text" id="sha256" placeholder="hex hash">
            </div>
            <div class="form-group">
              <label for="resume">Resume</label>
              <select id="resume"><option value="true">Yes</option><option value="false">No</option></select>
            </div>
          </div>
          <div style="display: flex; gap: 8px;">
            <button type="submit" class="btn btn-primary">Add to Queue</button>
          </div>
        </form>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Download Queue</span>
        <span id="queue-stats" style="font-size: 0.8125rem; color: #666;"></span>
      </div>
      <div class="panel-body">
        <table class="task-table" id="task-table">
          <thead>
            <tr>
              <th>URL</th>
              <th>Progress</th>
              <th>Speed / ETA</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody id="task-body"></tbody>
        </table>
        <div id="empty-state" class="empty-state" style="display: none;">No downloads yet. Add a URL above to get started.</div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Settings</span>
      </div>
      <div class="panel-body">
        <div class="settings-grid">
          <div class="setting-item">
            <label>Max Concurrent Downloads</label>
            <input type="number" id="max-concurrent" value="3" min="1" max="10">
          </div>
          <div class="setting-item">
            <label>Default Connections</label>
            <input type="number" id="default-connections" value="1" min="1" max="16">
          </div>
        </div>
      </div>
    </div>

    <footer>mydl - Stdlib-only download manager</footer>
  </div>

  <script>
    // --- State ---
    const ws = new WebSocket(`ws://${location.host}/api/ws`);
    const tasks = new Map();
    let reconnectTimeout = null;

    // --- DOM Elements ---
    const connStatus = document.getElementById('conn-status');
    const connText = document.getElementById('conn-text');
    const taskBody = document.getElementById('task-body');
    const emptyState = document.getElementById('empty-state');
    const queueStats = document.getElementById('queue-stats');
    const addForm = document.getElementById('add-form');
    const maxConcurrentInput = document.getElementById('max-concurrent');

    // --- Helpers ---
    function fmtBytes(bytes) {
      if (!bytes && bytes !== 0) return '?';
      const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
      let i = 0;
      while (bytes >= 1024 && i < units.length - 1) { bytes /= 1024; i++; }
      return i === 0 ? `${bytes} ${units[i]}` : `${bytes.toFixed(1)} ${units[i]}`;
    }

    function fmtTime(seconds) {
      if (!seconds || seconds !== seconds || seconds === Infinity) return '--:--';
      const s = Math.max(0, Math.floor(seconds));
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = s % 60;
      return h ? `${h}:${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}` : `${m.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
    }

    function statusClass(status) {
      return `status-${status}`;
    }

    function formatProgress(task) {
      const { downloaded, total, speed, eta } = task.progress;
      if (!total) return '<div class="progress-bar"><div class="progress-bar-fill" style="width: 0%"></div></div><div class="progress-text">Waiting...</div>';
      const pct = Math.round((downloaded / total) * 100);
      return `
        <div class="progress-bar ${task.status === 'completed' ? 'completed' : task.status === 'failed' ? 'failed' : ''}">
          <div class="progress-bar-fill" style="width: ${pct}%"></div>
        </div>
        <div class="progress-text">${fmtBytes(downloaded)} / ${fmtBytes(total)} (${pct}%)</div>
      `;
    }

    function formatSpeedEta(task) {
      const { speed, eta } = task.progress;
      if (!speed && !eta) return '<div class="speed-eta">--</div>';
      return `<div class="speed-eta">${fmtBytes(speed)}/s ${eta ? '• ETA ' + fmtTime(eta) : ''}</div>`;
    }

    function renderTask(task) {
      const tr = document.createElement('tr');
      tr.dataset.id = task.id;
      if (task.status === 'paused') tr.classList.add('paused');
      if (task.status === 'failed') tr.classList.add('failed');
      if (task.status === 'completed') tr.classList.add('completed');

      const mirrorsHtml = task.mirrors.length > 0 ? `<br><small style="color:#666">${task.mirrors.length} mirror(s)</small>` : '';
      const chosenHtml = task.chosen_mirror && task.chosen_mirror !== task.url ? `<br><small style="color:#1976d2">via: ${task.chosen_mirror}</small>` : '';

      tr.innerHTML = `
        <td class="url-cell" title="${task.url}">${task.url}${mirrorsHtml}${chosenHtml}</td>
        <td>${formatProgress(task)}</td>
        <td>${formatSpeedEta(task)}</td>
        <td><span class="status-badge ${statusClass(task.status)}">${task.status}</span>${task.error ? `<br><small style="color:#c62828">${escapeHtml(task.error)}</small>` : ''}</td>
        <td class="actions">
          ${task.status === 'running' ? `<button class="btn btn-small btn-secondary" onclick="pauseTask('${task.id}')">Pause</button>` : ''}
          ${task.status === 'paused' ? `<button class="btn btn-small btn-primary" onclick="resumeTask('${task.id}')">Resume</button>` : ''}
          ${(task.status === 'pending' || task.status === 'queued') ? `<button class="btn btn-small btn-primary" onclick="startTask('${task.id}')">Start</button>` : ''}
          ${(task.status === 'running' || task.status === 'paused' || task.status === 'queued' || task.status === 'pending') ? `<button class="btn btn-small btn-danger" onclick="cancelTask('${task.id}')">Cancel</button>` : ''}
          ${(task.status === 'completed' || task.status === 'failed' || task.status === 'cancelled') ? `<button class="btn btn-small btn-secondary" onclick="removeTask('${task.id}')">Remove</button>` : ''}
        </td>
      `;
      return tr;
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    function updateTaskInDOM(task) {
      const existing = taskBody.querySelector(`tr[data-id="${task.id}"]`);
      const newRow = renderTask(task);
      if (existing) {
        existing.replaceWith(newRow);
      } else {
        taskBody.appendChild(newRow);
      }
      tasks.set(task.id, task);
      updateStats();
    }

    function removeTaskFromDOM(taskId) {
      const row = taskBody.querySelector(`tr[data-id="${taskId}"]`);
      if (row) row.remove();
      tasks.delete(taskId);
      updateStats();
    }

    function updateStats() {
      let running = 0, queued = 0, completed = 0, failed = 0;
      for (const t of tasks.values()) {
        if (t.status === 'running') running++;
        else if (t.status === 'queued' || t.status === 'pending') queued++;
        else if (t.status === 'completed') completed++;
        else if (t.status === 'failed') failed++;
      }
      queueStats.textContent = `${running} running, ${queued} queued, ${completed} done, ${failed} failed`;
      emptyState.style.display = tasks.size === 0 ? 'block' : 'none';
    }

    // --- WebSocket ---
    ws.onopen = () => {
      connStatus.classList.add('connected');
      connText.textContent = 'Connected';
    };

    ws.onclose = () => {
      connStatus.classList.remove('connected');
      connText.textContent = 'Disconnected - Reconnecting...';
      reconnectTimeout = setTimeout(() => location.reload(), 5000);
    };

    ws.onerror = (e) => {
      console.error('WS error', e);
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'init') {
          taskBody.innerHTML = '';
          tasks.clear();
          for (const task of msg.tasks) {
            updateTaskInDOM(task);
          }
        } else if (msg.type === 'update') {
          updateTaskInDOM(msg.task);
        }
      } catch (e) {
        console.error('WS message parse error', e);
      }
    };

    // --- API Actions ---
    async function api(method, path, body = null) {
      const opts = { method, headers: { 'Content-Type': 'application/json' } };
      if (body) opts.body = JSON.stringify(body);
      const resp = await fetch(path, opts);
      if (!resp.ok) throw new Error(await resp.text());
      return resp.json();
    }

    window.startTask = (id) => api('PATCH', `/api/queue/${id}`, { action: 'resume' });
    window.pauseTask = (id) => api('PATCH', `/api/queue/${id}`, { action: 'pause' });
    window.resumeTask = (id) => api('PATCH', `/api/queue/${id}`, { action: 'resume' });
    window.cancelTask = (id) => api('PATCH', `/api/queue/${id}`, { action: 'cancel' });
    window.removeTask = (id) => api('DELETE', `/api/queue/${id}`);

    // --- Form Submit ---
    addForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const formData = new FormData(addForm);
      const data = {
        url: formData.get('url') || document.getElementById('url').value,
        mirrors: document.getElementById('mirrors').value.split(',').map(s => s.trim()).filter(Boolean),
        mirror_strategy: document.getElementById('mirror-strategy').value,
        connections: parseInt(document.getElementById('connections').value) || 1,
        speed_limit: document.getElementById('speed-limit').value || null,
        output_dir: document.getElementById('output-dir').value || null,
        output_template: document.getElementById('output-template').value || null,
        sha256: document.getElementById('sha256').value || null,
        resume: document.getElementById('resume').value === 'true',
      };
      try {
        await api('POST', '/api/queue', data);
        addForm.reset();
      } catch (err) {
        alert('Failed to add task: ' + err.message);
      }
    });

    // --- Settings ---
    maxConcurrentInput.addEventListener('change', () => {
      // This would need a settings API endpoint
      console.log('Max concurrent:', maxConcurrentInput.value);
    });
  </script>
</body>
</html>
"""


def get_dashboard_html() -> str:
    return DASHBOARD_HTML