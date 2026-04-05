/**
 * College Data Enrichment CRM — Frontend Controller
 * Handles file upload, SSE streaming, results display, and UI state.
 */

// ─── State ──────────────────────────────────────────────────────
const API_BASE = 'https://webcrm-r7lk.onrender.com/api';
let currentJobId = null;
let eventSource = null;
let selectedFile = null;
let isPaused = false;
let totalRows = 0;

// ─── DOM Elements ───────────────────────────────────────────────
const $ = (sel) => document.querySelector(sel);
const uploadZone = $('#uploadZone');
const fileInput = $('#fileInput');
const fileInfo = $('#fileInfo');
const fileName = $('#fileName');
const fileMeta = $('#fileMeta');
const fileRemove = $('#fileRemove');
const previewSection = $('#previewSection');
const previewHead = $('#previewHead');
const previewBody = $('#previewBody');
const uploadActions = $('#uploadActions');
const startBtn = $('#startBtn');
const rowCount = $('#rowCount');
const processingSection = $('#processingSection');
const progressBar = $('#progressBar');
const progressPercent = $('#progressPercent');
const statProcessed = $('#statProcessed');
const statActive = $('#statActive');
const statInactive = $('#statInactive');
const statNotFound = $('#statNotFound');
const currentItem = $('#currentItem');
const currentCollege = $('#currentCollege');
const etaInfo = $('#etaInfo');
const logConsole = $('#logConsole');
const pauseBtn = $('#pauseBtn');
const resumeBtn = $('#resumeBtn');
const cancelBtn = $('#cancelBtn');
const resultsSection = $('#resultsSection');
const resultsBody = $('#resultsBody');
const downloadBtn = $('#downloadBtn');
const searchInput = $('#searchInput');
const statusFilter = $('#statusFilter');
const emptyResults = $('#emptyResults');
const themeToggle = $('#themeToggle');
const footerTime = $('#footerTime');

// ─── Theme Toggle ───────────────────────────────────────────────
function initTheme() {
    const saved = localStorage.getItem('theme') || 'dark';
    document.documentElement.setAttribute('data-theme', saved);
    themeToggle.textContent = saved === 'dark' ? '🌙' : '☀️';
}

themeToggle.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
    themeToggle.textContent = next === 'dark' ? '🌙' : '☀️';
});

initTheme();

// ─── File Upload ────────────────────────────────────────────────
uploadZone.addEventListener('click', () => fileInput.click());

uploadZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadZone.classList.add('dragover');
});

uploadZone.addEventListener('dragleave', () => {
    uploadZone.classList.remove('dragover');
});

uploadZone.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadZone.classList.remove('dragover');
    const files = e.dataTransfer.files;
    if (files.length > 0) handleFile(files[0]);
});

fileInput.addEventListener('change', () => {
    if (fileInput.files.length > 0) handleFile(fileInput.files[0]);
});

fileRemove.addEventListener('click', () => {
    clearFile();
});

function handleFile(file) {
    const ext = file.name.split('.').pop().toLowerCase();
    if (!['xlsx', 'xls', 'csv'].includes(ext)) {
        showAlert('Invalid file type. Please upload .xlsx, .xls, or .csv');
        return;
    }
    if (file.size > 50 * 1024 * 1024) {
        showAlert('File too large. Maximum size is 50MB.');
        return;
    }

    selectedFile = file;
    fileName.textContent = file.name;
    fileMeta.textContent = `${formatSize(file.size)} • ${ext.toUpperCase()}`;
    fileInfo.classList.add('visible');
    uploadZone.style.display = 'none';

    uploadFile(file);
}

function clearFile() {
    selectedFile = null;
    fileInput.value = '';
    fileInfo.classList.remove('visible');
    previewSection.classList.remove('visible');
    uploadActions.style.display = 'none';
    uploadZone.style.display = '';
    currentJobId = null;
}

async function uploadFile(file) {
    startBtn.disabled = true;
    startBtn.innerHTML = '⏳ Uploading...';

    const formData = new FormData();
    formData.append('file', file);

    try {
        const resp = await fetch(`${API_BASE}/upload`, { method: 'POST', body: formData });
        const data = await resp.json();

        if (!resp.ok) {
            showAlert(data.error || 'Upload failed');
            clearFile();
            return;
        }

        currentJobId = data.job_id;
        totalRows = data.total_rows;
        rowCount.textContent = `${data.total_rows} colleges found`;

        // Show preview
        if (data.preview && data.preview.length > 0) {
            renderPreview(data.columns, data.preview);
            previewSection.classList.add('visible');
        }

        uploadActions.style.display = 'flex';
        startBtn.disabled = false;
        startBtn.innerHTML = '🚀 Start Processing';

    } catch (err) {
        showAlert('Upload failed: ' + err.message);
        clearFile();
    }
}

function renderPreview(columns, rows) {
    previewHead.innerHTML = '<tr>' + columns.map(c => `<th>${esc(c)}</th>`).join('') + '</tr>';
    previewBody.innerHTML = rows.map(row =>
        '<tr>' + columns.map(c => `<td>${esc(row[c] || '')}</td>`).join('') + '</tr>'
    ).join('');
}

// ─── Start Processing ───────────────────────────────────────────
startBtn.addEventListener('click', async () => {
    if (!currentJobId) return;

    startBtn.disabled = true;
    startBtn.innerHTML = '⏳ Starting...';

    try {
        const resp = await fetch(`${API_BASE}/start/${currentJobId}`, { method: 'POST' });
        const data = await resp.json();

        if (!resp.ok) {
            showAlert(data.error || 'Failed to start');
            startBtn.disabled = false;
            startBtn.innerHTML = '🚀 Start Processing';
            return;
        }

        // Switch to processing view
        uploadActions.style.display = 'none';
        processingSection.classList.add('visible');
        resultsSection.classList.add('visible');
        isPaused = false;

        // Connect to SSE
        connectSSE(currentJobId);

    } catch (err) {
        showAlert('Error: ' + err.message);
        startBtn.disabled = false;
        startBtn.innerHTML = '🚀 Start Processing';
    }
});

// ─── SSE Connection ─────────────────────────────────────────────
function connectSSE(jobId) {
    if (eventSource) eventSource.close();

    logConsole.innerHTML = '';
    addLogLine('INFO', 'Connecting to server...');

    eventSource = new EventSource(`${API_BASE}/events/${jobId}`);

    eventSource.onmessage = (e) => {
        try {
            const event = JSON.parse(e.data);
            handleSSEEvent(event);
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    };

    eventSource.onerror = () => {
        addLogLine('WARNING', 'Connection interrupted. Attempting to reconnect...');
    };
}

function handleSSEEvent(event) {
    switch (event.type) {
        case 'progress':
            updateProgress(event.data);
            break;
        case 'log':
            addLogLine(event.data.level, event.data.message);
            break;
        case 'status':
            updateJobStatus(event.data.status);
            break;
        case 'complete':
            onProcessingComplete(event.data);
            break;
        case 'error':
            addLogLine('ERROR', event.data.message);
            break;
        case 'done':
            if (eventSource) eventSource.close();
            break;
    }
}

function updateProgress(data) {
    const pct = totalRows > 0 ? Math.round((data.processed / totalRows) * 100) : 0;
    progressBar.style.width = pct + '%';
    progressPercent.textContent = pct + '%';
    statProcessed.textContent = data.processed;
    statActive.textContent = data.active;
    statInactive.textContent = data.inactive;
    statNotFound.textContent = data.not_found;

    if (data.current) {
        currentItem.style.display = 'flex';
        currentCollege.textContent = `Processing: ${data.current}`;
    }

    if (data.eta_seconds > 0) {
        etaInfo.textContent = `ETA: ${formatTime(data.eta_seconds)} • ${data.rate}/min`;
    }

    // Live-refresh results (every 5 processed)
    if (data.processed % 5 === 0 || data.processed === totalRows) {
        loadResults();
    }
}

function updateJobStatus(status) {
    if (status === 'paused') {
        isPaused = true;
        pauseBtn.style.display = 'none';
        resumeBtn.style.display = '';
        currentCollege.textContent = 'Paused';
    } else if (status === 'processing') {
        isPaused = false;
        pauseBtn.style.display = '';
        resumeBtn.style.display = 'none';
    }
}

function onProcessingComplete(data) {
    if (eventSource) eventSource.close();

    progressBar.style.width = '100%';
    progressPercent.textContent = '100%';
    currentItem.style.display = 'none';
    pauseBtn.style.display = 'none';
    resumeBtn.style.display = 'none';
    cancelBtn.style.display = 'none';

    addLogLine('INFO', `✅ ${data.message}`);
    loadResults();
    footerTime.textContent = `Completed in ${formatTime(data.elapsed)}`;
}

// ─── Pause / Resume / Cancel ────────────────────────────────────
pauseBtn.addEventListener('click', async () => {
    if (!currentJobId) return;
    await fetch(`${API_BASE}/pause/${currentJobId}`, { method: 'POST' });
});

resumeBtn.addEventListener('click', async () => {
    if (!currentJobId) return;
    await fetch(`${API_BASE}/resume/${currentJobId}`, { method: 'POST' });
});

cancelBtn.addEventListener('click', async () => {
    if (!currentJobId) return;
    if (!confirm('Cancel processing? Progress will be saved.')) return;
    await fetch(`${API_BASE}/cancel/${currentJobId}`, { method: 'POST' });
    if (eventSource) eventSource.close();
    addLogLine('WARNING', 'Processing cancelled');
    currentItem.style.display = 'none';
    loadResults();
});

// ─── Results ────────────────────────────────────────────────────
async function loadResults() {
    if (!currentJobId) return;

    const status = statusFilter.value;
    const search = searchInput.value.trim();
    const params = new URLSearchParams();
    if (status !== 'all') params.set('status', status);
    if (search) params.set('search', search);

    try {
        const resp = await fetch(`${API_BASE}/results/${currentJobId}?${params}`);
        const data = await resp.json();
        renderResults(data.colleges);
    } catch (err) {
        console.error('Failed to load results:', err);
    }
}

function renderResults(colleges) {
    if (!colleges || colleges.length === 0) {
        resultsBody.innerHTML = '';
        emptyResults.style.display = '';
        return;
    }

    emptyResults.style.display = 'none';
    resultsBody.innerHTML = colleges.map((c, i) => `
        <tr>
            <td>${i + 1}</td>
            <td title="${esc(c.college_name)}">${esc(c.college_name)}</td>
            <td>${esc(c.state)}</td>
            <td>${esc(c.district)}</td>
            <td>${c.found_website || c.original_website ?
                `<a href="${esc(c.found_website || c.original_website)}" target="_blank" style="color:var(--accent-secondary)">${truncate(c.found_website || c.original_website, 25)}</a>` :
                '<span class="text-muted">—</span>'}</td>
            <td>${esc(c.extracted_phone) || '—'}</td>
            <td>${c.extracted_email && c.extracted_email !== 'Not Found' ?
                `<a href="mailto:${esc(c.extracted_email)}" style="color:var(--accent-secondary)">${esc(c.extracted_email)}</a>` :
                '<span class="text-muted">—</span>'}</td>
            <td>${esc(c.extracted_principal) || '—'}</td>
            <td>${statusBadge(c.status)}</td>
        </tr>
    `).join('');
}

function statusBadge(status) {
    const map = {
        'Active': 'badge-active',
        'Inactive': 'badge-inactive',
        'Not Found': 'badge-not-found',
        'Pending': 'badge-pending',
    };
    const cls = map[status] || 'badge-pending';
    return `<span class="badge ${cls}">${esc(status || 'Pending')}</span>`;
}

// Filter and search listeners
let searchTimeout;
searchInput.addEventListener('input', () => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(loadResults, 300);
});
statusFilter.addEventListener('change', loadResults);

// ─── Download ───────────────────────────────────────────────────
downloadBtn.addEventListener('click', () => {
    if (!currentJobId) return;
    window.location.href = `${API_BASE}/download/${currentJobId}`;
});

// ─── Log Console ────────────────────────────────────────────────
function addLogLine(level, message) {
    const time = new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const line = document.createElement('div');
    line.className = 'log-line';
    line.innerHTML = `
        <span class="log-time">${time}</span>
        <span class="log-level ${level}">${level}</span>
        <span class="log-msg">${esc(message)}</span>
    `;
    logConsole.appendChild(line);
    logConsole.scrollTop = logConsole.scrollHeight;
}

// ─── Utilities ──────────────────────────────────────────────────
function esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}

function truncate(str, len) {
    if (!str) return '';
    return str.length > len ? str.substring(0, len) + '...' : str;
}

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatTime(seconds) {
    if (seconds < 60) return `${seconds}s`;
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (mins < 60) return `${mins}m ${secs}s`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}h ${mins % 60}m`;
}

function showAlert(message) {
    alert(message);
}

// Footer timestamp
footerTime.textContent = `Ready • ${new Date().toLocaleString()}`;
