// ============================================
// State Management
// ============================================
const state = {
    currentQuery: '',
    currentSQL: '',
    currentResults: null,
    isLoading: false,
    isEditing: false,
    chartInstance: null,
    currentView: 'table'
};

// ============================================
// DOM Elements
// ============================================
const elements = {
    queryInput: document.getElementById('queryInput'),
    submitBtn: document.getElementById('submitBtn'),
    examplesBtn: document.getElementById('examplesBtn'),
    schemaBtn: document.getElementById('schemaBtn'),
    themeToggle: document.getElementById('themeToggle'),
    loadingState: document.getElementById('loadingState'),
    sqlSection: document.getElementById('sqlSection'),
    sqlCode: document.getElementById('sqlCode'),
    copySqlBtn: document.getElementById('copySqlBtn'),
    editSqlBtn: document.getElementById('editSqlBtn'),
    sqlEditor: document.getElementById('sqlEditor'),
    sqlEditArea: document.getElementById('sqlEditArea'),
    cancelEditBtn: document.getElementById('cancelEditBtn'),
    executeEditBtn: document.getElementById('executeEditBtn'),
    resultsSection: document.getElementById('resultsSection'),
    resultsTable: document.getElementById('resultsTable'),
    resultsChart: document.getElementById('resultsChart'),
    resultCount: document.getElementById('resultCount'),
    exportBtn: document.getElementById('exportBtn'),
    errorMessage: document.getElementById('errorMessage'),
    errorText: document.getElementById('errorText'),
    historyList: document.getElementById('historyList'),
    clearHistoryBtn: document.getElementById('clearHistoryBtn'),
    schemaModal: document.getElementById('schemaModal'),
    schemaContent: document.getElementById('schemaContent'),
    closeSchemaModal: document.getElementById('closeSchemaModal'),
    examplesModal: document.getElementById('examplesModal'),
    examplesContent: document.getElementById('examplesContent'),
    closeExamplesModal: document.getElementById('closeExamplesModal'),
    chartType: document.getElementById('chartType'),
    chartXAxis: document.getElementById('chartXAxis'),
    chartYAxis: document.getElementById('chartYAxis'),
    dataChart: document.getElementById('dataChart')
};

// ============================================
// API Functions
// ============================================
async function processQuery(query) {
    const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Failed to process query');
    }

    // Get response text and replace NaN with null before parsing
    const text = await response.text();
    const sanitized = text.replace(/:\s*NaN/g, ': null');
    return JSON.parse(sanitized);
}

async function executeSQL(sql) {
    const response = await fetch('/api/execute-sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ sql })
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Failed to execute SQL');
    }

    // Get response text and replace NaN with null before parsing
    const text = await response.text();
    const sanitized = text.replace(/:\s*NaN/g, ': null');
    return JSON.parse(sanitized);
}

async function fetchSchema() {
    const response = await fetch('/api/schema');
    if (!response.ok) {
        throw new Error('Failed to fetch schema');
    }
    return await response.json();
}

async function fetchExamples() {
    const response = await fetch('/api/examples');
    if (!response.ok) {
        throw new Error('Failed to fetch examples');
    }
    return await response.json();
}

async function fetchHistory() {
    // Get history from localStorage instead of backend
    const history = localStorage.getItem('nl2sql_history');
    return history ? JSON.parse(history) : [];
}

async function clearHistory() {
    // Clear from localStorage
    localStorage.removeItem('nl2sql_history');
    return { message: 'History cleared' };
}

function saveToHistory(query, sql, numResults) {
    // Get existing history
    const history = JSON.parse(localStorage.getItem('nl2sql_history') || '[]');

    // Add new entry
    history.push({
        query: query,
        sql: sql,
        num_results: numResults,
        timestamp: new Date().toISOString()
    });

    // Keep only last 50 queries
    if (history.length > 50) {
        history.shift();
    }

    // Save back to localStorage
    localStorage.setItem('nl2sql_history', JSON.stringify(history));
}

// ============================================
// UI Functions
// ============================================
function showLoading() {
    state.isLoading = true;
    elements.loadingState.style.display = 'flex';
    elements.sqlSection.style.display = 'none';
    elements.resultsSection.style.display = 'none';
    elements.errorMessage.style.display = 'none';
    elements.submitBtn.disabled = true;
}

function hideLoading() {
    state.isLoading = false;
    elements.loadingState.style.display = 'none';
    elements.submitBtn.disabled = false;
}

function showError(message) {
    elements.errorText.textContent = message;
    elements.errorMessage.style.display = 'flex';
    setTimeout(() => {
        elements.errorMessage.style.display = 'none';
    }, 5000);
}

function displaySQL(sql) {
    state.currentSQL = sql;
    elements.sqlCode.textContent = sql;
    elements.sqlSection.style.display = 'block';
}

function isAggregateResult(results) {
    if (results.length !== 1) return false;
    const firstRow = results[0];
    const keys = Object.keys(firstRow);

    // Check if keys contain aggregate function names
    const aggregateFunctions = ['count', 'sum', 'avg', 'min', 'max', 'average', 'total'];
    return keys.some(key =>
        aggregateFunctions.some(fn => key.toLowerCase().includes(fn))
    );
}

function displayResults(results) {
    state.currentResults = results;
    elements.resultsSection.style.display = 'block';
    elements.resultCount.textContent = `${results.length} row${results.length !== 1 ? 's' : ''}`;

    if (results.length === 0) {
        elements.resultsTable.innerHTML = '<div class="empty-state"><p>No results found</p></div>';
        return;
    }

    // Check if this is an aggregate result
    if (isAggregateResult(results)) {
        displayAggregateResults(results[0]);
    } else {
        displayTableResults(results);
    }
}

function displayAggregateResults(result) {
    const container = document.createElement('div');
    container.className = 'aggregate-result';

    Object.entries(result).forEach(([key, value]) => {
        const card = document.createElement('div');
        card.className = 'aggregate-card';

        const label = document.createElement('div');
        label.className = 'aggregate-label';
        label.textContent = formatColumnName(key);

        const valueDiv = document.createElement('div');
        valueDiv.className = 'aggregate-value';
        valueDiv.textContent = formatValue(value);

        card.appendChild(label);
        card.appendChild(valueDiv);
        container.appendChild(card);
    });

    elements.resultsTable.innerHTML = '';
    elements.resultsTable.appendChild(container);
}

function displayTableResults(results) {
    const table = document.createElement('table');
    table.className = 'data-table';

    // Create header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    const columns = Object.keys(results[0]);

    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = formatColumnName(col);
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create body
    const tbody = document.createElement('tbody');
    results.forEach((row, index) => {
        const tr = document.createElement('tr');
        tr.style.animationDelay = `${index * 0.02}s`;

        columns.forEach(col => {
            const td = document.createElement('td');
            td.textContent = formatValue(row[col]);
            tr.appendChild(td);
        });

        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    elements.resultsTable.innerHTML = '';
    elements.resultsTable.appendChild(table);
}

function formatColumnName(name) {
    return name
        .replace(/_/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase());
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return 'NULL';
    }
    if (typeof value === 'number') {
        // Format numbers with commas
        return value.toLocaleString();
    }
    if (typeof value === 'boolean') {
        return value ? 'TRUE' : 'FALSE';
    }
    return String(value);
}

async function loadHistory() {
    try {
        const history = await fetchHistory();

        if (history.length === 0) {
            elements.historyList.innerHTML = `
                <div class="empty-state">
                    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
                    </svg>
                    <p>No queries yet</p>
                </div>
            `;
            return;
        }

        elements.historyList.innerHTML = '';
        history.reverse().forEach((item, index) => {
            const historyItem = document.createElement('div');
            historyItem.className = 'history-item';
            historyItem.style.animationDelay = `${index * 0.05}s`;

            // Format timestamp if available
            let timeStr = '';
            if (item.timestamp) {
                const date = new Date(item.timestamp);
                const now = new Date();
                const diffMs = now - date;
                const diffMins = Math.floor(diffMs / 60000);
                const diffHours = Math.floor(diffMs / 3600000);
                const diffDays = Math.floor(diffMs / 86400000);

                if (diffMins < 1) {
                    timeStr = 'Just now';
                } else if (diffMins < 60) {
                    timeStr = `${diffMins}m ago`;
                } else if (diffHours < 24) {
                    timeStr = `${diffHours}h ago`;
                } else if (diffDays < 7) {
                    timeStr = `${diffDays}d ago`;
                } else {
                    timeStr = date.toLocaleDateString();
                }
            }

            historyItem.innerHTML = `
                <div class="history-item-content">
                    <div class="history-item-query">${escapeHtml(item.query)}</div>
                    <div class="history-item-sql">${escapeHtml(item.sql)}</div>
                    <div class="history-item-meta">
                        <span>${item.num_results} result${item.num_results !== 1 ? 's' : ''}</span>
                        ${timeStr ? `<span class="history-item-time">• ${timeStr}</span>` : ''}
                    </div>
                </div>
                <button class="history-item-delete" title="Delete this query">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="3 6 5 6 21 6"></polyline>
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                    </svg>
                </button>
            `;

            // Click on content area to use query
            const contentArea = historyItem.querySelector('.history-item-content');
            contentArea.addEventListener('click', () => {
                elements.queryInput.value = item.query;
                elements.queryInput.focus();
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });

            // Click on delete button to remove item
            const deleteBtn = historyItem.querySelector('.history-item-delete');
            deleteBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                deleteHistoryItem(history.length - 1 - index);
            });

            elements.historyList.appendChild(historyItem);
        });
    } catch (error) {
        console.error('Failed to load history:', error);
    }
}

function deleteHistoryItem(index) {
    const history = JSON.parse(localStorage.getItem('nl2sql_history') || '[]');
    history.splice(index, 1);
    localStorage.setItem('nl2sql_history', JSON.stringify(history));
    loadHistory();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function exportToCSV() {
    if (!state.currentResults || state.currentResults.length === 0) {
        showError('No results to export');
        return;
    }

    const results = state.currentResults;
    const headers = Object.keys(results[0]);

    let csv = headers.join(',') + '\n';

    results.forEach(row => {
        const values = headers.map(header => {
            const value = row[header];
            if (value === null || value === undefined) return '';
            const stringValue = String(value);
            // Escape quotes and wrap in quotes if contains comma or quote
            if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
                return '"' + stringValue.replace(/"/g, '""') + '"';
            }
            return stringValue;
        });
        csv += values.join(',') + '\n';
    });

    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `query_results_${Date.now()}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

async function loadSchema() {
    try {
        elements.schemaContent.innerHTML = '<div class="loading-spinner"></div>';
        elements.schemaModal.classList.add('active');

        const schema = await fetchSchema();

        elements.schemaContent.innerHTML = '';

        // Schema format: { table_name: { columns: { col_name: { type, nullable } }, primary_keys, foreign_keys } }
        Object.entries(schema).forEach(([tableName, tableInfo]) => {
            const tableDiv = document.createElement('div');
            tableDiv.className = 'schema-table';

            // Table name header
            const title = document.createElement('h4');
            title.textContent = tableName;
            tableDiv.appendChild(title);

            // Extract columns
            const columns = tableInfo.columns || tableInfo;

            // Create columns container
            const columnsDiv = document.createElement('div');
            columnsDiv.className = 'schema-columns';

            // Check if columns is an object (nested format)
            if (typeof columns === 'object' && !Array.isArray(columns)) {
                // Format: { column_name: { type, nullable, ... } }
                Object.entries(columns).forEach(([colName, colInfo]) => {
                    const columnDiv = document.createElement('div');
                    columnDiv.className = 'schema-column';

                    const nameSpan = document.createElement('span');
                    nameSpan.className = 'schema-column-name';

                    // Add key indicator if it's a primary key
                    const isPrimary = tableInfo.primary_keys && tableInfo.primary_keys.includes(colName);
                    const isForeign = tableInfo.foreign_keys && tableInfo.foreign_keys.includes(colName);

                    let nameText = colName;
                    if (isPrimary) nameText = '🔑 ' + nameText;
                    if (isForeign) nameText = '🔗 ' + nameText;

                    nameSpan.textContent = nameText;

                    const typeSpan = document.createElement('span');
                    typeSpan.className = 'schema-column-type';

                    // Get type and nullable info
                    let typeText = '';
                    if (typeof colInfo === 'object') {
                        typeText = colInfo.type || 'unknown';
                        if (colInfo.nullable === false) {
                            typeText += ' NOT NULL';
                        }
                    } else if (typeof colInfo === 'string') {
                        typeText = colInfo;
                    }

                    typeSpan.textContent = typeText;

                    columnDiv.appendChild(nameSpan);
                    columnDiv.appendChild(typeSpan);
                    columnsDiv.appendChild(columnDiv);
                });
            } else if (Array.isArray(columns)) {
                // Format: [{ name, type }] or similar
                columns.forEach(col => {
                    const columnDiv = document.createElement('div');
                    columnDiv.className = 'schema-column';

                    const nameSpan = document.createElement('span');
                    nameSpan.className = 'schema-column-name';
                    nameSpan.textContent = col.name || col.column_name || 'unknown';

                    const typeSpan = document.createElement('span');
                    typeSpan.className = 'schema-column-type';
                    typeSpan.textContent = col.type || col.data_type || 'unknown';

                    columnDiv.appendChild(nameSpan);
                    columnDiv.appendChild(typeSpan);
                    columnsDiv.appendChild(columnDiv);
                });
            }

            tableDiv.appendChild(columnsDiv);

            // Add additional info section for keys
            if (tableInfo.primary_keys || tableInfo.foreign_keys) {
                const infoDiv = document.createElement('div');
                infoDiv.style.marginTop = 'var(--spacing-sm)';
                infoDiv.style.fontSize = '0.75rem';
                infoDiv.style.color = 'var(--color-text-tertiary)';

                const infoParts = [];
                if (tableInfo.primary_keys && tableInfo.primary_keys.length > 0) {
                    infoParts.push(`Primary: ${tableInfo.primary_keys.join(', ')}`);
                }
                if (tableInfo.foreign_keys && tableInfo.foreign_keys.length > 0) {
                    infoParts.push(`Foreign: ${tableInfo.foreign_keys.join(', ')}`);
                }

                if (infoParts.length > 0) {
                    infoDiv.textContent = infoParts.join(' | ');
                    tableDiv.appendChild(infoDiv);
                }
            }

            elements.schemaContent.appendChild(tableDiv);
        });

        // If no schema data was rendered, show a message
        if (elements.schemaContent.children.length === 0) {
            elements.schemaContent.innerHTML = '<p style="color: var(--color-text-tertiary); text-align: center; padding: 2rem;">No schema information available</p>';
        }
    } catch (error) {
        console.error('Schema loading error:', error);
        elements.schemaContent.innerHTML = `
            <div style="color: var(--color-error); padding: 1rem; text-align: center;">
                <p><strong>Error loading schema</strong></p>
                <p style="font-size: 0.875rem; margin-top: 0.5rem;">${error.message}</p>
            </div>
        `;
    }
}

async function loadExamples() {
    try {
        elements.examplesModal.classList.add('active');

        const examples = await fetchExamples();

        elements.examplesContent.innerHTML = '';

        examples.forEach(category => {
            const categoryDiv = document.createElement('div');
            categoryDiv.className = 'example-category';

            const title = document.createElement('h4');
            title.textContent = category.category;
            categoryDiv.appendChild(title);

            const queriesDiv = document.createElement('div');
            queriesDiv.className = 'example-queries';

            category.queries.forEach(query => {
                const queryDiv = document.createElement('div');
                queryDiv.className = 'example-query';
                queryDiv.textContent = query;

                queryDiv.addEventListener('click', () => {
                    elements.queryInput.value = query;
                    elements.examplesModal.classList.remove('active');
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    elements.queryInput.focus();
                });

                queriesDiv.appendChild(queryDiv);
            });

            categoryDiv.appendChild(queriesDiv);
            elements.examplesContent.appendChild(categoryDiv);
        });
    } catch (error) {
        elements.examplesContent.innerHTML = `<div class="error-message">${error.message}</div>`;
    }
}

// ============================================
// Event Handlers
// ============================================
async function handleQuerySubmit() {
    const query = elements.queryInput.value.trim();

    if (!query) {
        showError('Please enter a query');
        return;
    }

    state.currentQuery = query;
    showLoading();

    try {
        const result = await processQuery(query);
        hideLoading();

        displaySQL(result.sql_query);
        displayResults(result.results);

        // Save to localStorage history
        saveToHistory(query, result.sql_query, result.num_results);
        await loadHistory();
    } catch (error) {
        hideLoading();
        showError(error.message);
    }
}

async function handleSQLEdit() {
    const sql = elements.sqlEditArea.value.trim();

    if (!sql) {
        showError('Please enter SQL query');
        return;
    }

    showLoading();

    try {
        const result = await executeSQL(sql);
        hideLoading();

        state.currentSQL = result.sql_query;
        elements.sqlCode.textContent = result.sql_query;
        elements.sqlEditor.style.display = 'none';
        state.isEditing = false;

        displayResults(result.results);

        // Save edited SQL to history
        saveToHistory('Custom SQL Query', result.sql_query, result.num_results);
        await loadHistory();
    } catch (error) {
        hideLoading();
        showError(error.message);
    }
}

function toggleSQLEditor() {
    state.isEditing = !state.isEditing;

    if (state.isEditing) {
        elements.sqlEditArea.value = state.currentSQL;
        elements.sqlEditor.style.display = 'block';
        elements.sqlEditArea.focus();
    } else {
        elements.sqlEditor.style.display = 'none';
    }
}

function copySQLToClipboard() {
    navigator.clipboard.writeText(state.currentSQL).then(() => {
        const originalText = elements.copySqlBtn.innerHTML;
        elements.copySqlBtn.innerHTML = `
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="20 6 9 17 4 12"></polyline>
            </svg>
        `;
        setTimeout(() => {
            elements.copySqlBtn.innerHTML = originalText;
        }, 2000);
    }).catch(err => {
        showError('Failed to copy to clipboard');
    });
}

async function handleClearHistory() {
    if (confirm('Are you sure you want to clear all query history?')) {
        try {
            await clearHistory();
            await loadHistory();
        } catch (error) {
            showError(error.message);
        }
    }
}

// ============================================
// Theme Toggle Functions
// ============================================
function initTheme() {
    const savedTheme = localStorage.getItem('nl2sql_theme') || 'dark';
    setTheme(savedTheme);
}

function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('nl2sql_theme', theme);

    const sunIcon = elements.themeToggle.querySelector('.sun-icon');
    const moonIcon = elements.themeToggle.querySelector('.moon-icon');

    if (theme === 'light') {
        sunIcon.style.display = 'none';
        moonIcon.style.display = 'block';
    } else {
        sunIcon.style.display = 'block';
        moonIcon.style.display = 'none';
    }

    // Update chart if it exists
    if (state.chartInstance) {
        updateChartTheme();
    }
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
}

// ============================================
// Chart Visualization Functions
// ============================================
function initChart() {
    if (!state.currentResults || state.currentResults.length === 0) {
        return;
    }

    const columns = Object.keys(state.currentResults[0]);

    // Populate axis selectors
    elements.chartXAxis.innerHTML = '<option value="">Select X-Axis</option>';
    elements.chartYAxis.innerHTML = '<option value="">Select Y-Axis (Value)</option>';

    columns.forEach(col => {
        const optionX = document.createElement('option');
        optionX.value = col;
        optionX.textContent = formatColumnName(col);
        elements.chartXAxis.appendChild(optionX);

        // Only numeric columns for Y-axis
        const isNumeric = state.currentResults.some(row =>
            typeof row[col] === 'number' || !isNaN(parseFloat(row[col]))
        );

        if (isNumeric) {
            const optionY = document.createElement('option');
            optionY.value = col;
            optionY.textContent = formatColumnName(col);
            elements.chartYAxis.appendChild(optionY);
        }
    });

    // Auto-select reasonable defaults
    if (columns.length > 0) {
        elements.chartXAxis.value = columns[0];
    }
    if (columns.length > 1) {
        const numericCols = columns.filter(col =>
            state.currentResults.some(row =>
                typeof row[col] === 'number' || !isNaN(parseFloat(row[col]))
            )
        );
        if (numericCols.length > 0) {
            elements.chartYAxis.value = numericCols[0];
            renderChart();
        }
    }
}

function renderChart() {
    const xAxis = elements.chartXAxis.value;
    const yAxis = elements.chartYAxis.value;
    const chartType = elements.chartType.value;

    if (!xAxis || !yAxis) {
        document.querySelector('.chart-empty').style.display = 'flex';
        document.querySelector('.chart-container').style.display = 'none';
        return;
    }

    document.querySelector('.chart-empty').style.display = 'none';
    document.querySelector('.chart-container').style.display = 'block';

    // Destroy existing chart
    if (state.chartInstance) {
        state.chartInstance.destroy();
    }

    // Prepare data
    const labels = state.currentResults.map(row => String(row[xAxis]));
    const data = state.currentResults.map(row => {
        const val = row[yAxis];
        return typeof val === 'number' ? val : parseFloat(val) || 0;
    });

    // Get theme colors
    const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
    const colors = generateColors(data.length, isDark);

    const config = {
        type: chartType,
        data: {
            labels: labels,
            datasets: [{
                label: formatColumnName(yAxis),
                data: data,
                backgroundColor: chartType === 'line' ?
                    `rgba(0, 217, 255, 0.1)` : colors.background,
                borderColor: chartType === 'line' ?
                    '#00d9ff' : colors.border,
                borderWidth: 2,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: ['pie', 'doughnut', 'polarArea', 'radar'].includes(chartType),
                    labels: {
                        color: isDark ? '#f0f4f8' : '#0f172a',
                        font: {
                            family: "'JetBrains Mono', monospace"
                        }
                    }
                },
                tooltip: {
                    backgroundColor: isDark ? '#202b3d' : '#f1f5f9',
                    titleColor: isDark ? '#f0f4f8' : '#0f172a',
                    bodyColor: isDark ? '#99aab8' : '#475569',
                    borderColor: isDark ? '#00d9ff' : '#0891b2',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${context.parsed.y !== undefined ? context.parsed.y.toLocaleString() : context.parsed.toLocaleString()}`;
                        }
                    }
                }
            },
            scales: ['pie', 'doughnut', 'polarArea', 'radar'].includes(chartType) ? {} : {
                x: {
                    grid: {
                        color: isDark ? 'rgba(255, 255, 255, 0.05)' : 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        color: isDark ? '#99aab8' : '#475569',
                        font: {
                            family: "'JetBrains Mono', monospace"
                        }
                    }
                },
                y: {
                    grid: {
                        color: isDark ? 'rgba(255, 255, 255, 0.05)' : 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        color: isDark ? '#99aab8' : '#475569',
                        font: {
                            family: "'JetBrains Mono', monospace"
                        }
                    },
                    beginAtZero: true
                }
            }
        }
    };

    const ctx = elements.dataChart.getContext('2d');
    state.chartInstance = new Chart(ctx, config);
}

function generateColors(count, isDark) {
    const baseColors = isDark ? [
        '#00d9ff', '#00ffc8', '#7c3aed', '#ff4757', '#ffa502',
        '#00ff88', '#ff6b9d', '#4ecdc4', '#ffe66d', '#a8e6cf'
    ] : [
        '#0891b2', '#059669', '#7c3aed', '#dc2626', '#ea580c',
        '#10b981', '#ec4899', '#14b8a6', '#f59e0b', '#22c55e'
    ];

    const background = [];
    const border = [];

    for (let i = 0; i < count; i++) {
        const color = baseColors[i % baseColors.length];
        background.push(color + '40'); // Add alpha for background
        border.push(color);
    }

    return { background, border };
}

function updateChartTheme() {
    if (state.chartInstance && state.currentResults) {
        renderChart();
    }
}

function switchView(view) {
    state.currentView = view;

    // Update view toggle buttons
    document.querySelectorAll('.view-toggle-btn').forEach(btn => {
        if (btn.dataset.view === view) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });

    // Show/hide views
    if (view === 'table') {
        elements.resultsTable.classList.add('active');
        elements.resultsChart.classList.remove('active');
    } else {
        elements.resultsTable.classList.remove('active');
        elements.resultsChart.classList.add('active');
        initChart();
    }
}

// ============================================
// Event Listeners
// ============================================
elements.submitBtn.addEventListener('click', handleQuerySubmit);

elements.queryInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        handleQuerySubmit();
    }
});

elements.copySqlBtn.addEventListener('click', copySQLToClipboard);

elements.editSqlBtn.addEventListener('click', () => {
    toggleSQLEditor();
});

elements.cancelEditBtn.addEventListener('click', () => {
    toggleSQLEditor();
});

elements.executeEditBtn.addEventListener('click', handleSQLEdit);

elements.exportBtn.addEventListener('click', exportToCSV);

elements.schemaBtn.addEventListener('click', loadSchema);

elements.closeSchemaModal.addEventListener('click', () => {
    elements.schemaModal.classList.remove('active');
});

elements.examplesBtn.addEventListener('click', loadExamples);

elements.closeExamplesModal.addEventListener('click', () => {
    elements.examplesModal.classList.remove('active');
});

// Close modals on overlay click
elements.schemaModal.querySelector('.modal-overlay').addEventListener('click', () => {
    elements.schemaModal.classList.remove('active');
});

elements.examplesModal.querySelector('.modal-overlay').addEventListener('click', () => {
    elements.examplesModal.classList.remove('active');
});

// Close modals on ESC key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        elements.schemaModal.classList.remove('active');
        elements.examplesModal.classList.remove('active');
    }
});

elements.clearHistoryBtn.addEventListener('click', handleClearHistory);

// Theme toggle
elements.themeToggle.addEventListener('click', toggleTheme);

// View toggle
document.querySelectorAll('.view-toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        switchView(btn.dataset.view);
    });
});

// Chart controls
elements.chartType.addEventListener('change', renderChart);
elements.chartXAxis.addEventListener('change', renderChart);
elements.chartYAxis.addEventListener('change', renderChart);

// ============================================
// Initialize
// ============================================
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    loadHistory();
    elements.queryInput.focus();
});