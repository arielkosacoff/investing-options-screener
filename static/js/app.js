let currentResults = [];

document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
    loadCacheStatus();

    document.getElementById('runScreener').addEventListener('click', runScreenerFromConfig);
    document.getElementById('loadCached').addEventListener('click', loadCachedResults);
    document.getElementById('clearCache').addEventListener('click', clearCache);

    // Configuration modal handlers
    document.getElementById('showConfig').addEventListener('click', showConfigModal);
    document.querySelector('.close').addEventListener('click', closeConfigModal);
    document.getElementById('closeConfig').addEventListener('click', closeConfigModal);
    document.getElementById('configForm').addEventListener('submit', saveConfiguration);
    document.getElementById('resetConfig').addEventListener('click', resetConfiguration);

    // Show/hide custom tickers input based on universe selection
    document.getElementById('universeConfigSelect').addEventListener('change', toggleCustomTickersInput);

    // Close modal when clicking outside
    window.addEventListener('click', (event) => {
        const modal = document.getElementById('configModal');
        if (event.target === modal) {
            closeConfigModal();
        }
    });
});

async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();

        let universeDisplay = data.STOCK_UNIVERSE;
        if (data.STOCK_UNIVERSE === 'SP500') universeDisplay = 'S&P 500';
        else if (data.STOCK_UNIVERSE === 'NASDAQ100') universeDisplay = 'NASDAQ-100';
        else if (data.STOCK_UNIVERSE === 'RUSSELL1000') universeDisplay = 'Russell 1000';
        else if (data.STOCK_UNIVERSE === 'CUSTOM' && data.CUSTOM_TICKERS) {
            universeDisplay = `Custom: ${data.CUSTOM_TICKERS}`;
        }

        const configHtml = `
            <div><strong>Stock Universe:</strong> ${universeDisplay}</div>
            <div><strong>Maximum Stock 52W Percentile:</strong> ${(data.STOCK_52W_PERCENTILE_MAX * 100).toFixed(0)}%</div>
            <div><strong>Minimum Annualized Premium Yield:</strong> ${(data.MIN_ANNUALIZED_PREMIUM_YIELD * 100).toFixed(0)}%</div>
            <div><strong>Cache Duration:</strong> ${data.cache_duration_hours} hours</div>
            <div><strong>Sector/Market Requirement:</strong> Must be stronger than stock (higher 52W percentile)</div>
        `;

        document.getElementById('configContent').innerHTML = configHtml;
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

async function loadCacheStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        if (data.cached && data.cache_valid) {
            const cacheTime = new Date(data.timestamp).toLocaleString();
            const expiresAt = new Date(data.expires_at).toLocaleString();

            document.getElementById('cacheInfo').innerHTML = `
                <strong>Cached Results Available:</strong> ${data.num_results} opportunities found<br>
                <strong>Generated:</strong> ${cacheTime}<br>
                <strong>Expires:</strong> ${expiresAt}
            `;
        } else {
            document.getElementById('cacheInfo').innerHTML = 'No cached results available';
        }
    } catch (error) {
        console.error('Error loading cache status:', error);
    }
}

async function runScreening(tickers, universe) {
    setStatus('Running screening... This may take several minutes.', true);
    disableButtons(true);

    try {
        const response = await fetch('/api/screen', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ tickers: tickers, universe: universe }),
        });

        const data = await response.json();

        if (data.success) {
            currentResults = data.results;
            displayResults(data.results);
            setStatus(`Screening complete! Found ${data.count} opportunities.`, false);
            loadCacheStatus();
            loadSavedResults();
        } else {
            setStatus(`Error: ${data.error || data.message}`, false);
        }
    } catch (error) {
        setStatus(`Error: ${error.message}`, false);
    } finally {
        disableButtons(false);
    }
}

function runCustomScreening() {
    const tickerInput = document.getElementById('tickerInput').value.trim();

    if (!tickerInput) {
        alert('Please enter at least one ticker symbol');
        return;
    }

    runScreening(tickerInput);
}

async function loadCachedResults() {
    setStatus('Loading cached results...', true);

    try {
        const response = await fetch('/api/results?use_cache=true');
        const data = await response.json();

        if (data.success && data.results.length > 0) {
            currentResults = data.results;
            displayResults(data.results);
            setStatus(`Loaded ${data.count} cached results.`, false);
        } else {
            setStatus('No cached results available. Please run a new screening.', false);
        }
    } catch (error) {
        setStatus(`Error: ${error.message}`, false);
    }
}

async function clearCache() {
    if (!confirm('Are you sure you want to clear the cache?')) {
        return;
    }

    try {
        const response = await fetch('/api/clear-cache', { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            setStatus('Cache cleared successfully.', false);
            loadCacheStatus();
            document.getElementById('resultsContainer').style.display = 'none';
        } else {
            setStatus(`Error: ${data.error}`, false);
        }
    } catch (error) {
        setStatus(`Error: ${error.message}`, false);
    }
}


function displayResults(results) {
    if (results.length === 0) {
        document.getElementById('resultsContainer').style.display = 'none';
        return;
    }

    document.getElementById('resultsContainer').style.display = 'block';
    document.getElementById('resultsSummary').textContent = `Found ${results.length} opportunities`;

    // Get target premium from config (default 10,000)
    fetch('/api/config')
        .then(res => res.json())
        .then(config => {
            const targetPremium = config.TARGET_PREMIUM_THOUSANDS ? config.TARGET_PREMIUM_THOUSANDS * 1000 : 10000;

            const tbody = document.getElementById('resultsBody');
            tbody.innerHTML = results.map(result => {
                const contracts = Math.ceil(targetPremium / (result.Premium * 100));
                const totalPremium = contracts * result.Premium * 100;

                const formatValue = (val) => val != null ? val : 'N/A';
                const formatNum = (val) => val != null ? val.toFixed(1) : 'N/A';
                const formatCurrency = (val) => val != null ? `$${val.toFixed(2)}` : 'N/A';
                const formatPct = (val) => val != null ? `${(val * 100).toFixed(1)}%` : 'N/A';
                const formatLargeCurrency = (val) => val != null ? `$${val.toFixed(0)}M` : 'N/A';

                const lateralDisplay = result.Is_Lateral
                    ? `Yes (${(result.ATR_Pct * 100).toFixed(2)}% < ${(result.Lateral_Threshold * 100).toFixed(1)}%)`
                    : `No (${(result.ATR_Pct * 100).toFixed(2)}% >= ${(result.Lateral_Threshold * 100).toFixed(1)}%)`;

                return `
                    <tr>
                        <td class="ticker-cell">${result.Ticker}</td>
                        <td>${formatCurrency(result.Price)}</td>
                        <td>${formatValue(result.Industry)}</td>
                        <td>${formatValue(result.Sector)}</td>
                        <td>${formatValue(result.Sector_ETF)}</td>
                        <td>${formatPct(result.Stock_52W_Pct)}</td>
                        <td>${formatCurrency(result['52W_High'])}</td>
                        <td>${formatCurrency(result['52W_Low'])}</td>
                        <td>${formatNum(result.Dist_From_High_Pct)}%</td>
                        <td>${formatNum(result.Dist_From_Low_Pct)}%</td>
                        <td>${formatPct(result.Sector_52W_Pct)}</td>
                        <td>${formatPct(result.Market_52W_Pct)}</td>
                        <td>${formatNum(result.PE_Ratio)}</td>
                        <td>${formatNum(result.Sector_PE)}</td>
                        <td>${formatNum(result.Market_PE)}</td>
                        <td>${formatLargeCurrency(result.Market_Cap_M)}</td>
                        <td>${formatLargeCurrency(result.Avg_Volume_USD_M)}</td>
                        <td>${(result.ATR_Pct * 100).toFixed(2)}%</td>
                        <td>${lateralDisplay}</td>
                        <td>${formatCurrency(result.Put_Strike)}</td>
                        <td>${result.DTE}</td>
                        <td>${formatCurrency(result.Bid)}</td>
                        <td>${formatCurrency(result.Ask)}</td>
                        <td>${formatCurrency(result.Bid_Ask_Spread)}</td>
                        <td>${formatCurrency(result.Premium)}</td>
                        <td class="positive">${formatPct(result.Annualized_Yield)}</td>
                        <td>${contracts} (${formatCurrency(totalPremium)})</td>
                        <td class="links-cell">
                            <a href="${result.Chart_Link}" target="_blank" class="link-btn">Chart</a>
                            <a href="${result.Options_Link}" target="_blank" class="link-btn">Options</a>
                        </td>
                    </tr>
                `;
            }).join('');
        })
        .catch(error => {
            console.error('Error loading config for display:', error);
            // Fallback to default 10k if config fails
            const targetPremium = 10000;
            const tbody = document.getElementById('resultsBody');
            tbody.innerHTML = results.map(result => {
                const contracts = Math.ceil(targetPremium / (result.Premium * 100));
                const totalPremium = contracts * result.Premium * 100;

                // ... (same display code)
            }).join('');
        });
}


function setStatus(message, showSpinner) {
    document.getElementById('statusText').textContent = message;
    document.getElementById('spinner').style.display = showSpinner ? 'block' : 'none';
}

function disableButtons(disabled) {
    document.getElementById('runScreener').disabled = disabled;
    document.getElementById('loadCached').disabled = disabled;
    document.getElementById('clearCache').disabled = disabled;
}

// Run screener based on saved configuration
async function runScreenerFromConfig() {
    try {
        const response = await fetch('/api/config');
        const config = await response.json();

        let tickers = null;
        let universe = config.STOCK_UNIVERSE;

        // If custom universe, use the custom tickers
        if (universe === 'CUSTOM' && config.CUSTOM_TICKERS) {
            tickers = config.CUSTOM_TICKERS;
            universe = null;
        }

        runScreening(tickers, universe);
    } catch (error) {
        setStatus(`Error loading configuration: ${error.message}`, false);
    }
}

// Toggle custom tickers input visibility
function toggleCustomTickersInput() {
    const universeSelect = document.getElementById('universeConfigSelect');
    const customTickersGroup = document.getElementById('customTickersGroup');
    const customTickersInput = document.getElementById('customTickersInput');

    if (universeSelect.value === 'CUSTOM') {
        customTickersGroup.style.display = 'block';
        customTickersInput.required = true;
    } else {
        customTickersGroup.style.display = 'none';
        customTickersInput.required = false;
    }
}

// Configuration Modal Functions
function showConfigModal() {
    const modal = document.getElementById('configModal');
    fetch('/api/config')
        .then(res => res.json())
        .then(config => {
            // Populate form with current values
            const form = document.getElementById('configForm');
            for (const key in config) {
                const input = form.elements[key];
                if (input && key !== 'cache_duration_hours') {
                    input.value = config[key];
                }
            }
            // Toggle custom tickers visibility based on current selection
            toggleCustomTickersInput();
            modal.style.display = 'block';
        })
        .catch(error => {
            console.error('Error loading config:', error);
            alert('Failed to load configuration');
        });
}

function closeConfigModal() {
    document.getElementById('configModal').style.display = 'none';
}

async function saveConfiguration(event) {
    event.preventDefault();

    const formData = new FormData(document.getElementById('configForm'));
    const config = {};

    // Convert form data to config object with proper types
    for (const [key, value] of formData.entries()) {
        if (key === 'STOCK_UNIVERSE' || key === 'CUSTOM_TICKERS') {
            config[key] = value;
        } else if (key.includes('PERCENTILE') || key.includes('DISCOUNT') || key.includes('YIELD') || key.includes('THRESHOLD') || key === 'TARGET_PREMIUM_THOUSANDS') {
            config[key] = parseFloat(value);
        } else {
            config[key] = parseInt(value);
        }
    }

    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(config),
        });

        const result = await response.json();

        if (result.success) {
            alert('Configuration saved successfully! Please restart the application for changes to take effect.');
            closeConfigModal();
            loadConfig();
        } else {
            alert(`Error saving configuration: ${result.error}`);
        }
    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

async function resetConfiguration() {
    if (!confirm('Are you sure you want to reset configuration to defaults? This will restart the application.')) {
        return;
    }

    try {
        const response = await fetch('/api/config/reset', {
            method: 'POST',
        });

        const result = await response.json();

        if (result.success) {
            alert('Configuration reset to defaults! Please restart the application.');
            closeConfigModal();
            loadConfig();
        } else {
            alert(`Error resetting configuration: ${result.error}`);
        }
    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}
