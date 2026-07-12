(function () {
	'use strict';

	var A = window.ANALYTICS;
	if (!A) return;

	var CATS = ['violent', 'property', 'drugs', 'traffic', 'medical', 'harassment', 'other'];

	var CAT_LABELS = {
		violent:    'Violent',
		property:   'Property',
		drugs:      'Drug/Alcohol',
		traffic:    'Traffic',
		medical:    'Medical/Emergency',
		harassment: 'Harassment/Sexual',
		other:      'Other',
	};

	var COLORS = {
		violent:    { bg: 'rgba(198,40,40,0.7)',   border: '#c62828' },
		property:   { bg: 'rgba(230,81,0,0.7)',    border: '#e65100' },
		drugs:      { bg: 'rgba(106,27,154,0.7)',  border: '#6a1b9a' },
		traffic:    { bg: 'rgba(21,101,192,0.7)',  border: '#1565c0' },
		medical:    { bg: 'rgba(0,131,143,0.7)',   border: '#00838f' },
		harassment: { bg: 'rgba(194,24,91,0.7)',   border: '#c2185b' },
		other:      { bg: 'rgba(117,117,117,0.7)', border: '#757575' },
	};

	// ── Heatmap ──────────────────────────────────────────────────
	function buildHeatmap() {
		var container = document.getElementById('heatmap-container');
		if (!container || !A.heatmap) return;

		var DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
		var flat = [];
		for (var d = 0; d < 7; d++) {
			for (var h = 0; h < 24; h++) {
				flat.push(A.heatmap[d][h]);
			}
		}
		var maxVal = Math.max.apply(null, flat) || 1;

		var html = '<div class="heatmap-grid">';
		// Hour header row
		html += '<div class="hm-cell hm-corner"></div>';
		for (var h = 0; h < 24; h++) {
			html += '<div class="hm-cell hm-hour">' + h + '</div>';
		}
		// Data rows
		for (var d = 0; d < 7; d++) {
			html += '<div class="hm-cell hm-day">' + DAYS[d] + '</div>';
			for (var h = 0; h < 24; h++) {
				var val = A.heatmap[d][h];
				var intensity = val / maxVal;
				var alpha = (0.06 + intensity * 0.88).toFixed(2);
				var bg = 'rgba(178,34,34,' + alpha + ')';
				var color = intensity > 0.55 ? '#fff' : '#333';
				var titleText = DAYS[d] + ' ' + h + ':00\u2013' + (h + 1) + ':00 \u2014 ' + val + ' incident' + (val !== 1 ? 's' : '');
				html += '<div class="hm-cell hm-data" style="background:' + bg + ';color:' + color + '" title="' + titleText + '">' + (val > 0 ? val : '') + '</div>';
			}
		}
		html += '</div>';
		container.innerHTML = html;
	}

	// ── Monthly Trends ───────────────────────────────────────────
	function buildMonthlyChart() {
		var ctx = document.getElementById('monthly-chart');
		if (!ctx || !A.monthly_data || !A.monthly_data.labels.length) return;

		var datasets = CATS.map(function (cat) {
			return {
				label: CAT_LABELS[cat],
				data: A.monthly_data.series[cat] || [],
				borderColor: COLORS[cat].border,
				backgroundColor: COLORS[cat].bg,
				borderWidth: 2,
				tension: 0.3,
				fill: false,
				pointRadius: 2,
			};
		});

		new Chart(ctx, {
			type: 'line',
			data: { labels: A.monthly_data.labels, datasets: datasets },
			options: {
				responsive: true,
				maintainAspectRatio: false,
				interaction: { mode: 'index', intersect: false },
				plugins: { legend: { position: 'top' } },
				scales: {
					x: { ticks: { maxTicksLimit: 20, maxRotation: 45 } },
					y: { beginAtZero: true, title: { display: true, text: 'Incidents' } },
				},
			},
		});
	}

	// ── Semester Comparison ──────────────────────────────────────
	function buildSemesterChart() {
		var ctx = document.getElementById('semester-chart');
		if (!ctx || !A.semester_data || !A.semester_data.labels.length) return;

		var datasets = CATS.map(function (cat) {
			return {
				label: CAT_LABELS[cat],
				data: A.semester_data.series[cat] || [],
				backgroundColor: COLORS[cat].bg,
				borderColor: COLORS[cat].border,
				borderWidth: 1,
			};
		});

		new Chart(ctx, {
			type: 'bar',
			data: { labels: A.semester_data.labels, datasets: datasets },
			options: {
				responsive: true,
				maintainAspectRatio: false,
				plugins: { legend: { position: 'top' } },
				scales: {
					x: { stacked: false },
					y: { beginAtZero: true, title: { display: true, text: 'Incidents' } },
				},
			},
		});
	}

	// ── Year-over-Year ───────────────────────────────────────────
	function buildYoYChart() {
		var ctx = document.getElementById('yoy-chart');
		if (!ctx || !A.yoy_totals || !A.yoy_totals.labels.length) return;

		var datasets = CATS.map(function (cat) {
			return {
				label: CAT_LABELS[cat],
				data: A.yoy_totals.series[cat] || [],
				backgroundColor: COLORS[cat].bg,
				borderColor: COLORS[cat].border,
				borderWidth: 1,
			};
		});

		new Chart(ctx, {
			type: 'bar',
			data: { labels: A.yoy_totals.labels, datasets: datasets },
			options: {
				responsive: true,
				maintainAspectRatio: false,
				plugins: { legend: { position: 'top' } },
				scales: {
					x: { stacked: true },
					y: { stacked: true, beginAtZero: true, title: { display: true, text: 'Incidents' } },
				},
			},
		});
	}

	// ── Rate vs. Count (incidents per 1,000 students) ────────────
	function buildPer1000() {
		var ctx = document.getElementById('per1000-chart');
		if (!ctx || !A.per_1000 || !A.per_1000.length) return;

		var rows = A.per_1000;
		new Chart(ctx, {
			data: {
				labels: rows.map(function (r) { return r.label; }),
				datasets: [
					{
						type: 'bar',
						label: 'Incidents (count)',
						data: rows.map(function (r) { return r.incidents; }),
						backgroundColor: 'rgba(117,117,117,0.5)',
						yAxisID: 'y',
					},
					{
						type: 'line',
						label: 'Incidents per 1,000 students (rate)',
						data: rows.map(function (r) { return r.rate_per_1000; }),
						borderColor: '#b22222',
						backgroundColor: 'rgba(178,34,34,0.15)',
						borderWidth: 2,
						tension: 0.2,
						yAxisID: 'y2',
					},
				],
			},
			options: {
				responsive: true,
				maintainAspectRatio: false,
				interaction: { mode: 'index', intersect: false },
				plugins: { legend: { position: 'top' } },
				scales: {
					y:  { beginAtZero: true, position: 'left', title: { display: true, text: 'Incidents' } },
					y2: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false },
					      title: { display: true, text: 'Per 1,000 students' } },
				},
			},
		});

		var tbl = document.getElementById('per1000-table');
		if (tbl) {
			var html = '<table class="mini-table"><thead><tr><th>Year</th><th>Incidents</th>' +
				'<th>Fall enrollment</th><th>Per 1,000 students</th></tr></thead><tbody>';
			rows.forEach(function (r) {
				html += '<tr><td>' + r.label + '</td><td>' + r.incidents + '</td><td>' +
					r.enrollment.toLocaleString() + (r.enrollment_estimated ? '*' : '') + '</td><td>' +
					r.rate_per_1000 + '</td></tr>';
			});
			html += '</tbody></table>';
			var hasEst = rows.some(function (r) { return r.enrollment_estimated; });
			if (hasEst) html += '<p class="chart-desc">* Enrollment not yet published for this year; the latest known figure is used.</p>';
			tbl.innerHTML = html;
		}
	}

	// ── Days from occurrence to report ───────────────────────────
	function buildDaysToReport() {
		var container = document.getElementById('days-to-report-table');
		if (!container || !A.days_to_report || !A.days_to_report.rows) return;

		var html = '<table class="mini-table"><thead><tr><th>Crime type</th><th>Cases</th>' +
			'<th>Median days</th><th>Mean days</th><th>% reported a day or more later</th></tr></thead><tbody>';
		A.days_to_report.rows.forEach(function (r) {
			html += '<tr><td>' + r.crime_type + '</td><td>' + r.n + '</td><td>' + r.median_days +
				'</td><td>' + r.mean_days + '</td><td>' + r.pct_delayed + '%</td></tr>';
		});
		html += '</tbody></table>';
		if (A.days_to_report.skipped_negative) {
			html += '<p class="chart-desc">' + A.days_to_report.skipped_negative +
				' cases where the report date precedes the occurrence date (data-entry errors) were excluded.</p>';
		}
		container.innerHTML = html;
	}

	// ── copy buttons for the "Show your work" R snippets ─────────
	function wireCopyButtons() {
		var btns = document.querySelectorAll('.copy-r-btn');
		Array.prototype.forEach.call(btns, function (btn) {
			btn.addEventListener('click', function () {
				var code = btn.parentElement.querySelector('pre code');
				if (!code) return;
				var done = function () {
					btn.textContent = 'Copied!';
					setTimeout(function () { btn.textContent = 'Copy R code'; }, 1500);
				};
				if (navigator.clipboard && navigator.clipboard.writeText) {
					navigator.clipboard.writeText(code.textContent).then(done, function () {});
				} else {
					var range = document.createRange();
					range.selectNodeContents(code);
					var sel = window.getSelection();
					sel.removeAllRanges();
					sel.addRange(range);
					try { document.execCommand('copy'); done(); } catch (e) {}
					sel.removeAllRanges();
				}
			});
		});
	}

	buildHeatmap();
	buildMonthlyChart();
	buildSemesterChart();
	buildYoYChart();
	buildPer1000();
	buildDaysToReport();
	wireCopyButtons();
})();
