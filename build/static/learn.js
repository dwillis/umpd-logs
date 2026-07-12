(function () {
	'use strict';

	var L = window.LEARN;

	// ── shared helpers ───────────────────────────────────────────
	// Same conventions as learn_stats.py and R: median as R's median(),
	// sampleSd with an n-1 denominator like R's sd().

	function mean(xs) {
		if (!xs.length) return 0;
		var s = 0;
		for (var i = 0; i < xs.length; i++) s += xs[i];
		return s / xs.length;
	}

	function median(xs) {
		if (!xs.length) return 0;
		var a = xs.slice().sort(function (x, y) { return x - y; });
		var mid = Math.floor(a.length / 2);
		return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
	}

	function mode(xs) {
		var freq = {}, best = null, bestN = 0;
		for (var i = 0; i < xs.length; i++) {
			freq[xs[i]] = (freq[xs[i]] || 0) + 1;
			if (freq[xs[i]] > bestN) { bestN = freq[xs[i]]; best = xs[i]; }
		}
		return { value: best, count: bestN };
	}

	function sampleSd(xs) {
		if (xs.length < 2) return 0;
		var m = mean(xs), s = 0;
		for (var i = 0; i < xs.length; i++) s += (xs[i] - m) * (xs[i] - m);
		return Math.sqrt(s / (xs.length - 1)); // n-1: matches R's sd()
	}

	function rollingMean(xs, window) {
		var out = new Array(xs.length).fill(null);
		var running = 0;
		for (var i = 0; i < xs.length; i++) {
			running += xs[i];
			if (i >= window) running -= xs[i - window];
			if (i >= window - 1) out[i] = running / window;
		}
		return out;
	}

	function fmt1(x) { return (Math.round(x * 10) / 10).toFixed(1); }

	function addDays(iso, n) {
		var d = new Date(iso + 'T00:00:00');
		d.setDate(d.getDate() + n);
		return d.toISOString().slice(0, 10);
	}

	// Daily values with ISO date labels; the final (partial) day is dropped.
	function dailyData() {
		var counts = L.daily.counts.slice(0, -1);
		var labels = new Array(counts.length);
		for (var i = 0; i < counts.length; i++) labels[i] = addDays(L.daily.start, i);
		return { labels: labels, values: counts };
	}

	function weeklyData() {
		var counts = L.weekly.counts;
		var labels = new Array(counts.length);
		for (var i = 0; i < counts.length; i++) labels[i] = addDays(L.weekly.start_monday, i * 7);
		return { labels: labels, values: counts };
	}

	function tail(data, n) {
		return { labels: data.labels.slice(-n), values: data.values.slice(-n) };
	}

	// ── copy buttons (shared with any page that has .r-snippet) ──
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

	// ── Lab 1: which average? ────────────────────────────────────
	function initAverages() {
		var root = document.getElementById('lab-averages');
		if (!root || !L) return;

		var elGran = document.getElementById('avg-granularity');
		var elRange = document.getElementById('avg-range');
		var elOut = document.getElementById('avg-outliers');
		var chart = null;

		function currentValues() {
			var gran = elGran.value;
			var data = gran === 'week' ? weeklyData() : dailyData();
			var range = elRange.value;
			if (range === '12m') data = tail(data, gran === 'week' ? 52 : 365);
			if (range === '24m') data = tail(data, gran === 'week' ? 104 : 730);
			var values = data.values.slice();
			var removed = 0;
			if (elOut.checked) {
				var sorted = values.slice().sort(function (a, b) { return b - a; });
				removed = Math.max(1, Math.ceil(values.length * 0.01));
				var cutoff = sorted[removed - 1];
				// drop exactly `removed` of the largest values
				var toDrop = removed;
				values = values.filter(function (v) {
					if (v >= cutoff && toDrop > 0) { toDrop--; return false; }
					return true;
				});
			}
			return { values: values, removed: removed, gran: gran };
		}

		function render() {
			var cur = currentValues();
			var xs = cur.values;
			var m = mean(xs), md = median(xs), mo = mode(xs);

			document.getElementById('avg-mean').textContent = fmt1(m);
			document.getElementById('avg-median').textContent = fmt1(md);
			document.getElementById('avg-mode').textContent =
				mo.count > 1 ? mo.value : 'none';

			// histogram bins: width 1 for days, 5 for weeks
			var binW = cur.gran === 'week' ? 5 : 1;
			var maxV = Math.max.apply(null, xs.concat([1]));
			var nBins = Math.floor(maxV / binW) + 1;
			var bins = new Array(nBins).fill(0);
			xs.forEach(function (v) { bins[Math.floor(v / binW)]++; });
			var labels = [], colors = [];
			var meanBin = Math.floor(m / binW), medianBin = Math.floor(md / binW);
			for (var b = 0; b < nBins; b++) {
				labels.push(binW === 1 ? String(b) : (b * binW) + '–' + (b * binW + binW - 1));
				if (b === meanBin && b === medianBin) colors.push('rgba(106,27,154,0.8)');
				else if (b === meanBin) colors.push('rgba(198,40,40,0.8)');
				else if (b === medianBin) colors.push('rgba(21,101,192,0.8)');
				else colors.push('rgba(150,150,150,0.55)');
			}

			var note = meanBin === medianBin
				? 'The purple bar holds both the mean (' + fmt1(m) + ') and the median (' + fmt1(md) + ') — close, but check the exact values above.'
				: 'The red bar holds the mean (' + fmt1(m) + '); the blue bar holds the median (' + fmt1(md) + ').';
			if (cur.removed) {
				note += ' Excluded the ' + cur.removed + ' busiest ' +
					(cur.gran === 'week' ? 'weeks' : 'days') + ' — watch which average moved.';
			}
			if (L.daily.dropped_out_of_range) {
				note += ' (Data cleaning: ' + L.daily.dropped_out_of_range +
					' rows with impossible dates are excluded from all of this — real datasets are messy.)';
			}
			document.getElementById('avg-note').textContent = note;

			var ds = {
				label: 'Number of ' + (cur.gran === 'week' ? 'weeks' : 'days'),
				data: bins,
				backgroundColor: colors,
				borderWidth: 0,
			};
			if (chart) {
				chart.data.labels = labels;
				chart.data.datasets = [ds];
				chart.update('none');
			} else {
				chart = new Chart(document.getElementById('avg-histogram'), {
					type: 'bar',
					data: { labels: labels, datasets: [ds] },
					options: {
						responsive: true,
						maintainAspectRatio: false,
						plugins: { legend: { display: false } },
						scales: {
							x: { title: { display: true, text: 'Incidents per period' } },
							y: { beginAtZero: true, title: { display: true, text: 'How often' } },
						},
					},
				});
			}
		}

		elGran.addEventListener('change', render);
		elRange.addEventListener('change', render);
		elOut.addEventListener('change', render);
		render();
	}

	// ── Lab 2: moving averages ───────────────────────────────────
	function initMoving() {
		var root = document.getElementById('lab-moving');
		if (!root || !L) return;

		var elRange = document.getElementById('ma-range');
		var elWindow = document.getElementById('ma-window');
		var elLabel = document.getElementById('ma-window-label');
		var chart = null;

		function render() {
			var data = dailyData();
			if (elRange.value === '24m') data = tail(data, 730);
			var w = parseInt(elWindow.value, 10);
			elLabel.textContent = w;

			var raw = {
				label: 'Daily count',
				data: data.values,
				showLine: false,
				pointRadius: 1.5,
				pointBackgroundColor: 'rgba(120,120,120,0.45)',
				borderColor: 'rgba(120,120,120,0.45)',
			};
			var smooth = {
				label: w + '-day moving average',
				data: rollingMean(data.values, w),
				borderColor: '#b22222',
				backgroundColor: 'rgba(178,34,34,0.1)',
				borderWidth: 2,
				pointRadius: 0,
				tension: 0.2,
				spanGaps: false,
			};

			if (chart) {
				chart.data.labels = data.labels;
				chart.data.datasets[0].data = raw.data;
				chart.data.datasets[1].data = smooth.data;
				chart.data.datasets[1].label = smooth.label;
				chart.update('none');
			} else {
				chart = new Chart(document.getElementById('ma-chart'), {
					type: 'line',
					data: { labels: data.labels, datasets: [raw, smooth] },
					options: {
						responsive: true,
						maintainAspectRatio: false,
						animation: false,
						interaction: { mode: 'index', intersect: false },
						plugins: { legend: { position: 'top' } },
						scales: {
							x: { ticks: { maxTicksLimit: 14, maxRotation: 45 } },
							y: { beginAtZero: true, title: { display: true, text: 'Incidents per day' } },
						},
					},
				});
			}
		}

		elRange.addEventListener('change', render);
		elWindow.addEventListener('input', render);
		Array.prototype.forEach.call(root.querySelectorAll('.preset-btn'), function (btn) {
			btn.addEventListener('click', function () {
				elWindow.value = btn.getAttribute('data-window');
				render();
			});
		});
		render();
	}

	// ── Lab 3: cherry-picking machine ────────────────────────────
	function initCherry() {
		var root = document.getElementById('lab-cherry');
		if (!root || !L || !L.monthly.labels.length) return;

		var months = L.monthly.labels;
		var counts = L.monthly.counts;
		var ids = ['cp-base-start', 'cp-base-end', 'cp-comp-start', 'cp-comp-end'];
		var sels = ids.map(function (id) { return document.getElementById(id); });
		sels.forEach(function (sel) {
			months.forEach(function (m) {
				var opt = document.createElement('option');
				opt.value = m;
				opt.textContent = m;
				sel.appendChild(opt);
			});
		});

		// defaults: baseline = 2019 calendar year (or first 12 months),
		// comparison = the 12 most recent complete months
		function idx(m) { return months.indexOf(m); }
		var b0 = idx('2019-01') >= 0 ? '2019-01' : months[0];
		var b1 = idx('2019-12') >= 0 ? '2019-12' : months[Math.min(11, months.length - 1)];
		sels[0].value = b0;
		sels[1].value = b1;
		sels[2].value = months[Math.max(0, months.length - 12)];
		sels[3].value = months[months.length - 1];

		var seenStories = {};
		var chart = null;

		function render() {
			var bs = idx(sels[0].value), be = idx(sels[1].value);
			var cs = idx(sels[2].value), ce = idx(sels[3].value);
			var headline = document.getElementById('cp-headline');
			var detail = document.getElementById('cp-detail');

			if (be < bs || ce < cs) {
				headline.textContent = 'Each period must end after it starts.';
				headline.className = 'cp-headline';
				detail.textContent = '';
				return;
			}

			var base = counts.slice(bs, be + 1);
			var comp = counts.slice(cs, ce + 1);
			// mean per month, so unequal-length periods still compare fairly
			var mBase = mean(base), mComp = mean(comp);
			var pct = mBase > 0 ? (mComp - mBase) / mBase * 100 : 0;
			var rounded = Math.round(pct * 10) / 10;

			var dir = rounded > 0 ? 'UP' : rounded < 0 ? 'DOWN' : 'FLAT';
			headline.textContent = rounded === 0
				? 'Campus incidents flat.'
				: 'Campus incidents ' + dir + ' ' + Math.abs(rounded) + '%!';
			headline.className = 'cp-headline ' + (rounded > 0 ? 'cp-up' : rounded < 0 ? 'cp-down' : '');

			detail.textContent = 'Average ' + fmt1(mComp) + ' incidents/month in ' +
				sels[2].value + '–' + sels[3].value + ' (' + comp.length + ' months) vs. ' +
				fmt1(mBase) + '/month in ' + sels[0].value + '–' + sels[1].value +
				' (' + base.length + ' months). Averages per month, so different-length periods still compare.';

			seenStories[String(rounded)] = true;
			var n = Object.keys(seenStories).length;
			document.getElementById('cp-counter').textContent =
				'You’ve made this one dataset say ' + n + ' different thing' + (n > 1 ? 's' : '') + ' so far.';

			var colors = months.map(function (_, i) {
				var inBase = i >= bs && i <= be;
				var inComp = i >= cs && i <= ce;
				if (inBase && inComp) return 'rgba(106,27,154,0.8)';
				if (inBase) return 'rgba(21,101,192,0.75)';
				if (inComp) return 'rgba(198,40,40,0.75)';
				return 'rgba(180,180,180,0.4)';
			});

			if (chart) {
				chart.data.datasets[0].backgroundColor = colors;
				chart.update('none');
			} else {
				chart = new Chart(document.getElementById('cp-chart'), {
					type: 'bar',
					data: {
						labels: months,
						datasets: [{ label: 'Incidents per month', data: counts, backgroundColor: colors }],
					},
					options: {
						responsive: true,
						maintainAspectRatio: false,
						plugins: { legend: { display: false } },
						scales: {
							x: { ticks: { maxTicksLimit: 18, maxRotation: 45 } },
							y: { beginAtZero: true, title: { display: true, text: 'Incidents' } },
						},
					},
				});
			}
		}

		sels.forEach(function (sel) { sel.addEventListener('change', render); });
		render();
	}

	// ── Lab 4: spike detector ────────────────────────────────────
	function initSpikes() {
		var root = document.getElementById('lab-spikes');
		if (!root || !L) return;

		var elRange = document.getElementById('spike-range');
		var elAcad = document.getElementById('spike-academic');
		var chart = null;
		var current = null; // rendered state for the click handler

		function breakWeek(label) {
			var m = parseInt(label.slice(5, 7), 10);
			return m === 12 || m === 1 || m === 5;
		}

		function render() {
			var data = weeklyData();
			if (elRange.value === '24m') data = tail(data, 104);

			var bandXs = [];
			for (var i = 0; i < data.values.length; i++) {
				if (!elAcad.checked || !breakWeek(data.labels[i])) bandXs.push(data.values[i]);
			}
			var m = mean(bandXs), s = sampleSd(bandXs);
			var hi = m + 2 * s, lo = Math.max(0, m - 2 * s);

			var flagged = [];
			var pointColors = data.values.map(function (v, i) {
				if (v > hi) { flagged.push(i); return '#c62828'; }
				return 'rgba(90,90,90,0.55)';
			});
			var pointR = data.values.map(function (v) { return v > hi ? 4 : 2; });

			current = { data: data, mean: m, sd: s, hi: hi };

			document.getElementById('spike-count').textContent =
				flagged.length + ' of ' + data.values.length + ' complete weeks sit above the band ' +
				'(mean ' + fmt1(m) + ' incidents/week, SD ' + fmt1(s) + ', so the band tops out at ' + fmt1(hi) + ').' +
				(elAcad.checked ? ' Band computed from semester weeks only.' : '');

			var flat = function (v) { return data.values.map(function () { return v; }); };
			var datasets = [
				{
					label: 'Incidents per week',
					data: data.values,
					showLine: true,
					borderColor: 'rgba(90,90,90,0.35)',
					borderWidth: 1,
					pointBackgroundColor: pointColors,
					pointBorderColor: pointColors,
					pointRadius: pointR,
					order: 1,
				},
				{
					label: 'Mean',
					data: flat(m),
					borderColor: 'rgba(21,101,192,0.9)',
					borderWidth: 1.5,
					borderDash: [6, 4],
					pointRadius: 0,
					order: 2,
				},
				{
					label: 'Mean + 2 SD',
					data: flat(hi),
					borderColor: 'rgba(198,40,40,0.5)',
					borderWidth: 1,
					pointRadius: 0,
					fill: '+1',
					backgroundColor: 'rgba(21,101,192,0.08)',
					order: 3,
				},
				{
					label: 'Mean − 2 SD',
					data: flat(lo),
					borderColor: 'rgba(198,40,40,0.5)',
					borderWidth: 1,
					pointRadius: 0,
					order: 4,
				},
			];

			if (chart) {
				chart.data.labels = data.labels;
				chart.data.datasets = datasets;
				chart.update('none');
			} else {
				chart = new Chart(document.getElementById('spike-chart'), {
					type: 'line',
					data: { labels: data.labels, datasets: datasets },
					options: {
						responsive: true,
						maintainAspectRatio: false,
						animation: false,
						plugins: { legend: { position: 'top' } },
						scales: {
							x: { ticks: { maxTicksLimit: 14, maxRotation: 45 } },
							y: { beginAtZero: true, title: { display: true, text: 'Incidents per week' } },
						},
						onClick: function (evt) {
							var pts = chart.getElementsAtEventForMode(evt, 'nearest', { intersect: false }, true);
							if (!pts.length || !current) return;
							var i = pts[0].index;
							var v = current.data.values[i];
							var card = document.getElementById('spike-card');
							var z = current.sd > 0 ? (v - current.mean) / current.sd : 0;
							var weekStart = current.data.labels[i];
							var flaggedWeek = v > current.hi;
							card.style.display = 'block';
							card.innerHTML =
								'<strong>Week of ' + weekStart + '</strong> — ' + v + ' incidents, z-score ' +
								fmt1(z) + ' (' + fmt1(Math.abs(z)) + ' standard deviations ' +
								(z >= 0 ? 'above' : 'below') + ' the mean of ' + fmt1(current.mean) + ').' +
								(flaggedWeek
									? '<br><em>Newsworthy, or noise?</em> Before writing, check: Was this the first week of a semester? A home football weekend? Is one location (see <a href="../../trends/">Trends</a> clusters) generating the reports? Did UMPD change how it logs incidents?'
									: '<br>This week is inside the normal band — ordinary variation, not a story.');
						},
					},
				});
			}
		}

		elRange.addEventListener('change', render);
		elAcad.addEventListener('change', render);
		render();
	}

	wireCopyButtons();
	initAverages();
	initMoving();
	initCherry();
	initSpikes();
})();
