(function () {
	'use strict';

	var A = window.ANALYTICS;
	if (!A || !A.daily_by_category) return;

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

	var D = A.daily_by_category;
	var DAY = 86400000;

	function parseISO(s) {
		var p = s.split('-');
		return new Date(Date.UTC(+p[0], +p[1] - 1, +p[2]));
	}
	function toISO(d) { return d.toISOString().slice(0, 10); }

	var dataStart = parseISO(D.start);
	var dataEnd = parseISO(D.coverage_end || toISO(new Date(dataStart.getTime() + (D.total.length - 1) * DAY)));

	// index of a date in the daily arrays, clamped to coverage; -1 if fully outside
	function idxOf(d) {
		var i = Math.round((d - dataStart) / DAY);
		if (i < 0) return 0;
		if (i >= D.total.length) return D.total.length - 1;
		return i;
	}

	function sumRange(arr, i0, i1) {
		var n = 0;
		for (var i = i0; i <= i1; i++) n += arr[i] || 0;
		return n;
	}

	// ── default ranges: last 30 days of coverage vs. same dates a year ago ──
	function setDefaults() {
		var aEnd = dataEnd;
		var aStart = new Date(aEnd.getTime() - 29 * DAY);
		var bEnd = new Date(aEnd.getTime());   bEnd.setUTCFullYear(bEnd.getUTCFullYear() - 1);
		var bStart = new Date(aStart.getTime()); bStart.setUTCFullYear(bStart.getUTCFullYear() - 1);
		if (bStart < dataStart) { bStart = dataStart; }

		document.getElementById('a-start').value = toISO(aStart);
		document.getElementById('a-end').value = toISO(aEnd);
		document.getElementById('b-start').value = toISO(bStart);
		document.getElementById('b-end').value = toISO(bEnd);

		var min = toISO(dataStart), max = toISO(dataEnd);
		['a-start', 'a-end', 'b-start', 'b-end'].forEach(function (id) {
			var el = document.getElementById(id);
			el.min = min;
			el.max = max;
		});
	}

	var chart = null;

	function runCompare() {
		var errEl = document.getElementById('compare-error');
		errEl.textContent = '';

		var a0 = parseISO(document.getElementById('a-start').value);
		var a1 = parseISO(document.getElementById('a-end').value);
		var b0 = parseISO(document.getElementById('b-start').value);
		var b1 = parseISO(document.getElementById('b-end').value);

		if (isNaN(a0) || isNaN(a1) || isNaN(b0) || isNaN(b1)) {
			errEl.textContent = 'Please pick all four dates.';
			return;
		}
		if (a1 < a0 || b1 < b0) {
			errEl.textContent = 'Each period\u2019s end date must be on or after its start date.';
			return;
		}

		var ai0 = idxOf(a0), ai1 = idxOf(a1), bi0 = idxOf(b0), bi1 = idxOf(b1);

		var countsA = {}, countsB = {};
		CATS.forEach(function (cat) {
			countsA[cat] = sumRange(D.series[cat], ai0, ai1);
			countsB[cat] = sumRange(D.series[cat], bi0, bi1);
		});
		var totA = sumRange(D.total, ai0, ai1);
		var totB = sumRange(D.total, bi0, bi1);
		var arrA = sumRange(D.arrests, ai0, ai1);
		var arrB = sumRange(D.arrests, bi0, bi1);
		var daysA = Math.round((a1 - a0) / DAY) + 1;
		var daysB = Math.round((b1 - b0) / DAY) + 1;

		// summary sentence
		var pct = totB > 0 ? Math.round((totA - totB) / totB * 100) : null;
		var dir = pct === null ? '' : pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat';
		var rateA = totA > 0 ? (arrA / totA * 100).toFixed(1) : '0.0';
		var rateB = totB > 0 ? (arrB / totB * 100).toFixed(1) : '0.0';
		var summary = '<strong>Period A</strong> (' + toISO(a0) + ' \u2013 ' + toISO(a1) + ', ' + daysA + ' days): ' +
			totA + ' incidents, ' + rateA + '% with an arrest. ' +
			'<strong>Period B</strong> (' + toISO(b0) + ' \u2013 ' + toISO(b1) + ', ' + daysB + ' days): ' +
			totB + ' incidents, ' + rateB + '% with an arrest. ';
		if (pct !== null) {
			summary += 'That\u2019s <strong>' + dir + ' ' + Math.abs(pct) + '%</strong>' +
				(pct === 0 ? ' \u2014 no change.' : ' in A versus B.');
		} else {
			summary += 'Period B had zero incidents, so no percentage change is meaningful.';
		}
		document.getElementById('compare-summary').innerHTML = summary;

		// grouped bar chart
		var labels = CATS.map(function (c) { return CAT_LABELS[c]; });
		var dataA = CATS.map(function (c) { return countsA[c]; });
		var dataB = CATS.map(function (c) { return countsB[c]; });
		if (chart) chart.destroy();
		chart = new Chart(document.getElementById('compare-chart'), {
			type: 'bar',
			data: {
				labels: labels,
				datasets: [
					{ label: 'Period A', data: dataA, backgroundColor: 'rgba(178,34,34,0.75)', borderColor: '#b22222', borderWidth: 1 },
					{ label: 'Period B', data: dataB, backgroundColor: 'rgba(21,101,192,0.65)', borderColor: '#1565c0', borderWidth: 1 },
				],
			},
			options: {
				responsive: true,
				maintainAspectRatio: false,
				plugins: { legend: { position: 'top' } },
				scales: { y: { beginAtZero: true, title: { display: true, text: 'Incidents' } } },
			},
		});

		// table
		var html = '<table class="compare-tbl"><thead><tr>' +
			'<th>Category</th><th>A</th><th>B</th><th>Change</th></tr></thead><tbody>';
		CATS.forEach(function (cat) {
			var ca = countsA[cat], cb = countsB[cat];
			var change;
			if (cb === 0) {
				change = ca === 0 ? '\u2014' : 'new in A';
			} else {
				var p = Math.round((ca - cb) / cb * 100);
				change = (p > 0 ? '+' : '') + p + '%';
			}
			var cls = ca > cb ? 'cmp-up' : ca < cb ? 'cmp-down' : '';
			html += '<tr><td>' + CAT_LABELS[cat] + '</td><td>' + ca + '</td><td>' + cb + '</td>' +
				'<td class="' + cls + '">' + change + '</td></tr>';
		});
		html += '<tr class="cmp-total"><td>All incidents</td><td>' + totA + '</td><td>' + totB + '</td><td>' +
			(pct === null ? '\u2014' : (pct > 0 ? '+' : '') + pct + '%') + '</td></tr>';
		html += '<tr><td>Arrest rate</td><td>' + rateA + '%</td><td>' + rateB + '%</td><td>\u2014</td></tr>';
		html += '</tbody></table>';
		document.getElementById('compare-table').innerHTML = html;
	}

	setDefaults();
	runCompare();
	document.getElementById('compare-run').addEventListener('click', runCompare);
})();
