// charts.js - Chart rendering functions for Phase 2

let chartInstances = {}; // Store chart instances for updates

function initializeCharts() {
	const table = $('#crime-table').DataTable();
	const data = table.rows().data();

	renderCrimeTypePieChart(data);
	renderTimelineChart(data);
	renderLocationsChart(data);
	renderDispositionChart(data);
}

function renderCrimeTypePieChart(data) {
	const typeCounts = {};

	for (let i = 0; i < data.length; i++) {
		const type = $(data[i][3]).text();
		typeCounts[type] = (typeCounts[type] || 0) + 1;
	}

	// Get top 8 crime types, group rest as "Other"
	const sorted = Object.entries(typeCounts)
		.sort((a, b) => b[1] - a[1]);

	const top8 = sorted.slice(0, 8);
	const otherCount = sorted.slice(8).reduce((sum, [, count]) => sum + count, 0);

	if (otherCount > 0) {
		top8.push(['Other', otherCount]);
	}

	const labels = top8.map(([type]) => type);
	const values = top8.map(([, count]) => count);

	const ctx = document.getElementById('pieChart').getContext('2d');

	if (chartInstances.pie) chartInstances.pie.destroy();

	chartInstances.pie = new Chart(ctx, {
		type: 'doughnut',
		data: {
			labels: labels,
			datasets: [{
				data: values,
				backgroundColor: [
					'#b22222', '#e65100', '#6a1b9a', '#1565c0',
					'#2e7d32', '#f57c00', '#c62828', '#5e35b1', '#757575'
				],
				borderWidth: 2,
				borderColor: '#fff'
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: true,
			plugins: {
				legend: {
					position: 'right',
					labels: { font: { size: 11 }, padding: 10 }
				},
				tooltip: {
					callbacks: {
						label: function(context) {
							const total = context.dataset.data.reduce((a, b) => a + b, 0);
							const percentage = ((context.parsed / total) * 100).toFixed(1);
							return context.label + ': ' + context.parsed + ' (' + percentage + '%)';
						}
					}
				}
			}
		}
	});
}

function renderTimelineChart(data) {
	const last30Days = moment().subtract(30, 'days');
	const dailyCounts = {};

	// Initialize all dates in range
	for (let i = 0; i < 30; i++) {
		const date = moment().subtract(i, 'days').format('YYYY-MM-DD');
		dailyCounts[date] = 0;
	}

	// Count incidents per day
	for (let i = 0; i < data.length; i++) {
		const dateStr = $(data[i][1]).text(); // Occurred date
		const date = moment(dateStr, 'MMM D, YYYY, h:mma');

		if (date.isAfter(last30Days)) {
			const dateKey = date.format('YYYY-MM-DD');
			if (dailyCounts[dateKey] !== undefined) {
				dailyCounts[dateKey]++;
			}
		}
	}

	const sortedDates = Object.keys(dailyCounts).sort();
	const labels = sortedDates.map(d => moment(d).format('MMM D'));
	const values = sortedDates.map(d => dailyCounts[d]);

	const ctx = document.getElementById('timelineChart').getContext('2d');

	if (chartInstances.timeline) chartInstances.timeline.destroy();

	chartInstances.timeline = new Chart(ctx, {
		type: 'line',
		data: {
			labels: labels,
			datasets: [{
				label: 'Incidents',
				data: values,
				borderColor: '#b22222',
				backgroundColor: 'rgba(178, 34, 34, 0.1)',
				fill: true,
				tension: 0.3,
				pointRadius: 3,
				pointHoverRadius: 5
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: true,
			scales: {
				y: {
					beginAtZero: true,
					ticks: { stepSize: 1 }
				},
				x: {
					ticks: { maxRotation: 45, minRotation: 45 }
				}
			},
			plugins: {
				legend: { display: false }
			}
		}
	});
}

function renderLocationsChart(data) {
	const locationCounts = {};

	for (let i = 0; i < data.length; i++) {
		const location = $(data[i][5]).text();
		locationCounts[location] = (locationCounts[location] || 0) + 1;
	}

	const top10 = Object.entries(locationCounts)
		.sort((a, b) => b[1] - a[1])
		.slice(0, 10);

	const labels = top10.map(([loc]) => loc.length > 30 ? loc.substring(0, 30) + '...' : loc);
	const values = top10.map(([, count]) => count);

	const ctx = document.getElementById('locationsChart').getContext('2d');

	if (chartInstances.locations) chartInstances.locations.destroy();

	chartInstances.locations = new Chart(ctx, {
		type: 'bar',
		data: {
			labels: labels,
			datasets: [{
				label: 'Incidents',
				data: values,
				backgroundColor: '#b22222',
				borderColor: '#7a0000',
				borderWidth: 1
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: true,
			indexAxis: 'y',
			scales: {
				x: {
					beginAtZero: true,
					ticks: { stepSize: 1 }
				}
			},
			plugins: {
				legend: { display: false }
			}
		}
	});
}

function renderDispositionChart(data) {
	const dispositionCounts = {};

	for (let i = 0; i < data.length; i++) {
		const disposition = $(data[i][4]).text();
		dispositionCounts[disposition] = (dispositionCounts[disposition] || 0) + 1;
	}

	const labels = Object.keys(dispositionCounts);
	const values = Object.values(dispositionCounts);

	const ctx = document.getElementById('dispositionChart').getContext('2d');

	if (chartInstances.disposition) chartInstances.disposition.destroy();

	chartInstances.disposition = new Chart(ctx, {
		type: 'bar',
		data: {
			labels: labels,
			datasets: [{
				label: 'Count',
				data: values,
				backgroundColor: ['#1565c0', '#6a1b9a', '#e65100', '#2e7d32', '#757575'],
				borderWidth: 0
			}]
		},
		options: {
			responsive: true,
			maintainAspectRatio: true,
			scales: {
				y: {
					beginAtZero: true
				}
			},
			plugins: {
				legend: { display: false }
			}
		}
	});
}
