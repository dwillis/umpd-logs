// filters.js - Advanced filtering functionality for Phase 2

let dateRangePicker;
let activeFilters = {
	dateRange: null,
	crimeTypes: [],
	dispositions: [],
	location: ''
};

function initializeFilters() {
	// Initialize Flatpickr for date range
	dateRangePicker = flatpickr('#date-range', {
		mode: 'range',
		dateFormat: 'Y-m-d',
		maxDate: 'today'
	});

	// Populate crime type filter
	populateFilterDropdown('#crime-type-filter', 3); // Crime Type column

	// Populate disposition filter
	populateFilterDropdown('#disposition-filter', 4); // Disposition column

	// Event listeners
	$('#toggle-filters').on('click', toggleFiltersPanel);
	$('#apply-filters').on('click', applyAllFilters);
	$('#reset-filters').on('click', resetAllFilters);
	$('#clear-dates').on('click', clearDateRange);
	$('#location-search').on('input', debounce(updateLocationFilter, 300));
}

function populateFilterDropdown(selector, columnIndex) {
	const table = $('#crime-table').DataTable();
	const data = table.column(columnIndex).data();
	const unique = [...new Set(data.toArray().map(d => $(d).text()))].sort();

	const $select = $(selector);
	$select.empty();

	unique.forEach(value => {
		if (value) {
			$select.append('<option value="' + value + '">' + value + '</option>');
		}
	});
}

function toggleFiltersPanel() {
	const $content = $('#filters-content');
	const $btn = $('#toggle-filters');

	if ($content.is(':visible')) {
		$content.slideUp();
		$btn.text('Show Filters');
	} else {
		$content.slideDown();
		$btn.text('Hide Filters');
	}
}

function applyAllFilters() {
	const table = $('#crime-table').DataTable();

	// Get filter values
	activeFilters.dateRange = dateRangePicker.selectedDates;
	activeFilters.crimeTypes = $('#crime-type-filter').val() || [];
	activeFilters.dispositions = $('#disposition-filter').val() || [];
	activeFilters.location = $('#location-search').val();

	// Remove any existing custom search functions
	$.fn.dataTable.ext.search = [];

	// Custom search function
	$.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {
		// Date range filter
		if (activeFilters.dateRange && activeFilters.dateRange.length === 2) {
			const occurredDate = moment(data[1], 'MMM D, YYYY, h:mma');
			const startDate = moment(activeFilters.dateRange[0]);
			const endDate = moment(activeFilters.dateRange[1]);

			if (!occurredDate.isBetween(startDate, endDate, null, '[]')) {
				return false;
			}
		}

		// Crime type filter
		if (activeFilters.crimeTypes.length > 0) {
			const crimeType = data[3];
			if (!activeFilters.crimeTypes.includes(crimeType)) {
				return false;
			}
		}

		// Disposition filter
		if (activeFilters.dispositions.length > 0) {
			const disposition = $(data[4]).text();
			if (!activeFilters.dispositions.includes(disposition)) {
				return false;
			}
		}

		// Location filter
		if (activeFilters.location) {
			const location = data[5].toLowerCase();
			if (!location.includes(activeFilters.location.toLowerCase())) {
				return false;
			}
		}

		return true;
	});

	// Redraw table
	table.draw();

	// Update filter count
	updateFilterCount(table.rows({ search: 'applied' }).count());

	// Update charts with filtered data
	updateChartsWithFilteredData();
}

function resetAllFilters() {
	// Clear all filters
	dateRangePicker.clear();
	$('#crime-type-filter').val([]);
	$('#disposition-filter').val([]);
	$('#location-search').val('');

	activeFilters = {
		dateRange: null,
		crimeTypes: [],
		dispositions: [],
		location: ''
	};

	// Remove custom search functions
	$.fn.dataTable.ext.search = [];

	// Redraw table
	const table = $('#crime-table').DataTable();
	table.draw();

	// Update filter count
	$('#filter-count').text('');

	// Refresh charts
	initializeCharts();
}

function clearDateRange() {
	dateRangePicker.clear();
}

function updateLocationFilter() {
	// This is called on input, but actual filtering happens on "Apply"
	// Could implement live filtering here if desired
}

function updateFilterCount(count) {
	const total = $('#crime-table').DataTable().rows().count();
	$('#filter-count').text('Showing ' + count + ' of ' + total + ' incidents');
}

function updateChartsWithFilteredData() {
	const table = $('#crime-table').DataTable();
	const filteredData = table.rows({ search: 'applied' }).data();

	// Re-render all charts with filtered data
	renderCrimeTypePieChart(filteredData);
	renderTimelineChart(filteredData);
	renderLocationsChart(filteredData);
	renderDispositionChart(filteredData);
}

// Utility: debounce function
function debounce(func, wait) {
	let timeout;
	return function executedFunction(...args) {
		const later = () => {
			clearTimeout(timeout);
			func(...args);
		};
		clearTimeout(timeout);
		timeout = setTimeout(later, wait);
	};
}
