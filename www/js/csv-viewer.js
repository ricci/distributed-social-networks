const statusEl = document.getElementById("status");
const tableEl = document.getElementById("table");
const titleLinkEl = document.getElementById("title-link");
const rowLimitEl = document.getElementById("row-limit");
const downloadLinkEl = document.getElementById("download-link");
let grid = null;
let currentRows = [];
let currentColumns = [];
let currentCsvUrl = "";

function setStatus(message, isError) {
  statusEl.textContent = message;
  statusEl.classList.toggle("error", Boolean(isError));
}

function parseCSV(text) {
  const cleaned = text.replace(/^\uFEFF/, "").trim();
  if (!cleaned) {
    return [];
  }
  return cleaned
    .split(/\r?\n/)
    .map((line) => line.split(","))
    .filter((entry) => entry.some((cell) => cell !== ""));
}

function columnIsInteger(columnIndex, data) {
  let sawValue = false;
  for (const row of data) {
    const value = row[columnIndex];
    if (value === undefined || value === null || value === "") {
      continue;
    }
    sawValue = true;
    if (!/^-?\d+$/.test(String(value))) {
      return false;
    }
  }
  return sawValue;
}

function buildColumns(header, data) {
  return header.map((name, index) => {
    const columnName = name || `Column ${index + 1}`;
    const isInteger = columnIsInteger(index, data);
    if (!isInteger) {
      return { name: columnName, sort: true };
    }
    return {
      name: columnName,
      sort: {
        compare: (left, right) => {
          return Number(left) - Number(right);
        },
      },
    };
  });
}

function renderGrid() {
  if (!currentRows.length) {
    setStatus("No rows found in CSV.", true);
    return;
  }

  if (grid) {
    grid.destroy();
  }

  const limitValue = rowLimitEl.value;
  const pagination =
    limitValue === "all" ? false : { limit: Number(limitValue) || 100 };

  grid = new gridjs.Grid({
    columns: currentColumns,
    data: currentRows,
    search: true,
    sort: true,
    pagination,
    resizable: true,
  }).render(tableEl);
}

function renderTable(rows) {
  if (!rows.length) {
    setStatus("No rows found in CSV.", true);
    return;
  }

  const [header, ...data] = rows;
  const activeMonthIndex = header.findIndex((name) => {
    const normalized = name.trim().toLowerCase();
    return normalized === "active_month" || normalized === "mau";
  });
  if (activeMonthIndex !== -1) {
    const isActiveMonthInteger = columnIsInteger(activeMonthIndex, data);
    data.sort((left, right) => {
      const leftValue = left[activeMonthIndex] || "";
      const rightValue = right[activeMonthIndex] || "";
      if (isActiveMonthInteger) {
        return Number(rightValue) - Number(leftValue);
      }
      if (leftValue === rightValue) {
        return 0;
      }
      return leftValue > rightValue ? -1 : 1;
    });
  }
  currentColumns = buildColumns(header, data);
  currentRows = data;
  renderGrid();

  setStatus(`Loaded ${data.length} rows.`);
}

async function loadCSVFromUrl(url) {
  try {
    setStatus(`Loading ${url}...`);
    currentCsvUrl = url;
    titleLinkEl.textContent = url;
    titleLinkEl.href = url;
    downloadLinkEl.href = url;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to load CSV (${response.status}).`);
    }
    const text = await response.text();
    renderTable(parseCSV(text));
  } catch (error) {
    setStatus(error.message, true);
  }
}

const csvParam = new URLSearchParams(window.location.search).get("csv");
if (csvParam) {
  loadCSVFromUrl(csvParam);
} else {
  setStatus("Provide ?csv=data/file.csv to load a dataset.", true);
}

rowLimitEl.addEventListener("change", () => {
  if (currentRows.length) {
    renderGrid();
  }
});
