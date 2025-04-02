import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";

let db;

async function initDuckDB() {
  try {
    // receive the bundles of files required to run duckdb in the browser
    // this is the compiled wasm code, the js and worker scripts
    // worker scripts are js scripts ran in background threads (not the same thread as the ui)
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    // select bundle is a function that selects the files that will work with your browser
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    // creates storage and an address for the main worker
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      })
    );

    // creates the worker and logger required for an instance of duckdb
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);

    // loads the web assembly module into memory and configures it
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // revoke the object url now no longer needed
    URL.revokeObjectURL(worker_url);
    console.log("DuckDB-Wasm initialized successfully.");
  } catch (error) {
    console.error("Error initializing DuckDB-Wasm:", error);
  }
}

async function loadDatabaseFromFile(file) {
  try {
    if (!file || !db) {
      alert("請選擇 .db 檔案，並確認 DuckDB 已初始化");
      return;
    }

    const arrayBuffer = await file.arrayBuffer();
    const buffer = new Uint8Array(arrayBuffer);

    await db.importDatabase(buffer); // ✅ 載入成目前 active DB
    console.log("✅ 成功載入 .db 資料庫");
    updateTableList(); // 更新 UI 表格顯示
  } catch (err) {
    console.error("❌ 載入 .db 檔案失敗：", err);
  }
}



async function uploadTable() {
  try {
    const fileInput = document.getElementById("fileInput");
    const file = fileInput.files ? fileInput.files[0] : null;

    if (!file) {
      alert("Please select a file first.");
      return;
    }

    const tableNameInput = document.getElementById("tableNameInput");
    const tableName = tableNameInput.value;

    if (!tableName) {
      alert("Please enter a valid table name.");
      return;
    }

    const arrayBuffer = await file.arrayBuffer();

    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }
    console.log("File loaded:", file.name);

    const conn = await db.connect();
    console.log("Database connection established");

    const fileType = file.name.split(".").pop()?.toLowerCase() || "";

    if (fileType === "csv" || fileType === "parquet" || fileType === "json") {
      // Register the file in DuckDB's virtual file system
      const virtualFileName = `/${file.name}`;
      await db.registerFileBuffer(virtualFileName, new Uint8Array(arrayBuffer));

      let query = "";
      if (fileType === "csv") {
        query = `CREATE TABLE '${tableName}' AS FROM read_csv_auto('${virtualFileName}', header = true)`;
      } else if (fileType === "parquet") {
        query = `CREATE TABLE '${tableName}' AS FROM read_parquet('${virtualFileName}')`;
      } else if (fileType === "json") {
        query = `CREATE TABLE '${tableName}' AS FROM read_json_auto('${virtualFileName}')`;
      }

      await conn.query(query);
      updateTableList();
    } else {
      console.log("Invalid file type: ", fileType);
    }
  } catch (error) {
    console.error("Error processing file or querying data:", error);
  }
}

async function uploadMassiveMockTable() {
  try {
    const tableNameInput = document.getElementById("tableNameInput");
    const tableName = tableNameInput.value.trim();

    if (!tableName) {
      alert("Please enter a valid table name.");
      return;
    }

    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    // 清空前一次的匯入時間紀錄
    const importLogDiv = document.getElementById("importLogDiv");
    importLogDiv.innerHTML = "";

    console.log("⚙️ 建立大量假資料表：", tableName);
    const conn = await db.connect();
    console.log("✅ Database connection established");

    // 建立表格
    const createSQL = `
      CREATE TABLE '${tableName}' (
        id INTEGER,
        name TEXT,
        age INTEGER
      );
    `;
    await conn.query(createSQL);

    // 產生 10,000 筆資料
    const rows = [];
    for (let i = 1; i <= 10000; i++) {
      const name = `user_${i}`;
      const age = 20 + (i % 40); // 20~59
      rows.push(`(${i}, '${name}', ${age})`);
    }

    // 分批插入，每批 500 筆
    const batchSize = 500;
    for (let i = 0; i < rows.length; i += batchSize) {
      const chunk = rows.slice(i, i + batchSize).join(",\n");
      const insertSQL = `INSERT INTO '${tableName}' VALUES\n${chunk};`;

      const t0 = performance.now();
      await conn.query(insertSQL);
      const t1 = performance.now();
      const duration = (t1 - t0).toFixed(2);

      console.log(`🚀 匯入第 ${i + 1} ~ ${Math.min(i + batchSize, rows.length)} 筆，耗時 ${duration} ms`);
      importLogDiv.innerHTML += `🚀 第 ${i + 1} ~ ${Math.min(i + batchSize, rows.length)} 筆：${duration} ms<br>`;
    }

    await conn.close();
    console.log(`✅ 萬筆資料表 '${tableName}' 建立完成，共 ${rows.length} 筆`);
    updateTableList();
  } catch (error) {
    console.error("❌ Error creating massive mock table:", error);
  }
}

async function downloadExportedDatabaseFolderAsFile() {
  const filename = 'tt.db/database.duckdb';
  const downloadName = 'tt.db'; // 儲存時的檔名

  try {
    const buffer = await db.readFile(filename);
    const blob = new Blob([buffer], { type: 'application/octet-stream' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = downloadName;
    a.click();
    URL.revokeObjectURL(url);

    console.log(`✅ 成功下載匯出的記憶體資料庫：${downloadName}`);
  } catch (err) {
    console.error("❌ 讀取或下載失敗：", err);
  }
}






async function uploadMockTable() {
  try {
    const tableNameInput = document.getElementById("tableNameInput");
    const tableName = tableNameInput.value;

    if (!tableName) {
      alert("Please enter a valid table name.");
      return;
    }

    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    console.log("⚙️ 正在建立 mock 資料表：", tableName);
    const conn = await db.connect();
    console.log("Database connection established");

    // 10 筆假資料（每組 id 都一樣，不影響測試）
    const valuesSQL = `
      (1, 'Alice', 25),
      (2, 'Bob', 32),
      (3, 'Charlie', 40),
      (4, 'Diana', 28),
      (5, 'Ethan', 22),
      (6, 'Fiona', 35),
      (7, 'George', 30),
      (8, 'Helen', 27),
      (9, 'Ivan', 29),
      (10, 'Jenny', 33)
    `;

    // 匯入 10 次，共產生 100 筆資料
    let insertSQL = "";
    for (let i = 0; i < 10; i++) {
      insertSQL += `INSERT INTO '${tableName}' VALUES ${valuesSQL};\n`;
    }

    const createAndInsert = `
      CREATE TABLE '${tableName}' (
        id INTEGER,
        name TEXT,
        age INTEGER
      );
      ${insertSQL}
    `;

    await conn.query(createAndInsert);
    await conn.close();

    console.log(`✅ Mock table '${tableName}' 已建立，共 100 筆資料`);
    updateTableList();
  } catch (error) {
    console.error("❌ Error creating mock table:", error);
  }
}






async function updateTableList() {
  console.log("now running updateTableList");
  try {
    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    const conn = await db.connect();
    console.log("Database connection established");
    const query = `SELECT table_name as TABLES FROM information_schema.tables WHERE table_schema = 'main';;`;
    const showTables = await conn.query(query);

    const rowCount = showTables.numRows;
    const tablesDiv = document.getElementById("tablesDiv");
    const queryEntryDiv = document.getElementById("queryEntryDiv");
    console.log("rowCount: ", rowCount);

    if (rowCount === 0) {
      tablesDiv.style.display = "none";
      queryEntryDiv.style.display = "none";
    } else {
      tablesDiv.style.display = "block";
      queryEntryDiv.style.display = "block";
      arrowToHtmlTable(showTables, "tablesTable");
      await conn.close();
      console.log("Database connection closed");
    }
  } catch (error) {
    console.error("Error processing file or querying data:", error);
  }
}

async function getCurrentDBSize() {
  if (!db) {
    console.error("DuckDB 尚未初始化");
    return;
  }

  const conn = await db.connect();
  const buffer = await conn.exportFile();
  const sizeInKB = (buffer.byteLength / 1024).toFixed(2);
  await conn.close();

  console.log(`📦 DuckDB 資料庫大小：約 ${sizeInKB} KB`);
  return sizeInKB;
}

function arrowToHtmlTable(arrowTable, htmlTableId) {
  // Log the arrowTable to see if it's valid
  console.log("arrowTable:", arrowTable);

  if (!arrowTable) {
    console.error("The arrowTable object is invalid or null.");
    return;
  }

  const tableSchema = arrowTable.schema.fields.map((field) => field.name);
  console.log("tableSchema:", tableSchema); // Log the schema

  const tableRows = arrowTable.toArray();
  console.log("tableRows:", tableRows); // Log the rows

  let htmlTable = document.getElementById(htmlTableId);
  if (!htmlTable) {
    console.error(`Table with ID ${htmlTableId} not found in the DOM.`);
    return;
  }

  htmlTable.innerHTML = "";
  let tableHeaderRow = document.createElement("tr");
  htmlTable.appendChild(tableHeaderRow);
  tableSchema.forEach((tableColumn) => {
    let th = document.createElement("th");
    th.innerText = tableColumn;
    tableHeaderRow.appendChild(th);
  });

  tableRows.forEach((tableRow) => {
    let tr = document.createElement("tr");
    htmlTable.appendChild(tr);
    tableSchema.forEach((tableColumn) => {
      let td = document.createElement("td");
      td.innerText = tableRow[tableColumn];
      tr.appendChild(td);
    });
  });
}

async function runQuery() {
  const queryInput = document.getElementById("queryInput");
  let query = queryInput.value;
  const queryResultsDiv = document.getElementById("queryResultsDiv");

  // Make sure the results div is visible before populating it
  queryResultsDiv.style.display = "block";

  const lastQueryDiv = document.getElementById("lastQueryDiv");
  lastQueryDiv.innerHTML = query;

  const resultTable = document.getElementById("resultTable");
  const resultErrorDiv = document.getElementById("resultErrorDiv");

  try {
    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    const conn = await db.connect();
    console.log("Database connection established");

    const result = await conn.query(query);
    arrowToHtmlTable(result, "resultTable");
    updateTableList();
    queryResultsDiv.style.display = "block";
    resultTable.style.display = "block";
    resultErrorDiv.style.display = "none";
    resultErrorDiv.innerHTML = "";

    await conn.close();
    console.log("Database connection closed");
  } catch (error) {
    resultTable.style.display = "none";
    resultTable.innerHTML = "";
    resultErrorDiv.style.display = "block";
    resultErrorDiv.innerHTML = error;
    console.error("Error processing file or querying data:", error);
  }
}

// Initialize DuckDB on page load
document.addEventListener("DOMContentLoaded", () => {
  initDuckDB();

  window.loadDatabaseFromFile = loadDatabaseFromFile;
  window.downloadExportedDatabaseFolderAsFile = downloadExportedDatabaseFolderAsFile;
  window.uploadMassiveMockTable = uploadMassiveMockTable;
  window.uploadMockTable = uploadMockTable;
  window.uploadTable = uploadTable;
  window.runQuery = runQuery;
  window.getCurrentDBSize = getCurrentDBSize;
});
