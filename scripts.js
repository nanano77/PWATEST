import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm";

let db;

async function initDuckDB() {
  try {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      })
    );

    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // ✅ Persistent 模式開啟 IndexedDB 儲存
    await db.open({
      path: "my-duckdb",
      persistent: true,
    });

    URL.revokeObjectURL(worker_url);
    console.log("✅ DuckDB-Wasm with Persistent IndexedDB initialized.");

    updateTableList();
  } catch (error) {
    console.error("Error initializing DuckDB-Wasm:", error);
  }
}

// ✅ 重設 IndexedDB 儲存（清空資料庫）
async function resetPersistentDB() {
  try {
    if (!db) return;
    await db.close();
    await db.deletePersistentDatabase("my-duckdb");
    alert("🧹 資料庫已清除，下次重新整理會重新初始化");
    console.log("✅ Persistent DuckDB 已清除");
  } catch (err) {
    console.error("❌ 清除資料庫錯誤：", err);
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

    const importLogDiv = document.getElementById("importLogDiv");
    importLogDiv.innerHTML = "";

    console.log("⚙️ 建立大量假資料表：", tableName);
    const conn = await db.connect();
    console.log("✅ Database connection established");

    const createSQL = `
      CREATE TABLE '${tableName}' (
        id INTEGER,
        name TEXT,
        age INTEGER
      );
    `;
    await conn.query(createSQL);

    const rows = [];
    for (let i = 1; i <= 10000; i++) {
      const name = `user_${i}`;
      const age = 20 + (i % 40);
      rows.push(`(${i}, '${name}', ${age})`);
    }

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
async function forceResetBrokenDB() {
  try {
    if (db) await db.close(); // 保險起見先關閉
    await duckdb.deletePersistentDatabase("my-duckdb"); // 注意！這是 static 方法！
    alert("🚨 已強制清除損壞的 my-duckdb 資料庫，請重新整理頁面");
  } catch (err) {
    console.error("❌ 無法清除損壞資料庫:", err);
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
    const query = `SELECT table_name as TABLES FROM information_schema.tables WHERE table_schema = 'main';`;
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
    }

    await conn.close();
  } catch (error) {
    console.error("Error processing file or querying data:", error);
  }
}

function arrowToHtmlTable(arrowTable, htmlTableId) {
  const tableSchema = arrowTable.schema.fields.map((field) => field.name);
  const tableRows = arrowTable.toArray();

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
  const lastQueryDiv = document.getElementById("lastQueryDiv");
  const resultTable = document.getElementById("resultTable");
  const resultErrorDiv = document.getElementById("resultErrorDiv");

  try {
    if (!db) {
      console.error("DuckDB-Wasm is not initialized");
      return;
    }

    const conn = await db.connect();
    const result = await conn.query(query);
    arrowToHtmlTable(result, "resultTable");
    updateTableList();
    queryResultsDiv.style.display = "block";
    resultTable.style.display = "block";
    resultErrorDiv.style.display = "none";
    resultErrorDiv.innerHTML = "";
    lastQueryDiv.innerHTML = query;
    await conn.close();
  } catch (error) {
    resultTable.style.display = "none";
    resultErrorDiv.style.display = "block";
    resultErrorDiv.innerHTML = error;
    console.error("Error processing file or querying data:", error);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initDuckDB();

  window.uploadMockTable = uploadMockTable;
  window.uploadMassiveMockTable = uploadMassiveMockTable;
  window.uploadTable = uploadTable;
  window.runQuery = runQuery;
  window.resetPersistentDB = resetPersistentDB; // ✅ 新增綁定
  window.forceResetBrokenDB = forceResetBrokenDB;
});
