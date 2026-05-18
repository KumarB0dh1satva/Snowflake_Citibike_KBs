CREATE OR REPLACE PROCEDURE CITIBIKE_SYSTEM_DATA.LOGGING.SP_INGEST_STAGED_FILES(
    P_REGION VARCHAR   -- 'nyc' | 'jersey_city' | 'all'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// ── helpers ──────────────────────────────────────────────────────────────────

const DB      = "CITIBIKE_SYSTEM_DATA";
const LOGGING = DB + ".LOGGING";

function run(sql, binds) {
    return binds
        ? snowflake.execute({ sqlText: sql, binds: binds })
        : snowflake.execute({ sqlText: sql });
}

function nowUtc() {
    return new Date().toISOString().replace("T", " ").replace("Z", "+00:00");
}

// ── column definitions ───────────────────────────────────────────────────────
// Positional ($1..$N) to be robust to CSV header casing.
// Order must match the DDL column order in citibike_ddl.sql.

const TABLE_COLS = {
    "TRIPS_LEGACY_V1": {
        n: 15,
        names: [
            "tripduration","starttime","stoptime",
            "start_station_id","start_station_name",
            "start_station_latitude","start_station_longitude",
            "end_station_id","end_station_name",
            "end_station_latitude","end_station_longitude",
            "bikeid","usertype","birth_year","gender"
        ]
    },
    "TRIPS_LEGACY_V2": {
        n: 15,
        names: [
            "trip_duration","start_time","stop_time",
            "start_station_id","start_station_name",
            "start_station_latitude","start_station_longitude",
            "end_station_id","end_station_name",
            "end_station_latitude","end_station_longitude",
            "bike_id","user_type","birth_year","gender"
        ]
    },
    "TRIPS_MODERN": {
        n: 13,
        names: [
            "ride_id","rideable_type","started_at","ended_at",
            "start_station_name","start_station_id",
            "end_station_name","end_station_id",
            "start_lat","start_lng","end_lat","end_lng",
            "member_casual"
        ]
    }
};

// ── fetch pending files ───────────────────────────────────────────────────────

const pendingSql = `
    SELECT
        m.OUTPUT_FILE,
        m.REGION,
        m.SCHEMA_KEY,
        m.SF_SCHEMA,
        m.SF_TABLE,
        m.GZIP_SIZE_BYTES,
        COALESCE(l.ATTEMPT, 0) AS LAST_ATTEMPT
    FROM ${LOGGING}.STAGE_MANIFEST m
    LEFT JOIN ${LOGGING}.V_LATEST_INGEST_STATUS l
           ON l.OUTPUT_FILE = m.OUTPUT_FILE
    WHERE COALESCE(l.STATUS, 'PENDING') != 'SUCCESS'
      AND (? = 'all' OR m.REGION = ?)
    ORDER BY m.REGION, m.SF_TABLE, m.OUTPUT_FILE
`;

const pending = run(pendingSql, [P_REGION, P_REGION]);

let totalFiles  = 0;
let successCount = 0;
let failCount    = 0;
let skipCount    = 0;

// ── process each file ─────────────────────────────────────────────────────────

while (pending.next()) {

    const outputFile   = pending.getColumnValue("OUTPUT_FILE");
    const region       = pending.getColumnValue("REGION");
    const schemaKey    = pending.getColumnValue("SCHEMA_KEY");
    const sfSchema     = pending.getColumnValue("SF_SCHEMA");
    const sfTable      = pending.getColumnValue("SF_TABLE");
    const gzipBytes    = pending.getColumnValue("GZIP_SIZE_BYTES") || 0;
    const lastAttempt  = pending.getColumnValue("LAST_ATTEMPT");

    totalFiles++;
    const attempt      = lastAttempt + 1;
    const gzipMb       = (gzipBytes / (1024 * 1024)).toFixed(2);
    const startedAt    = nowUtc();

    // ── look up column mapping ───────────────────────────────────────────────
    const colDef = TABLE_COLS[sfTable];
    if (!colDef) {
        const errMsg = `No column mapping for table '${sfTable}'. Add it to TABLE_COLS in SP.`;
        run(
            `INSERT INTO ${LOGGING}.INGEST_LOG
             (OUTPUT_FILE,REGION,SCHEMA_KEY,SF_SCHEMA,SF_TABLE,GZIP_SIZE_MB,
              ATTEMPT,TRIGGERED_BY,STARTED_AT_UTC,ENDED_AT_UTC,
              STATUS,ROWS_LOADED,ERRORS_SEEN,ERROR_MESSAGE)
             VALUES(?,?,?,?,?,?,?,'SP_INGEST_STAGED_FILES',?,?,
                    'FAILED',0,0,?)`,
            [outputFile,region,schemaKey,sfSchema,sfTable,gzipMb,
             attempt,startedAt,nowUtc(),errMsg]
        );
        failCount++;
        continue;
    }

    // ── build COPY SQL ───────────────────────────────────────────────────────
    const stageFqn    = `${DB}.${sfSchema}.RAW_INGESTION`;
    const tableFqn    = `${DB}.${sfSchema}.${sfTable}`;
    const filename    = outputFile.split("/").pop();   // just the .csv.gz name
    const stagePath   = `@${stageFqn}/${filename}`;

    // positional $1..$N for data cols
    const positional  = colDef.names.map((_, i) => `$${i + 1}`).join(", ");
    const targetCols  = colDef.names.join(", ") + ", _source_file, _source_row_number";

    const copySql = `
        COPY INTO ${tableFqn} (
            ${targetCols}
        )
        FROM (
            SELECT
                ${positional},
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER
            FROM ${stagePath}
        )
        FILE_FORMAT = (
            TYPE                         = CSV
            COMPRESSION                  = GZIP
            SKIP_HEADER                  = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF                      = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL          = TRUE
            DATE_FORMAT                  = AUTO
            TIMESTAMP_FORMAT             = AUTO
        )
        ON_ERROR  = 'CONTINUE'
        PURGE     = TRUE
    `;

    // ── execute COPY and log result ──────────────────────────────────────────
    let status       = "FAILED";
    let rowsLoaded   = 0;
    let errorsSeenN  = 0;
    let copyStatuses = "";
    let errorMsg     = null;

    try {
        const copyResult = run(copySql);

        // COPY result cols: 0=file 1=status 2=rows_parsed 3=rows_loaded
        //                   4=errors_seen 5=first_error_line 6=first_error_char
        //                   7=first_error_col_name 8=first_error_col_type 9=first_error_message
        const statuses = new Set();
        while (copyResult.next()) {
            rowsLoaded  += (copyResult.getColumnValue(4) || 0);   // col index 3 (0-based)
            errorsSeenN += (copyResult.getColumnValue(5) || 0);   // col index 4
            statuses.add(copyResult.getColumnValue(2));           // col index 1 = status
        }
        copyStatuses = Array.from(statuses).join("|");

        if (copyStatuses.includes("LOAD_SKIPPED")) {
            status   = "FAILED";
            errorMsg = "COPY returned LOAD_SKIPPED — file already in Snowflake load history. "
                     + "TRUNCATE the target table and call SP with FORCE handling, or use "
                     + "ALTER TABLE ... REFRESH to clear load history.";
            failCount++;
        } else {
            status = "SUCCESS";
            successCount++;
        }

    } catch(e) {
        errorMsg = e.message;
        failCount++;
    }

    const endedAt = nowUtc();

    run(
        `INSERT INTO ${LOGGING}.INGEST_LOG
         (OUTPUT_FILE,REGION,SCHEMA_KEY,SF_SCHEMA,SF_TABLE,GZIP_SIZE_MB,
          ATTEMPT,TRIGGERED_BY,STARTED_AT_UTC,ENDED_AT_UTC,
          STATUS,ROWS_LOADED,ERRORS_SEEN,COPY_STATUS_RAW,ERROR_MESSAGE)
         VALUES(?,?,?,?,?,?,?,'SP_INGEST_STAGED_FILES',?,?,
                ?,?,?,?,?)`,
        [
            outputFile, region, schemaKey, sfSchema, sfTable, gzipMb,
            attempt, startedAt, endedAt,
            status, rowsLoaded, errorsSeenN, copyStatuses, errorMsg
        ]
    );
}

// ── return summary ────────────────────────────────────────────────────────────

const summary = `SP_INGEST_STAGED_FILES complete | `
              + `region=${P_REGION} | `
              + `total=${totalFiles} | `
              + `success=${successCount} | `
              + `failed=${failCount} | `
              + `skipped=${skipCount}`;

return summary;
$$;
