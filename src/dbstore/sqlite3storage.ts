import { SerializeToJsonString } from '../utils/serialization'
import { Database } from 'sqlite3'
import * as Logger from '../Logger'
import { DBCycle, Cycle } from './cycles'
import { Receipt, DBReceipt } from './receipts'
import { OriginalTxData } from './originalTxsData'
import { DBTransaction, Transaction } from './transactions'
import { DBAccount, AccountsCopy } from './accounts'

interface QueryTiming {
  id: number
  sql: string
  startMs: number
  engineMs?: number
}

const SQL_LOG_MAX_LENGTH = 200
const SQL_ENGINE_WARN_THRESHOLD_MS = 500
const SQL_QUEUE_WARN_THRESHOLD_MS = 250
const SQL_TOTAL_WARN_THRESHOLD_MS = 1000

let queryIdSequence = 0
const pendingQueries = new Map<number, QueryTiming>()
const queuedBySql = new Map<string, number[]>()

export interface DBOriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: object
  sign: object
}

type DBRecord =
  | DBTransaction
  | DBCycle
  | DBOriginalTxData
  | Cycle
  | Receipt
  | DBReceipt
  | OriginalTxData
  | Transaction
  | DBAccount
  | AccountsCopy

export const readFromDB = async (dbPath: string, dbName: string): Promise<Database> => {
  logInfo('Read From DB -> dbName: ', dbName, 'dbPath: ', dbPath)
  const db = new Database(dbPath, (err) => {
    if (err) {
      logError('❌ Error opening database:', err)
      throw err
    }
  })
  db.on('profile', (sql, time) => {
    const engineMs = Number(time)
    const queue = queuedBySql.get(sql)
    const id = queue && queue.length > 0 ? queue[0] : undefined
    if (id === undefined) {
      logWarn('[DB Timing] profile event without pending query', {
        pid: process.pid,
        engineMs,
        sql: formatSqlForLog(sql),
      })
      return
    }
    const entry = pendingQueries.get(id)
    if (!entry) {
      logWarn('[DB Timing] profile missing pending entry', {
        pid: process.pid,
        engineMs,
        sql: formatSqlForLog(sql),
      })
      return
    }
    entry.engineMs = engineMs
    if (engineMs > SQL_ENGINE_WARN_THRESHOLD_MS) {
      logWarn('[DB Engine] Slow engine execution detected', {
        pid: process.pid,
        engineMs: Number(engineMs.toFixed(2)),
        sql: formatSqlForLog(sql),
      })
    }
  })
  await run(db, 'PRAGMA journal_mode=WAL')
  await run(db, 'PRAGMA synchronous = NORMAL')
  await run(db, 'PRAGMA temp_store = MEMORY')
  await run(db, 'PRAGMA query_only = 1') // Read-only mode - reduces lock contention for queries
  await run(db, 'PRAGMA cache_size = -128000') // Increased to ~128MB cache for better performance
  await run(db, 'PRAGMA wal_autocheckpoint = 5000') // Checkpoint every 5000 pages (less frequent = less lock contention)
  await run(db, 'PRAGMA mmap_size = 536870912') // 512MB memory-mapped I/O for faster reads (reduced disk I/O)
  await run(db, 'PRAGMA busy_timeout = 30000') // Wait up to 30s if database is locked
  await run(db, 'PRAGMA threads = 4') // Use up to 4 threads for parallel operations
  logInfo(`Database ${dbName} Initialized!`)
  logInfo(`✅ Database: ${dbName} initialized with performance optimizations`)
  return db
}
/**
 * Closes Database Connection Gracefully
 */
export async function close(db: Database, dbName: string): Promise<void> {
  try {
    logInfo(`Terminating ${dbName} Database/Indexer Connections...`)
    await new Promise<void>((resolve, reject) => {
      db.close((err) => {
        if (err) {
          logError(`Error closing ${dbName} Database Connection.`)
          logError(err)
          reject(err)
        } else {
          logInfo(`${dbName} Database connection closed.`)
          resolve()
        }
      })
    })
  } catch (err) {
    logError(`Error thrown in ${dbName} db close() function: `)
    logError(err)
  }
}

export async function run(db: Database, sql: string, params = [] || {}): Promise<{ id: number } | Error> {
  const entry = registerQuery(sql)
  return new Promise((resolve, reject) => {
    const finalize = (): void => {
      setImmediate(() => {
        logTiming('run', entry)
        cleanupQuery(entry)
      })
    }
    db.run(sql, params, function (err) {
      if (err) {
        logError('Error running sql ' + sql)
        logError(err)
        finalize()
        reject(err)
      } else {
        finalize()
        resolve({ id: this.lastID })
      }
    })
  })
}

export async function get(db: Database, sql: string, params = []): Promise<DBRecord> {
  const entry = registerQuery(sql)
  return new Promise((resolve, reject) => {
    const finalize = (rows?: number): void => {
      setImmediate(() => {
        logTiming('get', entry, rows)
        cleanupQuery(entry)
      })
    }
    db.get(sql, params, (err, result) => {
      if (err) {
        logError('Error running sql: ' + sql)
        logError(err)
        finalize()
        reject(err)
      } else {
        finalize(result ? 1 : 0)
        resolve(result as DBRecord)
      }
    })
  })
}

export async function all(db: Database, sql: string, params = []): Promise<DBRecord[]> {
  const entry = registerQuery(sql)
  return new Promise((resolve, reject) => {
    const finalize = (rowsCount?: number): void => {
      setImmediate(() => {
        logTiming('all', entry, rowsCount)
        cleanupQuery(entry)
      })
    }
    db.all(sql, params, (err, rows) => {
      if (err) {
        logError('Error running sql: ' + sql)
        logError(err)
        finalize()
        reject(err)
      } else {
        finalize(rows ? rows.length : 0)
        resolve(rows as DBRecord[])
      }
    })
  })
}

export function extractValues(object: unknown): (string | number | boolean | null)[] | void {
  try {
    const inputs: (string | number | boolean | null)[] = []
    for (const column of Object.keys(object)) {
      if (Object.prototype.hasOwnProperty.call(object, column)) {
        // eslint-disable-next-line security/detect-object-injection
        let value = object[column]
        if (typeof value === 'object') value = SerializeToJsonString(value)
        inputs.push(value as string | number | boolean | null)
      }
    }
    return inputs
  } catch (e) {
    logError(e)
  }
}

export function extractValuesFromArray(arr: DBRecord[]): (string | number | boolean | null)[] | void {
  try {
    const inputs = []
    for (const object of arr) {
      for (const column of Object.keys(object)) {
        if (Object.prototype.hasOwnProperty.call(object, column)) {
          let value = Reflect.get(object, column)
          if (typeof value === 'object') value = SerializeToJsonString(value)
          inputs.push(value)
        }
      }
    }
    return inputs
  } catch (e) {
    logError(e)
  }
}

function registerQuery(sql: string): QueryTiming {
  const entry: QueryTiming = {
    id: ++queryIdSequence,
    sql,
    startMs: Date.now(),
  }
  pendingQueries.set(entry.id, entry)
  let queue = queuedBySql.get(sql)
  if (!queue) {
    queue = []
    queuedBySql.set(sql, queue)
  }
  queue.push(entry.id)
  return entry
}

function cleanupQuery(entry: QueryTiming): void {
  pendingQueries.delete(entry.id)
  const queue = queuedBySql.get(entry.sql)
  if (!queue) return
  const index = queue.indexOf(entry.id)
  if (index !== -1) queue.splice(index, 1)
  if (queue.length === 0) queuedBySql.delete(entry.sql)
}

function logTiming(operation: string, entry: QueryTiming, rows?: number): void {
  const totalMs = Date.now() - entry.startMs
  const engineMs = entry.engineMs ?? 0
  const queueMs = Math.max(0, totalMs - engineMs)
  const payload = {
    pid: process.pid,
    operation,
    totalMs: Number(totalMs.toFixed(2)),
    queueMs: Number(queueMs.toFixed(2)),
    engineMs: Number(engineMs.toFixed(2)),
    sql: formatSqlForLog(entry.sql),
    rows,
  }

  if (totalMs > SQL_TOTAL_WARN_THRESHOLD_MS || queueMs > SQL_QUEUE_WARN_THRESHOLD_MS) {
    const payloadWithoutSql = { ...payload, sql: undefined }
    logWarn('[DB Timing]', JSON.stringify(payloadWithoutSql, null, 2))
  }
}

function logInfo(...args: any[]): void {
  if (Logger.mainLogger) Logger.mainLogger.info(args)
  else console.log(args)
}

function logWarn(...args: any[]): void {
  if (Logger.mainLogger) Logger.mainLogger.warn(args)
  else console.warn(args)
}

function logDebug(...args: any[]): void {
  if (Logger.mainLogger) Logger.mainLogger.debug(args)
  else console.log(args)
}

function logError(...args: any[]): void {
  if (Logger.mainLogger) Logger.mainLogger.error(args)
  else console.error(args)
}

function formatSqlForLog(sql: string): string {
  const normalized = sql.replace(/\s+/g, ' ').trim()
  if (normalized.length <= SQL_LOG_MAX_LENGTH) return normalized
  return `${normalized.slice(0, SQL_LOG_MAX_LENGTH - 3)}...`
}
