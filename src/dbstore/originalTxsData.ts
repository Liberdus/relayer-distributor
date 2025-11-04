import * as db from './sqlite3storage'
import { originalTxDataDatabase } from '.'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'
import { isNumber } from '../utils/Utils'

export interface OriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: unknown
  // sign: Signature
}

export interface OriginalTxsDataCountByCycle {
  cycle: number
  originalTxsData: number
}

export type DBOriginalTxData = OriginalTxData & {
  originalTxData: string
}

export async function insertOriginalTxData(OriginalTxData: OriginalTxData): Promise<void> {
  try {
    const fields = Object.keys(OriginalTxData).join(', ')
    const placeholders = Object.keys(OriginalTxData).fill('?').join(', ')
    const values = db.extractValues(OriginalTxData)
    if (!values || values.length === 0) {
      throw new Error(`No values extracted from OriginalTxData with txId ${OriginalTxData.txId}`)
    }
    const sql = 'INSERT OR REPLACE INTO originalTxsData (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(originalTxDataDatabase, sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted OriginalTxData', OriginalTxData.txId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert OriginalTxData or it is already stored in to database',
      OriginalTxData.txId
    )
  }
}

export async function bulkInsertOriginalTxsData(originalTxsData: OriginalTxData[]): Promise<void> {
  try {
    const fields = Object.keys(originalTxsData[0]).join(', ')
    const placeholders = Object.keys(originalTxsData[0]).fill('?').join(', ')
    const values = db.extractValuesFromArray(originalTxsData)
    if (!values || values.length === 0) {
      throw new Error(`No values extracted from originalTxsData. Number of originalTxsData: ${originalTxsData.length}`)
    }
    let sql = 'INSERT OR REPLACE INTO originalTxsData (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < originalTxsData.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(originalTxDataDatabase, sql, values)
    Logger.mainLogger.debug('Successfully inserted OriginalTxsData', originalTxsData.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert OriginalTxsData', originalTxsData.length)
    throw e // check with Achal/Jai
  }
}

export async function queryOriginalTxDataCount(startCycle?: number, endCycle?: number): Promise<number> {
  let originalTxsData
  try {
    let sql = `SELECT COUNT(*) FROM originalTxsData`
    const values: number[] = []
    if (isNumber(startCycle) && isNumber(endCycle)) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    originalTxsData = await db.get(originalTxDataDatabase, sql, values)
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData count', originalTxsData)
  }
  return originalTxsData['COUNT(*)'] || 0
}

export async function queryOriginalTxsData(
  skip = 0,
  limit = 10,
  startCycle?: number,
  endCycle?: number
): Promise<OriginalTxData[]> {
  let originalTxsData
  try {
    let sql = `SELECT * FROM originalTxsData`
    const sqlSuffix = ` ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    const values: number[] = []
    if (isNumber(startCycle) && isNumber(endCycle)) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    sql += sqlSuffix
    originalTxsData = await db.all(originalTxDataDatabase, sql, values)
    originalTxsData.forEach((originalTxData: DBOriginalTxData) => {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
    })
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData originalTxsData', originalTxsData)
  }
  return originalTxsData
}

export async function queryOriginalTxDataByTxId(txId: string): Promise<OriginalTxData | null> {
  try {
    const sql = `SELECT * FROM originalTxsData WHERE txId=?`
    const originalTxData = (await db.get(originalTxDataDatabase, sql, [txId])) as DBOriginalTxData
    if (originalTxData) {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('OriginalTxData txId', originalTxData)
    }
    return originalTxData as OriginalTxData
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryOriginalTxDataCountByCycles(
  start: number,
  end: number
): Promise<OriginalTxsDataCountByCycle[]> {
  let originalTxsData
  try {
    const sql = `SELECT cycle, COUNT(*) FROM originalTxsData GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    originalTxsData = await db.all(originalTxDataDatabase, sql, [start, end])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData count by cycle', originalTxsData)
  }
  if (originalTxsData.length > 0) {
    originalTxsData.forEach((OriginalTxData) => {
      OriginalTxData['originalTxsData'] = OriginalTxData['COUNT(*)']
      delete OriginalTxData['COUNT(*)']
    })
  }
  return originalTxsData
}

export async function queryLatestOriginalTxs(count: number): Promise<DBOriginalTxData[] | void> {
  try {
    const sql = `SELECT * FROM originalTxsData ORDER BY cycle DESC, timestamp DESC LIMIT ${count ? count : 100}`
    const originalTxs = (await db.all(originalTxDataDatabase, sql)) as DBOriginalTxData[]
    if (originalTxs.length > 0) {
      originalTxs.forEach((tx: DBOriginalTxData) => {
        if (tx.originalTxData) tx.originalTxData = DeSerializeFromJsonString(tx.originalTxData)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Latest Original-Tx: ', originalTxs)
    }
    return originalTxs
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

/**
 * Query originalTxsData for a specific cycle using composite cursor pagination
 * This prevents data loss when multiple transactions have the same timestamp
 * @param cycle - The cycle number to query
 * @param afterTimestamp - Cursor timestamp (fetch records after this)
 * @param afterTxId - Cursor txId (for records with same timestamp)
 * @param beforeTimestamp - Optional upper bound timestamp
 * @param limit - Maximum number of records to return
 */
export async function queryOriginalTxsDataByCycleCursor(
  cycle: number,
  afterTimestamp: number,
  afterTxId: string = '',
  beforeTimestamp?: number,
  limit: number = 500
): Promise<OriginalTxData[]> {
  let originalTxsData: OriginalTxData[] = []
  try {
    // Use cycle as the source of truth, no timestamp upper bound
    // This prevents data loss if transactions have timestamps outside cycle boundaries
    const sql = `
      SELECT * FROM originalTxsData
      WHERE cycle = ?
        AND (timestamp > ? OR (timestamp = ? AND txId > ?))
      ORDER BY timestamp ASC, txId ASC
      LIMIT ?
    `
    const params: (number | string)[] = [cycle, afterTimestamp, afterTimestamp, afterTxId, limit]

    originalTxsData = (await db.all(originalTxDataDatabase, sql, params)) as DBOriginalTxData[]
    if (originalTxsData.length > 0) {
      originalTxsData.forEach((tx: DBOriginalTxData) => {
        if (tx.originalTxData) tx.originalTxData = DeSerializeFromJsonString(tx.originalTxData)
      })
    }
  } catch (e) {
    Logger.mainLogger.error('Error in queryOriginalTxsDataByCycleCursor:', e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      `OriginalTxsData by cycle cursor - cycle: ${cycle}, count: ${originalTxsData.length}, afterTimestamp: ${afterTimestamp}, afterTxId: ${afterTxId}`
    )
  }
  return originalTxsData as OriginalTxData[]
}

/**
 * Query originalTxsData across multiple cycles using composite cursor pagination
 * Optimized for fetching small cycles in batches to reduce HTTP overhead
 *
 * Example usage:
 *   Request: cycles=[100,101,102], limit=500
 *   Response: 450 originalTxs from cycles 100-101
 *   Next request: cycles=[101,102], afterCycle=101, afterTimestamp=X, afterTxId=Y
 *
 * @param cycles - Array of cycle numbers to query (must be in ascending order)
 * @param afterCycle - Cursor cycle number (resume from this cycle)
 * @param afterTimestamp - Cursor timestamp within the afterCycle
 * @param afterTxId - Cursor txId (for records with same timestamp)
 * @param limit - Maximum number of records to return across all cycles
 */
export async function queryOriginalTxsDataMultiCycleCursor(
  cycles: number[],
  afterCycle: number,
  afterTimestamp: number,
  afterTxId: string = '',
  limit: number = 500
): Promise<OriginalTxData[]> {
  let originalTxsData: OriginalTxData[] = []
  try {
    if (cycles.length === 0) {
      return originalTxsData
    }

    // Build WHERE clause for cycle range
    const minCycle = Math.min(...cycles)
    const maxCycle = Math.max(...cycles)

    // Fetch across cycle range with composite cursor
    const sql = `
      SELECT * FROM originalTxsData
      WHERE cycle BETWEEN ? AND ?
        AND (
          cycle > ?
          OR (cycle = ? AND timestamp > ?)
          OR (cycle = ? AND timestamp = ? AND txId > ?)
        )
      ORDER BY cycle ASC, timestamp ASC, txId ASC
      LIMIT ?
    `

    const params: (number | string)[] = [
      minCycle,
      maxCycle,
      afterCycle,
      afterCycle,
      afterTimestamp,
      afterCycle,
      afterTimestamp,
      afterTxId,
      limit,
    ]

    // Time the SQL query
    // NOTE: queryElapsed = queueMs (lock wait) + engineMs (actual SQL execution)
    // See sqlite3storage.ts [DB Timing] logs for queueMs vs engineMs breakdown
    const queryStartTime = Date.now()
    originalTxsData = (await db.all(originalTxDataDatabase, sql, params)) as DBOriginalTxData[]
    const queryElapsed = Date.now() - queryStartTime

    // Time the deserialization
    const deserializeStartTime = Date.now()
    if (originalTxsData.length > 0) {
      originalTxsData.forEach((tx: DBOriginalTxData) => {
        if (tx.originalTxData) tx.originalTxData = DeSerializeFromJsonString(tx.originalTxData)
      })
    }
    const deserializeElapsed = Date.now() - deserializeStartTime

    // Calculate metrics
    const totalElapsed = queryElapsed + deserializeElapsed
    const avgTimePerRecord = originalTxsData.length > 0 ? (totalElapsed / originalTxsData.length).toFixed(2) : '0'
    const hitLimit = originalTxsData.length === limit
    const cycleRange = `${minCycle}-${maxCycle}`

    // Log detailed timing breakdown with enhanced diagnostics
    // Lower threshold to 250ms to catch lock contention (high queueMs)
    if (config.VERBOSE || queryElapsed > 250) {
      Logger.mainLogger.debug(
        `[DB Timing] OriginalTxsData multi-cycle cursor - ${process.pid} : ` +
          `range=${cycleRange}, cursor=(cycle:${afterCycle}, ts:${afterTimestamp}, id:${afterTxId.slice(0, 8)}...), ` +
          `query=${queryElapsed}ms, deserialize=${deserializeElapsed}ms, count=${originalTxsData.length}/${limit}, ` +
          `avg=${avgTimePerRecord}ms/rec, hitLimit=${hitLimit}`
      )
    }

    // Warn on slow queries with more context
    if (queryElapsed > 500) {
      Logger.mainLogger.warn(
        `[SLOW QUERY] OriginalTxsData multi-cycle cursor - ${process.pid} took ${queryElapsed}ms - ` +
          `range=${cycleRange}, afterCycle=${afterCycle}, count=${originalTxsData.length}, ` +
          `High queryElapsed usually indicates lock contention (high queueMs waiting for database lock). ` +
          `Check sqlite3storage [DB Timing] logs for queueMs vs engineMs breakdown. ` +
          `If queueMs > 250ms, consider increasing NUMBER_OF_WORKERS or reducing PARALLEL_SYNC_CONCURRENCY.`
      )
    }
  } catch (e) {
    Logger.mainLogger.error('Error in queryOriginalTxsDataMultiCycleCursor:', e)
  }
  return originalTxsData as OriginalTxData[]
}
