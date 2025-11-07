import * as db from './sqlite3storage'
import { originalTxDataDatabase } from '.'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'
import { isNumber } from '../utils/Utils'
import { MAX_ORIGINAL_TXS_PER_REQUEST } from '../api'

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
 * Query originalTxsData between cycles range with ( timestamp + txId ) pagination
 * Optimized for fetching small cycles in batches to reduce HTTP overhead
 *
 * @param startCycle - Start cycle number to query (inclusive)
 * @param endCycle - End cycle number to query (inclusive)
 * @param afterTimestamp - timestamp (fetch records after the given timestamp)
 * @param afterTxId - txId (for records with same timestamp)
 * @param limit - Maximum number of records to return
 */
export async function queryOriginalTxsDataByCycleRange(
  startCycle: number,
  endCycle: number,
  afterTimestamp: number,
  afterTxId = '',
  limit: number = MAX_ORIGINAL_TXS_PER_REQUEST
): Promise<OriginalTxData[]> {
  let originalTxsData: OriginalTxData[] = []
  try {
    // Fetch between cycles range
    let sql = `
      SELECT * FROM originalTxsData
      WHERE cycle BETWEEN ? AND ?
    `
    const params: (number | string)[] = [startCycle, endCycle]
    if (afterTimestamp > 0 && afterTxId !== '') {
      sql += ` AND timestamp = ? AND txId > ?`
      params.push(afterTimestamp, afterTxId)
    } else if (afterTimestamp > 0) {
      sql += ` AND timestamp > ?`
      params.push(afterTimestamp)
    }
    sql += ` ORDER BY cycle ASC, timestamp ASC, txId ASC
      LIMIT ?
    `
    params.push(limit)

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

    // Log detailed timing breakdown with enhanced diagnostics
    // Lower threshold to 250ms to catch lock contention (high queueMs)
    if (config.VERBOSE || queryElapsed > 250) {
      Logger.mainLogger.debug(
        `[DB Timing] OriginalTxsData by cycle range - ${process.pid} : ` +
          `range=${startCycle}-${endCycle}, ts:${afterTimestamp}, id:${afterTxId.slice(0, 8)}...), ` +
          `query=${queryElapsed}ms, deserialize=${deserializeElapsed}ms, count=${originalTxsData.length}/${limit}, ` +
          `avg=${avgTimePerRecord}ms/rec, hitLimit=${hitLimit}`
      )
    }

    // Warn on slow queries with more context
    if (queryElapsed > 500) {
      Logger.mainLogger.warn(
        `[SLOW QUERY] OriginalTxsData by cycle range - ${process.pid} took ${queryElapsed}ms - ` +
          `range=${startCycle}-${endCycle}, count=${originalTxsData.length}, ` +
          `High queryElapsed usually indicates lock contention (high queueMs waiting for database lock). ` +
          `Check sqlite3storage [DB Timing] logs for queueMs vs engineMs breakdown. ` +
          `If queueMs > 250ms, consider increasing NUMBER_OF_WORKERS or reducing PARALLEL_SYNC_CONCURRENCY.`
      )
    }
  } catch (e) {
    Logger.mainLogger.error('Error in queryOriginalTxsDataByCycleRange:', e)
  }
  return originalTxsData as OriginalTxData[]
}
