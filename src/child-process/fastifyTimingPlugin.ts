import fp from 'fastify-plugin'
import { Stream } from 'stream'
import { Socket } from 'net'
import * as Logger from '../Logger'
import { config } from '../Config'

declare module 'fastify' {
  interface FastifyRequest {
    _t0?: bigint
    _tHandlerStart?: bigint
    _tPreSerialization?: bigint
    _tOnSend?: bigint
    _timingQueueMs?: number
    _payloadSizeUncompressed?: number
    _bytesWrittenBefore?: number
    _socket?: Socket
  }
}

export default fp(async function fastifyTimingPlugin(fastify) {
  fastify.addHook('onRequest', async (req) => {
    req._t0 = process.hrtime.bigint()
  })

  fastify.addHook('preHandler', async (req) => {
    const now = process.hrtime.bigint()
    if (req._t0) {
      req._timingQueueMs = Number(now - req._t0) / 1e6
    }
    req._tHandlerStart = now
  })

  // [TODO - Fix the issue with preSerialization hook]
  // fastify.addHook('preSerialization', async (req) => {
  //   req._tPreSerialization = process.hrtime.bigint()
  // })

  fastify.addHook('onSend', async (req, reply, payload) => {
    const tOnSend = process.hrtime.bigint()
    req._tOnSend = tOnSend

    // Capture uncompressed payload size
    if (payload) {
      if (typeof payload === 'string') {
        req._payloadSizeUncompressed = Buffer.byteLength(payload)
      } else if (Buffer.isBuffer(payload)) {
        req._payloadSizeUncompressed = payload.length
      } else if (payload instanceof Stream) {
        // For streams, we can't easily measure size here
        req._payloadSizeUncompressed = undefined
      } else {
        // For other types (objects, etc.), try to get size
        const str = typeof payload === 'object' ? JSON.stringify(payload) : String(payload)
        req._payloadSizeUncompressed = Buffer.byteLength(str)
      }
    }

    // Capture bytes written to socket before response headers/body are sent
    // Store socket reference as it might not be accessible later
    const socket = reply.raw.socket
    if (socket && 'bytesWritten' in socket) {
      req._socket = socket as Socket
      req._bytesWrittenBefore = socket.bytesWritten as number
    }
  })

  fastify.addHook('onResponse', async (req, reply) => {
    const t0 = req._t0 ?? process.hrtime.bigint()
    const tHandlerStart = req._tHandlerStart ?? t0
    const tPreSerialization = req._tPreSerialization ?? tHandlerStart
    const tOnSend = req._tOnSend ?? tPreSerialization
    const tEnd = process.hrtime.bigint()

    const preMs = req._timingQueueMs ?? Number(tHandlerStart - t0) / 1e6 // parsing/validation hooks
    const handler = Number(tOnSend - tHandlerStart) / 1e6 // your route
    const serializeMs = Number(tOnSend - tPreSerialization) / 1e6
    const tail = Number(tEnd - tOnSend) / 1e6 // serialize/flush
    const total = Number(tEnd - t0) / 1e6

    // routerPath may be undefined outside handler; fall back to raw URL
    const route = (req.routerPath ?? req.routeOptions?.url ?? req.raw.url) || 'unknown'

    // Get payload sizes
    const uncompressedSize = req._payloadSizeUncompressed

    // Try to get compressed size from multiple sources:
    // 1. Content-Length header (works when compression is off or header is set)
    const contentLength = reply.getHeader('content-length') || reply.raw.getHeader('content-length')
    let compressedSize = contentLength ? parseInt(String(contentLength), 10) : undefined

    // 2. Calculate from bytes written to socket (works with chunked/compressed responses)
    if (!compressedSize && req._bytesWrittenBefore !== undefined && req._socket) {
      const socket = req._socket
      if (socket && 'bytesWritten' in socket) {
        // Wait for the 'finish' event to ensure all data is written
        await new Promise<void>((resolve) => {
          if (reply.raw.writableFinished) {
            resolve()
          } else {
            reply.raw.once('finish', () => resolve())
          }
        })

        const bytesWrittenAfter = socket.bytesWritten as number
        const totalBytes = bytesWrittenAfter - req._bytesWrittenBefore

        // Subtract estimated header size (typically 100-400 bytes for HTTP headers + chunked encoding overhead)
        const estimatedHeaderSize = 250
        if (totalBytes > estimatedHeaderSize) {
          compressedSize = totalBytes - estimatedHeaderSize
        }
      }
    }

    // Calculate compression ratio if both sizes are available
    const compressionRatio =
      uncompressedSize && compressedSize && uncompressedSize > 0
        ? +(compressedSize / uncompressedSize).toFixed(3)
        : undefined

    interface TimingLog {
      pid: number
      reqId: string
      method: string
      route: string
      statusCode: number
      preHandlerPipelineMs: number
      handlerMs: number
      serializeMs?: number
      tailMs: number
      totalMs: number
      payloadBytes?: number
      payloadBytesUncompressed?: number
      compressionRatio?: number
      compressionSavings?: string
    }

    const timingLog: TimingLog = {
      pid: process.pid,
      reqId: req.id,
      method: req.method,
      route,
      statusCode: reply.statusCode,
      preHandlerPipelineMs: +preMs.toFixed(1),
      handlerMs: +handler.toFixed(1),
      serializeMs: +serializeMs.toFixed(1),
      tailMs: +tail.toFixed(1),
      totalMs: +total.toFixed(1),
    }

    // Add payload size information if available
    if (compressedSize !== undefined) {
      timingLog.payloadBytes = compressedSize
    }
    if (uncompressedSize !== undefined && uncompressedSize !== compressedSize) {
      timingLog.payloadBytesUncompressed = uncompressedSize
    }
    if (compressionRatio !== undefined && compressionRatio < 1) {
      timingLog.compressionRatio = compressionRatio
      timingLog.compressionSavings = `${((1 - compressionRatio) * 100).toFixed(1)}%`
    }

    // Only log timing information if enabled in config
    if (config.FASTIFY_TIMING_LOGS_ENABLED) {
      req.log.info({
        msg: '[Fastify Timing]',
        ...timingLog,
      })
      if (Logger.mainLogger) {
        Logger.mainLogger.info('[Fastify Timing]', JSON.stringify(timingLog))
      }
    }

    // Visible in browser DevTools → Network → Headers
    const parts = [
      `pre;dur=${preMs.toFixed(1)}`,
      `handler;dur=${handler.toFixed(1)}`,
      `tail;dur=${tail.toFixed(1)}`,
      `total;dur=${total.toFixed(1)}`,
    ]
    reply.header('Server-Timing', parts.join(', '))
  })
})
