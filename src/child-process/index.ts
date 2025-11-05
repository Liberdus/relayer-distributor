import * as url from 'url'
import * as net from 'net'
import * as http from 'http'
import * as Logger from '../Logger'

import { config, distributorMode } from '../Config'
import DataLogReader from '../log-reader'
import fastifyCors from '@fastify/cors'
import fastifyCompress from '@fastify/compress'
import fastifyTimingPlugin from './fastifyTimingPlugin'
import type { Worker } from 'node:cluster'
import { handleSocketRequest, registerParentProcessListener, registerDataReaderListeners } from './child'
import Fastify, { FastifyInstance } from 'fastify'
import fastifyRateLimit from '@fastify/rate-limit'
import { Utils as StringUtils } from '@shardeum-foundation/lib-types'
import { registerRoutes, validateRequestData } from '../api'
import { healthCheckRouter } from '../routes/healthCheck'

interface ClientRequestDataInterface {
  header: object
  socket: net.Socket
}

let httpServer: http.Server
export const workerClientMap = new Map<Worker, string[]>()

const connectToSocketClient = (clientKey: string, clientRequestData: ClientRequestDataInterface): void => {
  try {
    handleSocketRequest({ ...clientRequestData.header, socket: clientRequestData.socket, clientKey })
  } catch (e) {
    throw new Error(`Error in connectToSocketClient(): ${e}`)
  }
}

export const initHttpServer = async (worker: Worker): Promise<void> => {
  const serverFactory = (handler: (req: http.IncomingMessage, res: http.ServerResponse) => void): http.Server => {
    httpServer = http.createServer((req, res) => {
      handler(req, res)
    })

    // Optimize TCP socket settings for large response payloads
    httpServer.on('connection', (socket) => {
      try {
        // Disable Nagle's algorithm - send data immediately without buffering
        // Reduces latency for large responses
        socket.setNoDelay(true)
        // Enable keep-alive to reuse connections
        socket.setKeepAlive(true, 30000)
      } catch (error) {
        Logger.mainLogger.warn('Failed to set socket options:', error)
      }
    })

    return httpServer
  }

  const fastifyServer = Fastify({
    serverFactory,
    logger: true,
    connectionTimeout: 0, // Disable connection timeout (allow long-running requests)
    bodyLimit: 1 * 1024 * 1024, // 1MB limit for incoming REQUEST bodies (not responses)
  })
  await fastifyServer.register(fastifyTimingPlugin)

  // Register compression middleware  [ Optional - reduces payload size - but doesn't seem to help much in syncing performance ]
  if (config.FASTIFY_COMPRESSION_ENABLED) {
    await fastifyServer.register(fastifyCompress, {
      global: true,
      threshold: 1024, // Only compress responses larger than 1KB
      encodings: ['gzip', 'deflate'],
    })
    Logger.mainLogger.info('Fastify compression enabled (gzip, deflate)')
  } else {
    Logger.mainLogger.info('Fastify compression disabled')
  }

  await fastifyServer.register(fastifyCors)
  await fastifyServer.register(fastifyRateLimit, {
    global: true,
    max: config.RATE_LIMIT,
    timeWindow: 10,
    allowList: ['127.0.0.1', '0.0.0.0'], // Excludes local IPs from rate limits
  })
  await fastifyServer.register(healthCheckRouter)

  fastifyServer.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      const jsonString = typeof body === 'string' ? body : body.toString('utf8')
      done(null, StringUtils.safeJsonParse(jsonString))
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  fastifyServer.setReplySerializer(function (payload) {
    // Time the serialization operation
    const serializeStart = Date.now()
    const stringified = StringUtils.safeStringify(payload)
    const serializeElapsed = Date.now() - serializeStart
    const sizeBytes = Buffer.byteLength(stringified)

    // Debug: Check if this.request is available
    console.log('[Serializer Debug]', {
      hasThis: !!this,
      hasRequest: !!(this && this.request),
      serializeMs: serializeElapsed,
      sizeBytes,
    })

    // Store serialization metrics on request for API timing logs
    if (this && this.request) {
      this.request._serializeMs = serializeElapsed
      this.request._serializedBytes = sizeBytes
      console.log('[Serializer Debug] Stored metrics on request:', {
        _serializeMs: this.request._serializeMs,
        _serializedBytes: this.request._serializedBytes,
      })
    }

    return stringified
  })

  // Register API routes
  registerRoutes(fastifyServer as FastifyInstance<http.Server, http.IncomingMessage, http.ServerResponse>)

  initSocketServer(httpServer, worker)

  registerParentProcessListener()
  // Start server and bind to port on all interfaces
  fastifyServer.ready(() => {
    httpServer.listen(config.DISTRIBUTOR_PORT, () => {
      console.log(`Distributor-Server (${process.pid}) listening on port ${config.DISTRIBUTOR_PORT}.`)
      Logger.mainLogger.debug(`API-Server started on Worker Process (${process.pid}).`)
      return
    })

    httpServer.on('error', (err) => {
      Logger.mainLogger.error('Distributor failed to start.', err)
      process.exit(1)
    })
  })
}

const initSocketServer = async (httpServer: http.Server, worker: Worker): Promise<void> => {
  // Handles incoming upgrade requests from clients (to upgrade to a Socket connection)
  httpServer.on('upgrade', (req: http.IncomingMessage, socket: net.Socket, head: Buffer) => {
    const queryObject = url.parse(req.url!, true).query
    const decodedData = decodeURIComponent(queryObject.data as string)
    const clientData = StringUtils.safeJsonParse(decodedData)

    const auth = validateRequestData(clientData, {
      collectorInfo: 'o',
      sender: 's',
      sign: 'o',
    })
    if (auth.success) {
      const clientKey = clientData.sender
      connectToSocketClient(clientKey, {
        header: { headers: req.headers, method: req.method, head, clientKey },
        socket,
      })
    } else {
      Logger.mainLogger.debug(`❌ Unauthorized Client Request from ${req.headers.host}, Reason: ${auth.error}`)

      socket.write('HTTP/1.1 401 Unauthorized\r\n')
      socket.write('Content-Type: text/plain\r\n')
      socket.write('Connection: close\r\n')
      socket.write('Unauthorized: Authentication failed\r\n')

      socket.end()
      return
    }
  })
}

export const initWorker = async (): Promise<void> => {
  try {
    if (config.distributorMode === distributorMode.MQ) {
      return
    }
    const DATA_LOG_PATH = config.DATA_LOG_DIR
    const cycleReader = new DataLogReader(DATA_LOG_PATH, 'cycle')
    const receiptReader = new DataLogReader(DATA_LOG_PATH, 'receipt')
    const originalTxReader = new DataLogReader(DATA_LOG_PATH, 'originalTx')
    await Promise.all([receiptReader.init(), cycleReader.init(), originalTxReader.init()])
    registerDataReaderListeners(cycleReader)
    registerDataReaderListeners(receiptReader)
    registerDataReaderListeners(originalTxReader)
  } catch (e) {
    if (e.code === 'ENOENT') {
      console.error(
        '❌ Path to the data-logs directory does not exist. Please check the path in the config file.\n Current Path: ',
        config.DATA_LOG_DIR
      )
      process.exit(0)
    } else {
      console.error('Error in Child Process: ', e.message, e.code)
    }
  }
}
