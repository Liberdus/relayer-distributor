import fp from 'fastify-plugin'
import * as Logger from '../Logger'

declare module 'fastify' {
  interface FastifyRequest {
    _t0?: bigint
    _tHandlerStart?: bigint
    _tOnSend?: bigint
    _timingQueueMs?: number
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

  fastify.addHook('onSend', async (req) => {
    req._tOnSend = process.hrtime.bigint()
  })

  fastify.addHook('onResponse', async (req, reply) => {
    const t0 = req._t0 ?? process.hrtime.bigint()
    const tHandlerStart = req._tHandlerStart ?? t0
    const tOnSend = req._tOnSend ?? tHandlerStart
    const tEnd = process.hrtime.bigint()

    const preMs = req._timingQueueMs ?? Number(tHandlerStart - t0) / 1e6 // parsing/validation hooks
    const handler = Number(tOnSend - tHandlerStart) / 1e6 // your route
    const tail = Number(tEnd - tOnSend) / 1e6 // serialize/flush
    const total = Number(tEnd - t0) / 1e6

    // routerPath may be undefined outside handler; fall back to raw URL
    const route = (req.routerPath ?? req.routeOptions?.url ?? req.raw.url) || 'unknown'

    const timingLog = {
      reqId: req.id,
      pid: process.pid,
      method: req.method,
      route,
      statusCode: reply.statusCode,
      preHandlerPipelineMs: +preMs.toFixed(1),
      handlerMs: +handler.toFixed(1),
      tailMs: +tail.toFixed(1),
      totalMs: +total.toFixed(1),
    }

    req.log.info({
      msg: '[Fastify Timing]',
      ...timingLog,
    })
    if (Logger.mainLogger) {
      Logger.mainLogger.info('[Fastify Timing]', JSON.stringify(timingLog))
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
