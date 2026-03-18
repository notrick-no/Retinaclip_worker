/**
 * Worker 日志工具
 * 统一的日志格式，支持级别过滤和结构化输出
 */

type LogLevel = 'debug' | 'info' | 'warn' | 'error'

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
}

let currentLevel: LogLevel = 'info'

export function setLogLevel(level: LogLevel) {
  currentLevel = level
}

function shouldLog(level: LogLevel): boolean {
  return LOG_LEVELS[level] >= LOG_LEVELS[currentLevel]
}

function formatTimestamp(): string {
  return new Date().toISOString()
}

function formatMessage(level: LogLevel, module: string, message: string, meta?: Record<string, any>): string {
  const timestamp = formatTimestamp()
  const prefix = `[${timestamp}] [${level.toUpperCase().padEnd(5)}] [${module}]`
  
  if (meta && Object.keys(meta).length > 0) {
    return `${prefix} ${message} ${JSON.stringify(meta)}`
  }
  return `${prefix} ${message}`
}

/**
 * 创建模块级别的 logger
 */
export function createLogger(module: string) {
  return {
    debug(message: string, meta?: Record<string, any>) {
      if (shouldLog('debug')) {
        console.debug(formatMessage('debug', module, message, meta))
      }
    },

    info(message: string, meta?: Record<string, any>) {
      if (shouldLog('info')) {
        console.log(formatMessage('info', module, message, meta))
      }
    },

    warn(message: string, meta?: Record<string, any>) {
      if (shouldLog('warn')) {
        console.warn(formatMessage('warn', module, message, meta))
      }
    },

    error(message: string, error?: Error | unknown, meta?: Record<string, any>) {
      if (shouldLog('error')) {
        const errorMeta: Record<string, any> = { ...meta }
        if (error instanceof Error) {
          errorMeta.error = error.message
          errorMeta.stack = error.stack
        } else if (error) {
          errorMeta.error = String(error)
        }
        console.error(formatMessage('error', module, message, errorMeta))
      }
    },
  }
}
