/**
 * Error Handling System
 * Production-grade error classes with proper categorization
 */

class BaseError extends Error {
  constructor(message, statusCode = 500, isOperational = true) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.isOperational = isOperational;
    this.timestamp = new Date().toISOString();
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      name: this.name,
      message: this.message,
      statusCode: this.statusCode,
      timestamp: this.timestamp,
      ...(process.env.NODE_ENV === 'development' && { stack: this.stack })
    };
  }
}

class ValidationError extends BaseError {
  constructor(message, field = null) {
    super(message, 400);
    this.field = field;
  }
}

class DatabaseError extends BaseError {
  constructor(message, originalError = null) {
    super(message, 500);
    this.originalError = originalError;
    this.retryable = this.isRetryableError(originalError);
  }

  isRetryableError(error) {
    if (!error) return false;
    
    const retryableCodes = ['ECONNREFUSED', 'ETIMEDOUT', '57P01', '40001'];
    return retryableCodes.includes(error.code);
  }
}

class BusinessLogicError extends BaseError {
  constructor(message) {
    super(message, 422);
  }
}

class AuthorizationError extends BaseError {
  constructor(message = 'Unauthorized access') {
    super(message, 403);
  }
}

class NotFoundError extends BaseError {
  constructor(resource, identifier = null) {
    const message = identifier 
      ? `${resource} with identifier '${identifier}' not found`
      : `${resource} not found`;
    super(message, 404);
    this.resource = resource;
    this.identifier = identifier;
  }
}

class RateLimitError extends BaseError {
  constructor(limit, window) {
    super(`Rate limit exceeded: ${limit} requests per ${window}`, 429);
    this.limit = limit;
    this.window = window;
  }
}

/**
 * Error Handler Middleware
 */
function errorHandler(err, req, res, next) {
  // Log error
  const logLevel = err.statusCode >= 500 ? 'error' : 'warn';
  console[logLevel]({
    error: err.name,
    message: err.message,
    statusCode: err.statusCode,
    path: req.path,
    method: req.method,
    timestamp: err.timestamp,
    ...(err.originalError && { originalError: err.originalError.message }),
    stack: err.stack
  });

  // Don't leak error details in production
  const isDevelopment = process.env.NODE_ENV === 'development';
  
  res.status(err.statusCode || 500).json({
    error: {
      message: err.isOperational ? err.message : 'Internal server error',
      code: err.name,
      ...(isDevelopment && { details: err.toJSON() })
    }
  });
}

/**
 * Async handler wrapper to catch errors
 */
function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

module.exports = {
  BaseError,
  ValidationError,
  DatabaseError,
  BusinessLogicError,
  AuthorizationError,
  NotFoundError,
  RateLimitError,
  errorHandler,
  asyncHandler
};