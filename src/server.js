/**
 * API Server
 * Includes security middleware, rate limiting, monitoring, and graceful shutdown
 */

const express = require('express');
const rateLimit = require('express-rate-limit');
require('dotenv').config();

const connector = require('./connectors/mock_connector');
const db = require('./services/database');
const ingestionService = require('./services/ingestion');
const alertsService = require('./services/alerts');
const exportService = require('./services/export');
const { errorHandler, asyncHandler } = require('./services/errors');

const app = express();
const PORT = process.env.PORT || 3000;

// Request ID middleware for tracing
app.use((req, res, next) => {
  req.id = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  res.setHeader('X-Request-ID', req.id);
  next();
});

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    const logData = {
      requestId: req.id,
      method: req.method,
      path: req.path,
      statusCode: res.statusCode,
      duration: `${duration}ms`,
      userAgent: req.get('user-agent')
    };

    if (res.statusCode >= 400) {
      console.error('Request failed:', logData);
    } else if (duration > 1000) {
      console.warn('Slow request:', logData);
    } else {
      console.log('Request completed:', logData);
    }
  });

  next();
});

// Body parser with size limits
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  next();
});

// Rate limiting
const generalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: { error: 'Too many requests, please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

const ingestionLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 10, // Limit ingestion to 10 requests per minute
  message: { error: 'Ingestion rate limit exceeded. Please wait before trying again.' }
});

app.use('/api/', generalLimiter);

// Health check (no rate limiting)
app.get('/health', asyncHandler(async (req, res) => {
  const dbHealth = await db.healthCheck();
  const dbStats = db.getStats();
  const ingestionMetrics = ingestionService.getMetrics();

  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    database: {
      ...dbHealth,
      stats: dbStats
    },
    ingestion: ingestionMetrics,
    memory: {
      used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
      unit: 'MB'
    }
  });
}));

// Metrics endpoint
app.get('/api/metrics', asyncHandler(async (req, res) => {
  res.json({
    database: db.getStats(),
    ingestion: ingestionService.getMetrics(),
    timestamp: new Date().toISOString()
  });
}));

// INGESTION ENDPOINTS

app.post('/api/ingest/bulk', ingestionLimiter, asyncHandler(async (req, res) => {
  const options = {
    batchSize: parseInt(req.body.batchSize) || 100,
    validate: req.body.validate !== false
  };

  const records = await connector.fetchBulk();
  const result = await ingestionService.ingestRecords(records, 'bulk', options);
  
  res.status(200).json({
    success: true,
    data: result
  });
}));

app.post('/api/ingest/recent', ingestionLimiter, asyncHandler(async (req, res) => {
  const hours = parseInt(req.body.hours) || 72;
  const options = {
    batchSize: parseInt(req.body.batchSize) || 100,
    validate: req.body.validate !== false
  };

  if (hours < 1 || hours > 168) {
    return res.status(400).json({
      error: 'hours must be between 1 and 168 (1 week)'
    });
  }

  const records = await connector.fetchRecent(hours);
  const result = await ingestionService.ingestRecords(records, 'recent', options);
  
  res.status(200).json({
    success: true,
    data: result
  });
}));

app.get('/api/ingestion/history', asyncHandler(async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);
  const offset = parseInt(req.query.offset) || 0;

  const result = await db.transaction(async (client) => {
    return await ingestionService.getIngestionHistory(limit, offset, client);
  });
  
  res.json({
    success: true,
    data: result
  });
}));

// ALERT ENDPOINTS

app.post('/api/alerts', asyncHandler(async (req, res) => {
  const { userId, entityNameNorm, region } = req.body;

  if (!userId) {
    return res.status(400).json({
      error: 'userId is required'
    });
  }

  const result = await db.transaction(async (client) => {
    return await alertsService.createAlertRule(
      parseInt(userId),
      entityNameNorm || null,
      region || null,
      client
    );
  });

  res.status(201).json({
    success: true,
    data: result
  });
}));

app.get('/api/alerts/user/:userId', asyncHandler(async (req, res) => {
  const userId = parseInt(req.params.userId);
  
  const alerts = await db.transaction(async (client) => {
    return await alertsService.getUserAlertRules(userId, client);
  });

  res.json({
    success: true,
    data: alerts
  });
}));

app.get('/api/alerts/user/:userId/stats', asyncHandler(async (req, res) => {
  const userId = parseInt(req.params.userId);
  
  const stats = await db.transaction(async (client) => {
    return await alertsService.getUserAlertStats(userId, client);
  });

  res.json({
    success: true,
    data: stats
  });
}));

app.delete('/api/alerts/:alertId', asyncHandler(async (req, res) => {
  const alertId = parseInt(req.params.alertId);
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({
      error: 'userId is required in request body'
    });
  }

  const deleted = await db.transaction(async (client) => {
    return await alertsService.deleteAlertRule(alertId, parseInt(userId), client);
  });

  res.json({
    success: true,
    data: deleted
  });
}));

app.get('/api/alerts/logs', asyncHandler(async (req, res) => {
  const options = {
    alertRuleId: req.query.alertRuleId ? parseInt(req.query.alertRuleId) : null,
    userId: req.query.userId ? parseInt(req.query.userId) : null,
    actionType: req.query.actionType || null,
    limit: Math.min(parseInt(req.query.limit) || 50, 100),
    offset: parseInt(req.query.offset) || 0
  };

  const result = await db.transaction(async (client) => {
    return await alertsService.getAlertLogs(options, client);
  });

  res.json({
    success: true,
    data: result
  });
}));

// EXPORT ENDPOINTS

app.get('/api/export/csv', asyncHandler(async (req, res) => {
  const userId = parseInt(req.query.userId);

  if (!userId) {
    return res.status(400).json({
      error: 'userId query parameter is required'
    });
  }

  const filters = {
    entity_name_norm: req.query.entityNameNorm,
    region: req.query.region,
    date_from: req.query.dateFrom,
    date_to: req.query.dateTo
  };

  const csv = await db.transaction(async (client) => {
    return await exportService.exportToCSV(userId, filters, client);
  });

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=records.csv');
  res.send(csv);
}));

app.get('/api/export/stats', asyncHandler(async (req, res) => {
  const userId = parseInt(req.query.userId);

  if (!userId) {
    return res.status(400).json({
      error: 'userId query parameter is required'
    });
  }

  const stats = await db.transaction(async (client) => {
    return await exportService.getExportStats(userId, client);
  });

  res.json({
    success: true,
    data: stats
  });
}));

// UTILITY ENDPOINTS

app.get('/api/records', asyncHandler(async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  const offset = parseInt(req.query.offset) || 0;

  const result = await db.transaction(async (client) => {
    const records = await client.query(
      'SELECT * FROM records ORDER BY published_at DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );

    const countResult = await client.query('SELECT COUNT(*) as total FROM records');

    return {
      records: records.rows,
      pagination: {
        limit,
        offset,
        total: parseInt(countResult.rows[0].total)
      }
    };
  });

  res.json({
    success: true,
    data: result
  });
}));

app.get('/api/users', asyncHandler(async (req, res) => {
  const result = await db.transaction(async (client) => {
    return await client.query('SELECT id, email, plan, created_at FROM users ORDER BY id');
  });

  res.json({
    success: true,
    data: result.rows
  });
}));

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    path: req.path
  });
});

// Error handling middleware (must be last)
app.use(errorHandler);

// Graceful shutdown handler
let server;

async function gracefulShutdown(signal) {
  console.log(`\n${signal} received. Starting graceful shutdown...`);

  if (server) {
    server.close(() => {
      console.log('HTTP server closed');
    });
  }

  try {
    await db.shutdown();
    console.log('Database connections closed');
    
    // Force exit after brief delay
    setTimeout(() => {
      process.exit(0);
    }, 1000);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Uncaught exception handler
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Start server
async function startServer() {
  try {
    // Initialize database
    await db.initialize();
    
    // Refresh alert cache
    await alertsService.refreshCache();

    server = app.listen(PORT, () => {
      console.log(`\n╔═══════════════════════════════════════════════════════╗`);
      console.log(`║     INGESTION PIPELINE - API SERVER       ║`);
      console.log(`╚═══════════════════════════════════════════════════════╝\n`);
      console.log(`Server running on http://localhost:${PORT}\n`);
      console.log('API Endpoints:');
      console.log('─'.repeat(60));
      console.log('  Health & Metrics:');
      console.log('    GET    /health                           - Health check');
      console.log('    GET    /api/metrics                      - Service metrics\n');
      console.log('  Ingestion:');
      console.log('    POST   /api/ingest/bulk                  - Run bulk ingestion');
      console.log('    POST   /api/ingest/recent                - Run recent ingestion');
      console.log('    GET    /api/ingestion/history            - Get ingestion logs\n');
      console.log('  Alerts:');
      console.log('    POST   /api/alerts                       - Create alert rule');
      console.log('    GET    /api/alerts/user/:userId          - Get user alerts');
      console.log('    GET    /api/alerts/user/:userId/stats    - Get user alert stats');
      console.log('    DELETE /api/alerts/:alertId              - Delete alert');
      console.log('    GET    /api/alerts/logs                  - Get alert logs\n');
      console.log('  Export:');
      console.log('    GET    /api/export/csv?userId=X          - Export to CSV');
      console.log('    GET    /api/export/stats?userId=X        - Get export stats\n');
      console.log('  Utility:');
      console.log('    GET    /api/records                      - Get all records');
      console.log('    GET    /api/users                        - Get all users\n');
      console.log('─'.repeat(60));
      console.log('\nPress Ctrl+C to stop the server\n');
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Only start if this file is run directly
if (require.main === module) {
  startServer();
}

module.exports = app;