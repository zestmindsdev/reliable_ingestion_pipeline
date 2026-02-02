/**
 * Database Service - Production Grade
 * Handles connection pooling, retries, health checks, and graceful shutdown
 */

const { Pool } = require('pg');
const { DatabaseError } = require('./errors');
require('dotenv').config();

class DatabaseService {
  constructor() {
    this.pool = null;
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 1000; // Start with 1 second
  }

  /**
   * Initialize database connection pool
   */
  async initialize() {
    if (this.pool) {
      console.warn('Database pool already initialized');
      return;
    }

    const config = {
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '5432'),
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      max: parseInt(process.env.DB_POOL_MAX || '20'),
      min: parseInt(process.env.DB_POOL_MIN || '2'),
      idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000'),
      connectionTimeoutMillis: parseInt(process.env.DB_CONNECT_TIMEOUT || '10000'),
    };

    this.pool = new Pool(config);

    this.pool.on('connect', () => {
      this.isConnected = true;
      this.reconnectAttempts = 0;
      console.log('Database client connected');
    });

    this.pool.on('error', (err) => {
      console.error('Unexpected database pool error:', err);
      this.isConnected = false;
      this.handleReconnect();
    });

    this.pool.on('remove', () => {
      console.log('Database client removed from pool');
    });

    // Verify connection on startup
    await this.healthCheck();
  }

  /**
   * Handle automatic reconnection with exponential backoff
   */
  async handleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('Max reconnection attempts reached. Manual intervention required.');
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
    
    console.log(`Attempting database reconnection ${this.reconnectAttempts}/${this.maxReconnectAttempts} in ${delay}ms`);

    setTimeout(async () => {
      try {
        await this.healthCheck();
        console.log('Database reconnection successful');
      } catch (error) {
        console.error('Reconnection failed:', error.message);
        this.handleReconnect();
      }
    }, delay);
  }

  /**
   * Execute query with automatic retry logic
   */
  async query(text, params, retries = 3) {
    if (!this.pool) {
      await this.initialize();
    }

    const startTime = Date.now();
    let lastError;

    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const result = await this.pool.query(text, params);
        const duration = Date.now() - startTime;

        // Log slow queries
        if (duration > 1000) {
          console.warn(`Slow query detected (${duration}ms):`, {
            query: text.substring(0, 100),
            duration,
            rows: result.rowCount
          });
        }

        return result;
      } catch (error) {
        lastError = error;

        const dbError = new DatabaseError(
          `Query failed: ${error.message}`,
          error
        );

        // Don't retry if it's not a retryable error
        if (!dbError.retryable || attempt === retries) {
          throw dbError;
        }

        // Exponential backoff for retries
        const backoff = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.warn(`Query failed (attempt ${attempt}/${retries}), retrying in ${backoff}ms`);
        await new Promise(resolve => setTimeout(resolve, backoff));
      }
    }

    throw new DatabaseError(`Query failed after ${retries} attempts`, lastError);
  }

  /**
   * Get a client for transaction management
   */
  async getClient() {
    if (!this.pool) {
      await this.initialize();
    }

    try {
      const client = await this.pool.connect();
      
      // Add query method to client for consistency
      const originalQuery = client.query.bind(client);
      client.query = async (text, params) => {
        const startTime = Date.now();
        try {
          const result = await originalQuery(text, params);
          const duration = Date.now() - startTime;
          
          if (duration > 1000) {
            console.warn(`Slow transaction query (${duration}ms):`, text.substring(0, 100));
          }
          
          return result;
        } catch (error) {
          throw new DatabaseError(`Transaction query failed: ${error.message}`, error);
        }
      };

      return client;
    } catch (error) {
      throw new DatabaseError(`Failed to get database client: ${error.message}`, error);
    }
  }

  /**
   * Execute function within a transaction with automatic rollback on error
   */
  async transaction(callback) {
    const client = await this.getClient();
    
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Health check to verify database connectivity with timeout
   */
  async healthCheck() {
    if (!this.pool) {
      throw new DatabaseError('Database pool not initialized');
    }

    let client = null;
    
    try {
      // Add timeout to prevent hanging
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Health check timeout after 5 seconds')), 5000)
      );
      
      // Get a client directly from pool for health check
      const healthCheckPromise = (async () => {
        client = await this.pool.connect();
        const result = await client.query('SELECT 1 as health_check');
        return result;
      })();
      
      const result = await Promise.race([healthCheckPromise, timeoutPromise]);
      
      if (result.rows[0].health_check === 1) {
        this.isConnected = true;
        return { status: 'healthy', timestamp: new Date().toISOString() };
      }
      throw new Error('Health check query returned unexpected result');
    } catch (error) {
      this.isConnected = false;
      throw new DatabaseError('Database health check failed', error);
    } finally {
      // Always release the client
      if (client) {
        try {
          client.release();
        } catch (releaseError) {
          console.error('Error releasing health check client:', releaseError.message);
        }
      }
    }
  }

  /**
   * Get connection pool statistics
   */
  getStats() {
    if (!this.pool) {
      return { status: 'not_initialized' };
    }

    return {
      totalCount: this.pool.totalCount,
      idleCount: this.pool.idleCount,
      waitingCount: this.pool.waitingCount,
      isConnected: this.isConnected,
      reconnectAttempts: this.reconnectAttempts
    };
  }

  /**
   * Graceful shutdown with timeout
   */
  async shutdown() {
    if (!this.pool) {
      console.log('Database pool already closed');
      return;
    }

    console.log('Shutting down database connections...');
    
    try {
      // Add timeout to prevent hanging on shutdown
      const shutdownPromise = this.pool.end();
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Shutdown timeout after 10 seconds')), 10000)
      );
      
      await Promise.race([shutdownPromise, timeoutPromise]);
      
      this.pool = null;
      this.isConnected = false;
      console.log('Database pool closed successfully');
    } catch (error) {
      console.error('Error closing database pool:', error.message);
      
      // Force close if graceful shutdown fails
      if (this.pool) {
        console.log('Forcing pool termination...');
        this.pool = null;
        this.isConnected = false;
      }
      
      // Don't throw error on shutdown - just log it
      console.log('Database shutdown completed with errors');
    }
  }
}

// Export singleton instance
const databaseService = new DatabaseService();

module.exports = databaseService;