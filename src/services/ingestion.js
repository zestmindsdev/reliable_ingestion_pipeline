/**
 * Ingestion Service - Production Grade
 * Handles batch processing, validation, metrics, and idempotent ingestion
 */

const crypto = require('crypto');
const db = require('./database');
const alertService = require('./alerts');
const { ValidationError, DatabaseError, BusinessLogicError } = require('./errors');

class IngestionService {
  constructor() {
    this.metrics = {
      totalIngestions: 0,
      totalRecordsProcessed: 0,
      totalErrors: 0,
      averageProcessingTime: 0
    };
  }

  /**
   * Validate record against schema
   */
  validateRecord(record, index) {
    const requiredFields = [
      'source_key',
      'published_at',
      'title',
      'entity_name_raw',
      'entity_name_norm',
      'region',
      'record_id',
      'status'
    ];

    const errors = [];

    for (const field of requiredFields) {
      // MINIMAL CHANGE: avoid treating empty string as missing if you want;
      // but keeping your original strict check is fine. Keeping as-is:
      if (!record[field]) {
        errors.push(`Missing required field: ${field}`);
      }
    }

    // Validate data types and formats
    if (record.source_key && record.source_key.length > 255) {
      errors.push('source_key exceeds maximum length of 255 characters');
    }

    if (record.published_at) {
      const date = new Date(record.published_at);
      if (isNaN(date.getTime())) {
        errors.push('published_at is not a valid ISO 8601 date');
      }
    }

    if (record.region && !/^[A-Z]{2}$/.test(record.region)) {
      errors.push('region must be a 2-letter uppercase code');
    }

    if (errors.length > 0) {
      throw new ValidationError(
        `Record at index ${index} validation failed: ${errors.join(', ')}`
      );
    }

    return true;
  }

  /**
   * Generate deterministic content hash
   */
  generateContentHash(record) {
    const contentFields = {
      source_key: record.source_key,

      // MINIMAL CHANGE (IMPORTANT): include published_at in hash
      // so changes to timestamp are detected consistently.
      published_at: record.published_at,

      title: record.title,
      entity_name_raw: record.entity_name_raw,
      entity_name_norm: record.entity_name_norm,
      region: record.region,
      record_id: record.record_id,
      status: record.status,
      document_url: record.document_url || '',
    };

    // Sort keys for determinism and create JSON string
    const sortedKeys = Object.keys(contentFields).sort();
    const contentString = sortedKeys
      .map(key => `${key}:${contentFields[key]}`)
      .join('|');

    return crypto
      .createHash('sha256')
      .update(contentString, 'utf8')
      .digest('hex');
  }

  /**
   * Process a single record with upsert logic
   */
  async processRecord(client, record, sourceType, metrics) {
    const contentHash = this.generateContentHash(record);

    // MINIMAL CHANGE (BUG FIX): also select last_source_type (needed for precedence)
    const existingRecord = await client.query(
      'SELECT id, content_hash, last_source_type FROM records WHERE source_key = $1',
      [record.source_key]
    );

    if (existingRecord.rows.length === 0) {
      // Insert new record
      const result = await client.query(
        `INSERT INTO records 
         (source_key, published_at, title, entity_name_raw, entity_name_norm, 
          region, record_id, status, document_url, raw_json, content_hash,
          last_source_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING id`,
        [
          record.source_key,
          record.published_at,
          record.title,
          record.entity_name_raw,
          record.entity_name_norm,
          record.region,
          record.record_id,
          record.status,
          record.document_url || null,
          JSON.stringify(record.raw_json || {}),
          contentHash,
          sourceType
        ]
      );

      metrics.inserted++;

      // Trigger alerts with correct parameter order: recordId, actionType, client
      await alertService.checkAndTriggerAlerts(result.rows[0].id, 'insert', client);

      return { action: 'inserted', recordId: result.rows[0].id };
    } else {
      // MINIMAL CHANGE (BUG FIX): your code referenced `existing` which doesn't exist
      const existingRow = existingRecord.rows[0];

      // MINIMAL CHANGE (REQUIRED): precedence rule
      // bulk is master, recent must not override bulk
      if (sourceType === 'recent' && existingRow.last_source_type === 'bulk') {
        metrics.skipped++;
        return { action: 'skipped', recordId: existingRow.id };
      }

      // Check if content changed
      if (existingRow.content_hash !== contentHash) {
        await client.query(
          `UPDATE records SET
            published_at = $2,
            title = $3,
            entity_name_raw = $4,
            entity_name_norm = $5,
            region = $6,
            record_id = $7,
            status = $8,
            document_url = $9,
            raw_json = $10,
            content_hash = $11,
            last_source_type = $12,
            updated_at = NOW()
          WHERE source_key = $1`,
          [
            record.source_key,
            record.published_at,
            record.title,
            record.entity_name_raw,
            record.entity_name_norm,
            record.region,
            record.record_id,
            record.status,
            record.document_url || null,
            JSON.stringify(record.raw_json || {}),
            contentHash,
            sourceType
          ]
        );

        metrics.updated++;

        // Trigger alerts with correct parameter order: recordId, actionType, client
        await alertService.checkAndTriggerAlerts(
          existingRow.id,
          'update',
          client
        );

        return { action: 'updated', recordId: existingRow.id };
      } else {
        metrics.skipped++;
        return { action: 'skipped', recordId: existingRow.id };
      }
    }
  }

  /**
   * Process records in batches for better performance
   */
  async processBatch(client, records, sourceType, batchSize = 100) {
    const metrics = {
      inserted: 0,
      updated: 0,
      skipped: 0,
      failed: 0
    };

    const results = [];

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);

      for (const record of batch) {
        try {
          // MINIMAL CHANGE (BUG FIX): you must pass sourceType to processRecord
          const result = await this.processRecord(client, record, sourceType, metrics);
          results.push({ success: true, ...result });
        } catch (error) {
          metrics.failed++;
          results.push({
            success: false,
            error: error.message,
            sourceKey: record.source_key
          });

          // Log but continue processing other records
          console.error(`Failed to process record ${record.source_key}:`, error.message);
        }
      }

      // Log progress for large batches
      if (records.length > batchSize) {
        const progress = Math.min(i + batchSize, records.length);
        console.log(`Processed ${progress}/${records.length} records`);
      }
    }

    return { metrics, results };
  }

  /**
   * Main ingestion method with full transaction support
   */
  async ingestRecords(records, sourceType, options = {}) {
    const startTime = Date.now();
    const batchSize = options.batchSize || 100;
    const validateRecords = options.validate !== false; // Default to true

    // Input validation
    if (!Array.isArray(records)) {
      throw new ValidationError('Records must be an array');
    }

    if (records.length === 0) {
      throw new ValidationError('Records array cannot be empty');
    }

    if (!['bulk', 'recent'].includes(sourceType)) {
      throw new ValidationError('sourceType must be either "bulk" or "recent"');
    }

    if (sourceType === 'recent') {
      records = this.filterRecentWindow(records, 72);
    }

    // Validate all records before processing
    if (validateRecords) {
      console.log('Validating records...');
      records.forEach((record, index) => {
        this.validateRecord(record, index);
      });
      console.log(`Validation passed for ${records.length} records`);
    }

    let runId;
    let metrics = {
      inserted: 0,
      updated: 0,
      skipped: 0,
      failed: 0
    };

    try {
      const result = await db.transaction(async (client) => {
        // Create ingestion run log
        const logResult = await client.query(
          `INSERT INTO ingestion_runs (source_type, started_at, records_fetched) 
           VALUES ($1, $2, $3) 
           RETURNING id`,
          [sourceType, new Date(), records.length]
        );
        runId = logResult.rows[0].id;

        // Process records in batches
        // MINIMAL CHANGE: pass sourceType into processBatch
        const batchResult = await this.processBatch(client, records, sourceType, batchSize);
        metrics = batchResult.metrics;

        // Update ingestion run with results
        await client.query(
          `UPDATE ingestion_runs 
           SET finished_at = $2, 
               records_inserted = $3, 
               records_updated = $4, 
               error = $5
           WHERE id = $1`,
          [
            runId,
            new Date(),
            metrics.inserted,
            metrics.updated,
            metrics.failed > 0 ? `${metrics.failed} records failed to process` : null
          ]
        );

        return {
          runId,
          sourceType,
          recordsFetched: records.length,
          recordsInserted: metrics.inserted,
          recordsUpdated: metrics.updated,
          recordsSkipped: metrics.skipped,
          recordsFailed: metrics.failed,
          processingTime: Date.now() - startTime
        };
      });

      // Update service metrics
      this.updateMetrics(result);

      console.log(`Ingestion completed in ${result.processingTime}ms:`, {
        inserted: result.recordsInserted,
        updated: result.recordsUpdated,
        skipped: result.recordsSkipped,
        failed: result.recordsFailed
      });

      return result;

    } catch (error) {
      // Log error to database if possible
      if (runId) {
        try {
          await db.query(
            `UPDATE ingestion_runs 
             SET finished_at = $2, error = $3 
             WHERE id = $1`,
            [runId, new Date(), error.message]
          );
        } catch (logError) {
          console.error('Failed to log ingestion error:', logError);
        }
      }

      this.metrics.totalErrors++;

      if (error instanceof ValidationError || error instanceof DatabaseError) {
        throw error;
      }

      throw new DatabaseError(`Ingestion failed: ${error.message}`, error);
    }
  }

  /**
   * Update internal metrics
   */
  updateMetrics(result) {
    this.metrics.totalIngestions++;
    this.metrics.totalRecordsProcessed += result.recordsFetched;

    // Calculate rolling average processing time
    const currentAvg = this.metrics.averageProcessingTime;
    const count = this.metrics.totalIngestions;
    this.metrics.averageProcessingTime =
      (currentAvg * (count - 1) + result.processingTime) / count;
  }

  /**
   * Get ingestion history with pagination
   */
  async getIngestionHistory(limit = 10, offset = 0, client = null) {
    const executor = client ?? db;

    if (limit > 100) {
      throw new ValidationError('Limit cannot exceed 100');
    }

    const result = await executor.query(
      `SELECT * FROM ingestion_runs 
       ORDER BY started_at DESC 
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    const countResult = await executor.query(
      'SELECT COUNT(*) as total FROM ingestion_runs'
    );

    return {
      runs: result.rows,
      pagination: {
        limit,
        offset,
        total: parseInt(countResult.rows[0].total)
      }
    };
  }

  /**
   * Get service metrics
   */
  getMetrics() {
    return {
      ...this.metrics,
      averageProcessingTime: Math.round(this.metrics.averageProcessingTime)
    };
  }

  /**
   * Filter records by recent window
   */
  filterRecentWindow(records, hours = 72) {
    const cutoff = Date.now() - hours * 60 * 60 * 1000;

    return records.filter(r => {
      const ts = new Date(r.published_at).getTime();
      return !isNaN(ts) && ts >= cutoff;
    });
  }
}

// Export singleton instance
const ingestionService = new IngestionService();

module.exports = ingestionService;
