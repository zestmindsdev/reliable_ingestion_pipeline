/**
 * Export Service
 * Handles CSV export with plan-based access control
 */
const db = require('./database');
const { Parser } = require('json2csv');
const { NotFoundError, BusinessLogicError } = require('./errors');

/**
 * Check if user can export based on their plan
 * @param {number} userId - User ID
 * @param {Object} client - Database client (optional)
 * @returns {Promise<boolean>} True if user can export
 */
async function canExport(userId, client = null) {
  const executor = client ?? db;
  
  const result = await executor.query(
    'SELECT plan FROM users WHERE id = $1',
    [userId]
  );
  
  if (result.rows.length === 0) {
    throw new NotFoundError('User', userId);
  }
  
  const plan = result.rows[0].plan;
  
  // Starter plan is blocked from CSV export
  return plan !== 'starter';
}

/**
 * Export records to CSV
 * @param {number} userId - User ID
 * @param {Object} filters - Optional filters (entity_name_norm, region, date_from, date_to)
 * @param {Object} client - Database client (optional)
 * @returns {Promise<string>} CSV string
 */
async function exportToCSV(userId, filters = {}, client = null) {
  const executor = client ?? db;
  
  // Check if user can export
  const allowed = await canExport(userId, executor);
  
  if (!allowed) {
    throw new BusinessLogicError(
      'CSV export is not available on the Starter plan. Please upgrade to Pro or Team.'
    );
  }
  
  // Build query with filters
  let query = 'SELECT * FROM records WHERE 1=1';
  const params = [];
  let paramIndex = 1;
  
  if (filters.entity_name_norm) {
    query += ` AND entity_name_norm = $${paramIndex}`;
    params.push(filters.entity_name_norm);
    paramIndex++;
  }
  
  if (filters.region) {
    query += ` AND region = $${paramIndex}`;
    params.push(filters.region);
    paramIndex++;
  }
  
  if (filters.date_from) {
    query += ` AND published_at >= $${paramIndex}`;
    params.push(filters.date_from);
    paramIndex++;
  }
  
  if (filters.date_to) {
    query += ` AND published_at <= $${paramIndex}`;
    params.push(filters.date_to);
    paramIndex++;
  }
  
  query += ' ORDER BY published_at DESC';
  
  // Execute query
  const result = await executor.query(query, params);
  
  if (result.rows.length === 0) {
    return '';
  }
  
  // Define CSV fields
  const fields = [
    'id',
    'source_key',
    'published_at',
    'title',
    'entity_name_raw',
    'entity_name_norm',
    'region',
    'record_id',
    'status',
    'document_url',
    'content_hash',
    'created_at',
    'updated_at'
  ];
  
  // Convert to CSV
  const json2csvParser = new Parser({ fields });
  const csv = json2csvParser.parse(result.rows);
  
  return csv;
}

/**
 * Get export statistics
 * @param {number} userId - User ID
 * @param {Object} client - Database client (optional)
 * @returns {Promise<Object>} Export statistics
 */
async function getExportStats(userId, client = null) {
  const executor = client ?? db;
  
  const allowed = await canExport(userId, executor);
  
  const userResult = await executor.query(
    'SELECT plan FROM users WHERE id = $1',
    [userId]
  );
  
  const plan = userResult.rows[0].plan;
  
  const countResult = await executor.query(
    'SELECT COUNT(*) as total FROM records'
  );
  
  return {
    plan,
    canExport: allowed,
    totalRecords: parseInt(countResult.rows[0].total),
    message: allowed ? 'CSV export available' : 'Upgrade to Pro or Team to export CSV'
  };
}

module.exports = {
  canExport,
  exportToCSV,
  getExportStats
};