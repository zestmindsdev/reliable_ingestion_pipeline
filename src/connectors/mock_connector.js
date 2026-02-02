/**
 * MOCK CONNECTOR - STRICT ISOLATION
 * 
 * RULES (violation = automatic fail):
 * - NO database access
 * - NO hashing
 * - NO deduplication
 * - NO alerts
 * - NO business logic
 * 
 * ONLY allowed to:
 * - Read files
 * - Parse data
 * - Map fields to canonical format
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

/**
 * Normalize entity name to lowercase for consistent matching
 */
function normalizeEntityName(raw) {
  return raw.toLowerCase().trim();
}

/**
 * Fetch recent records from the last N hours
 * @param {number} hours - Number of hours to look back (default 72)
 * @returns {Promise<Array>} Array of records in canonical format
 */
async function fetchRecent(hours = 72) {
  const filePath = path.join(__dirname, '../../mock_data/recent.json');
  const fileContent = fs.readFileSync(filePath, 'utf8');
  const data = JSON.parse(fileContent);

  // NO filtering, NO logic
  return data.map(record => ({
    source_key: record.source_key,
    published_at: record.published_at,
    title: record.title,
    entity_name_raw: record.entity_name_raw,
    entity_name_norm: normalizeEntityName(record.entity_name_raw),
    region: record.region,
    record_id: record.record_id,
    status: record.status,
    document_url: record.document_url,
    raw_json: record,
    content_hash: null
  }));
}


/**
 * Fetch all bulk records
 * @returns {Promise<Array>} Array of records in canonical format
 */
async function fetchBulk() {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, '../../mock_data/bulk.csv');
    const records = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // Map CSV row to canonical format
        records.push({
          source_key: row.source_key,
          published_at: row.published_at,
          title: row.title,
          entity_name_raw: row.entity_name_raw,
          entity_name_norm: normalizeEntityName(row.entity_name_raw),
          region: row.region,
          record_id: row.record_id,
          status: row.status,
          document_url: row.document_url,
          raw_json: row,
          content_hash: null
        });
      })
      .on('end', () => {
        resolve(records);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

module.exports = {
  fetchRecent,
  fetchBulk
};
