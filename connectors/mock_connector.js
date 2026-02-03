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
 * - Map fields to canonical format (RAW ONLY)
 */

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

/**
 * Fetch recent records
 * @param {number} hours - informational only
 */
async function fetchRecent(hours = 72) {
  const filePath = path.join(__dirname, '../mock_data/recent.json');
  const fileContent = fs.readFileSync(filePath, 'utf8');
  const data = JSON.parse(fileContent);

  return data.map(record => ({
    source_key: record.source_key,
    published_at: record.published_at,
    title: record.title,
    entity_name_raw: record.entity_name_raw,
    region: record.region,
    record_id: record.record_id,
    status: record.status,
    document_url: record.document_url,
    raw_json: record
  }));
}

/**
 * Fetch bulk records
 */
async function fetchBulk() {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, '../mock_data/bulk.csv');
    const records = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        records.push({
          source_key: row.source_key,
          published_at: row.published_at,
          title: row.title,
          entity_name_raw: row.entity_name_raw,
          region: row.region,
          record_id: row.record_id,
          status: row.status,
          document_url: row.document_url,
          raw_json: row
        });
      })
      .on('end', () => resolve(records))
      .on('error', reject);
  });
}

module.exports = {
  fetchRecent,
  fetchBulk
};
