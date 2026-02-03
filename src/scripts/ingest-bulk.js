/**
 * Bulk Ingestion Script
 * Production-ready bulk data ingestion
 */

const connector = require('../../connectors/mock_connector');
const ingestionService = require('../services/ingestion');
const db = require('../services/database');

async function runBulkIngestion() {
  console.log('=== Starting Bulk Ingestion ===\n');
  
  try {
    
    // Fetch bulk data from connector
    console.log('Fetching bulk records from connector...');
    const records = await connector.fetchBulk();
    console.log(`Fetched ${records.length} records\n`);
    
    // Display sample records
    console.log('Sample records:');
    records.slice(0, 2).forEach(r => {
      console.log(`  - ${r.source_key}: ${r.title} (${r.entity_name_raw})`);
    });
    console.log();
    
    // Ingest records with validation and batch processing
    console.log('Ingesting records...');
    const result = await ingestionService.ingestRecords(records, 'bulk', {
      validate: true,
      batchSize: 100
    });
    
    console.log('\n=== Ingestion Complete ===');
    console.log(`Run ID: ${result.runId}`);
    console.log(`Records Fetched: ${result.recordsFetched}`);
    console.log(`Records Inserted: ${result.recordsInserted}`);
    console.log(`Records Updated: ${result.recordsUpdated}`);
    console.log(`Records Skipped: ${result.recordsSkipped}`);
    console.log(`Processing Time: ${result.processingTime}ms`);
    
    // Show metrics
    const metrics = ingestionService.getMetrics();
    console.log('\nService Metrics:');
    console.log(`  Total Ingestions: ${metrics.totalIngestions}`);
    console.log(`  Total Records Processed: ${metrics.totalRecordsProcessed}`);
    console.log(`  Average Processing Time: ${metrics.averageProcessingTime}ms`);
    
  } catch (error) {
    console.error('\nBulk ingestion failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  } finally {
    await db.shutdown();
  }
}

// Run if called directly
if (require.main === module) {
  runBulkIngestion();
}

module.exports = runBulkIngestion;
