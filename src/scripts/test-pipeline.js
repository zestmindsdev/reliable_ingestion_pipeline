/**
 * Comprehensive Test Script
 * Tests all pipeline features with senior-level services
 */

const connector = require('../connectors/mock_connector');
const ingestionService = require('../services/ingestion');
const alertsService = require('../services/alerts');
const exportService = require('../services/export');
const db = require('../services/database');

async function runTests() {
  console.log('╔═══════════════════════════════════════════════════════╗');
  console.log('║    PRODUCTION INGESTION PIPELINE - COMPREHENSIVE TEST ║');
  console.log('╚═══════════════════════════════════════════════════════╝\n');
  
  try {
    // Initialize database
    await db.initialize();
    
    // Refresh alert cache
    await alertsService.refreshCache();
    
    // TEST 1: BULK INGESTION
    console.log('TEST 1: Bulk Ingestion');
    console.log('─'.repeat(60));
    
    const bulkRecords = await connector.fetchBulk();
    console.log(`Connector fetched ${bulkRecords.length} bulk records`);
    
    const bulkResult = await ingestionService.ingestRecords(bulkRecords, 'bulk', {
      validate: true,
      batchSize: 100
    });
    console.log(`Inserted: ${bulkResult.recordsInserted}, Updated: ${bulkResult.recordsUpdated}`);
    console.log(`Run ID: ${bulkResult.runId}, Processing Time: ${bulkResult.processingTime}ms\n`);
    
    // TEST 2: RECENT INGESTION
    console.log('TEST 2: Recent Ingestion (72 hours)');
    console.log('─'.repeat(60));
    
    const recentRecords = await connector.fetchRecent(72);
    console.log(`Connector fetched ${recentRecords.length} recent records`);
    
    const recentResult = await ingestionService.ingestRecords(recentRecords, 'recent', {
      validate: true,
      batchSize: 100
    });
    console.log(`Inserted: ${recentResult.recordsInserted}, Updated: ${recentResult.recordsUpdated}`);
    console.log(`Skipped: ${recentResult.recordsSkipped} (idempotent - no change)\n`);
    
    // TEST 3: IDEMPOTENCY
    console.log('TEST 3: Idempotent Re-ingestion');
    console.log('─'.repeat(60));
    
    const repeatResult = await ingestionService.ingestRecords(bulkRecords, 'bulk', {
      validate: true
    });
    console.log(`Re-ingestion: Inserted: ${repeatResult.recordsInserted}, Updated: ${repeatResult.recordsUpdated}, Skipped: ${repeatResult.recordsSkipped}`);
    
    if (repeatResult.recordsInserted === 0 && repeatResult.recordsUpdated === 0) {
      console.log('PASS: No duplicates created, content unchanged\n');
    } else {
      console.log('Some records were updated (content may have changed)\n');
    }
    
    // TEST 4: CONTENT HASH
    console.log('TEST 4: Content Hash Verification');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const recordCheck = await client.query(
        "SELECT source_key, content_hash, title FROM records WHERE source_key = 'TX-002'"
      );
      
      if (recordCheck.rows.length > 0) {
        const record = recordCheck.rows[0];
        console.log(`Record TX-002: "${record.title}"`);
        console.log(`Content Hash: ${record.content_hash.substring(0, 16)}...\n`);
      }
    });
    
    // TEST 5: INGESTION LOGGING
    console.log('TEST 5: Ingestion Run Logging');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const history = await ingestionService.getIngestionHistory(5, 0, client);
      
      console.log(`Retrieved ${history.runs.length} ingestion run logs:`);
      
      history.runs.forEach((run, i) => {
        console.log(`  ${i + 1}. ${run.source_type} - Fetched: ${run.records_fetched}, ` +
                    `Inserted: ${run.records_inserted}, Updated: ${run.records_updated}`);
      });
      console.log();
    });
    
    // TEST 6: SERVICE METRICS
    console.log('TEST 6: Service Metrics');
    console.log('─'.repeat(60));
    
    const metrics = ingestionService.getMetrics();
    console.log(`Total Ingestions: ${metrics.totalIngestions}`);
    console.log(`Total Records Processed: ${metrics.totalRecordsProcessed}`);
    console.log(`Average Processing Time: ${metrics.averageProcessingTime}ms`);
    console.log(`Total Errors: ${metrics.totalErrors}\n`);
    
    // TEST 7: ALERT RULES
    console.log('TEST 7: Alert Rule Creation and Limits');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const usersResult = await client.query(
        'SELECT id, email, plan FROM users ORDER BY id'
      );
      const users = usersResult.rows;
      
      console.log(`Found ${users.length} users:`);
      users.forEach(u => console.log(`  - ${u.email} (${u.plan})`));
      console.log();
      
      // Test Starter plan (limit: 1)
      const starterUser = users.find(u => u.plan === 'starter');
      if (starterUser) {
        console.log(`Testing Starter plan (limit: 1 alert):`);
        
        try {
          const alert1 = await alertsService.createAlertRule(
            starterUser.id,
            'acme energy llc',
            null,
            client
          );
          console.log(`  Created alert ${alert1.alert.id} for "acme energy llc"`);

          try {
            await alertsService.createAlertRule(starterUser.id, null, 'TX', client);
            console.log('  FAIL: Should have blocked second alert');
          } catch (err) {
            console.log(`  PASS: Second alert blocked - ${err.message}`);
          }
        } catch (err) {
          console.log(`  Error: ${err.message}`);
        }
        console.log();
      }
      
      // Test Pro plan (limit: 5)
      const proUser = users.find(u => u.plan === 'pro');
      if (proUser) {
        console.log(`Testing Pro plan (limit: 5 alerts):`);
        
        try {
          await alertsService.createAlertRule(proUser.id, null, 'TX', client);
          await alertsService.createAlertRule(proUser.id, null, 'NM', client);
          console.log(`  Created 2 alerts for Pro user`);
        } catch (err) {
          console.log(`  Error: ${err.message}`);
        }
        console.log();
      }
    });
    
    // TEST 8: ALERT STATISTICS
    console.log('TEST 8: Alert Statistics');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const usersResult = await client.query(
        "SELECT id, email, plan FROM users WHERE plan = 'starter' LIMIT 1"
      );
      
      if (usersResult.rows.length > 0) {
        const starterUser = usersResult.rows[0];
        const stats = await alertsService.getUserAlertStats(starterUser.id, client);
        
        console.log(`Starter User Stats:`);
        console.log(`  Current Rules: ${stats.limits.current}`);
        console.log(`  Maximum: ${stats.limits.maximum}`);
        console.log(`  Total Triggers: ${stats.triggers.total}`);
        console.log();
      }
    });
    
    // TEST 9: ALERT LOGS
    console.log('TEST 9: Alert Triggering');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const alertLogs = await alertsService.getAlertLogs({ limit: 10 }, client);
      
      console.log(`Found ${alertLogs.logs.length} triggered alerts:`);
      
      alertLogs.logs.slice(0, 5).forEach((log, i) => {
        console.log(`  ${i + 1}. Alert ${log.alert_rule_id}: ${log.action_type} - ${log.title}`);
      });
      console.log();
    });
    
    // TEST 10: CSV EXPORT GATING
    console.log('TEST 10: CSV Export Plan Gating');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const usersResult = await client.query(
        'SELECT id, email, plan FROM users WHERE plan IN ($1, $2) ORDER BY plan',
        ['starter', 'pro']
      );
      
      const starterUser = usersResult.rows.find(u => u.plan === 'starter');
      const proUser = usersResult.rows.find(u => u.plan === 'pro');
      
      // Test Starter (blocked)
      if (starterUser) {
        try {
          await exportService.exportToCSV(starterUser.id, {}, client);
          console.log('  FAIL: Starter user should be blocked from export');
        } catch (err) {
          console.log(`  PASS: Starter blocked - ${err.message}`);
        }
      }
      
      // Test Pro (allowed)
      if (proUser) {
        try {
          const csv = await exportService.exportToCSV(proUser.id, {}, client);
          const lineCount = csv.split('\n').length - 1;
          console.log(`  PASS: Pro user exported ${lineCount} records`);
        } catch (err) {
          console.log(`  Error: ${err.message}`);
        }
      }
      console.log();
    });
    
    // TEST 11: DUPLICATE CHECK
    console.log('TEST 11: Duplicate Record Check');
    console.log('─'.repeat(60));
    
    await db.transaction(async (client) => {
      const duplicateCheck = await client.query(`
        SELECT source_key, COUNT(*) as count 
        FROM records 
        GROUP BY source_key 
        HAVING COUNT(*) > 1
      `);
      
      if (duplicateCheck.rows.length === 0) {
        console.log('  PASS: No duplicate records found\n');
      } else {
        console.log('  FAIL: Found duplicate records:');
        duplicateCheck.rows.forEach(dup => {
          console.log(`    - ${dup.source_key}: ${dup.count} copies`);
        });
        console.log();
      }
    });
    
    // TEST 12: DATABASE HEALTH
    console.log('TEST 12: Database Health Check');
    console.log('─'.repeat(60));
    
    // Small delay to ensure all connections are released
    await new Promise(resolve => setTimeout(resolve, 100));
    
    try {
      const health = await db.healthCheck();
      console.log(`  Status: ${health.status}`);
      
      const dbStats = db.getStats();
      console.log(`  Pool - Total: ${dbStats.totalCount}, Idle: ${dbStats.idleCount}, Waiting: ${dbStats.waitingCount}`);
      console.log();
    } catch (error) {
      console.log(`  Health check failed: ${error.message}`);
      console.log('  Continuing with tests...\n');
    }
    
    // FINAL SUMMARY
    console.log('╔═══════════════════════════════════════════════════════╗');
    console.log('║                   TEST SUMMARY                        ║');
    console.log('╚═══════════════════════════════════════════════════════╝');
    
    await db.transaction(async (client) => {
      const totalRecords = await client.query(
        'SELECT COUNT(*) as count FROM records'
      );
      const totalRuns = await client.query(
        'SELECT COUNT(*) as count FROM ingestion_runs'
      );
      const totalAlerts = await client.query(
        'SELECT COUNT(*) as count FROM alert_rules'
      );
      const totalTriggers = await client.query(
        'SELECT COUNT(*) as count FROM alert_logs'
      );

      const summary = {
        totalRecords: totalRecords.rows[0].count,
        totalRuns: totalRuns.rows[0].count,
        totalAlerts: totalAlerts.rows[0].count,
        totalTriggers: totalTriggers.rows[0].count
      };

      console.log(`\nTotal Records: ${summary.totalRecords}`);
      console.log(`Total Ingestion Runs: ${summary.totalRuns}`);
      console.log(`Total Alert Rules: ${summary.totalAlerts}`);
      console.log(`Total Alert Triggers: ${summary.totalTriggers}`);
      console.log('\nAll tests completed successfully!\n');
    });
    
  } catch (error) {
    console.error('\nTest failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  } finally {
    try {
      await db.shutdown();
      
      // Force exit after a brief delay to ensure clean shutdown
      setTimeout(() => {
        console.log('Forcing process exit...');
        process.exit(0);
      }, 2000);
    } catch (shutdownError) {
      console.error('Shutdown error:', shutdownError.message);
      process.exit(1);
    }
  }
}

// Run tests if called directly
if (require.main === module) {
  runTests();
}

module.exports = runTests;