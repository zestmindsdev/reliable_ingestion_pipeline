/**
 * Alerts Service - Production Grade
 * Handles alert rule management with caching, batch processing, and rate limiting
 */

const db = require('./database');
const { 
  ValidationError, 
  BusinessLogicError, 
  NotFoundError,
  AuthorizationError,
  DatabaseError
} = require('./errors');


class AlertsService {
  constructor() {
    this.ruleCache = new Map();
    this.cacheTimeout = 5 * 60 * 1000; // 5 minutes
    this.lastCacheUpdate = null;
    
    this.planLimits = {
      starter: 1,
      pro: 5,
      team: Infinity
    };
  }

  /**
   * Load alert rules into cache
   */
  async refreshCache(client = null) {
    const executor = client ?? db;
    
    try {
      const result = await executor.query(
        'SELECT * FROM alert_rules ORDER BY created_at DESC'
      );
      
      this.ruleCache.clear();
      
      for (const rule of result.rows) {
        if (!this.ruleCache.has(rule.user_id)) {
          this.ruleCache.set(rule.user_id, []);
        }
        this.ruleCache.get(rule.user_id).push(rule);
      }
      
      this.lastCacheUpdate = Date.now();
      console.log(`Alert rules cache refreshed: ${result.rows.length} rules loaded`);
    } catch (error) {
      console.error('Failed to refresh alert rules cache:', error);
      throw error;
    }
  }

  /**
   * Get cached rules or refresh if stale
   */
  async getCachedRules(userId = null, client = null) {
    const cacheAge = this.lastCacheUpdate 
      ? Date.now() - this.lastCacheUpdate 
      : Infinity;

    if (cacheAge > this.cacheTimeout) {
      await this.refreshCache(client);
    }

    if (userId) {
      return this.ruleCache.get(userId) || [];
    }

    return Array.from(this.ruleCache.values()).flat();
  }

  /**
   * Validate user exists and get their plan
   */
  async getUserPlan(userId, client = null) {
    const executor = client ?? db;

    const result = await executor.query(
      'SELECT plan FROM users WHERE id = $1',
      [userId]
    );

    if (result.rows.length === 0) {
      throw new NotFoundError('User', userId);
    }

    return result.rows[0].plan;
  }

  /**
   * Check if user can create more alerts
   */
  async canCreateAlert(userId, client = null) {
    const executor = client ?? db;

    const plan = await this.getUserPlan(userId, executor);
    const limit = this.planLimits[plan];

    if (limit === Infinity) {
      return { allowed: true, plan, currentCount: 0, limit: 'unlimited' };
    }

    const countResult = await executor.query(
      'SELECT COUNT(*) as count FROM alert_rules WHERE user_id = $1',
      [userId]
    );

    const currentCount = parseInt(countResult.rows[0].count);
    const allowed = currentCount < limit;

    return {
      allowed,
      plan,
      currentCount,
      limit,
      remaining: allowed ? limit - currentCount : 0
    };
  }


  /**
   * Create an alert rule with validation
   */
  async createAlertRule(userId, entityNameNorm = null, region = null, client = null) {
    const executor = client ?? db;
    
    // Validate inputs
    if (!userId || !Number.isInteger(userId)) {
      throw new ValidationError('userId must be a valid integer');
    }

    if (!entityNameNorm && !region) {
      throw new ValidationError(
        'At least one filter (entityNameNorm or region) must be provided'
      );
    }

    if (entityNameNorm && entityNameNorm.length > 255) {
      throw new ValidationError('entityNameNorm exceeds maximum length of 255');
    }

    if (region && !/^[A-Z]{2}$/.test(region)) {
      throw new ValidationError('region must be a 2-letter uppercase code');
    }

    // Check user exists and get plan
    const plan = await this.getUserPlan(userId, executor);

    // Check if user can create more alerts
    const { allowed, currentCount, limit } = await this.canCreateAlert(userId, executor);

    if (!allowed) {
      throw new BusinessLogicError(
        `Alert limit reached for ${plan} plan (${currentCount}/${limit}). ` +
        `Please upgrade to create more alerts.`
      );
    }

    // Create the alert rule
    try {
      const result = await executor.query(
        `INSERT INTO alert_rules (user_id, entity_name_norm, region)
        VALUES ($1, $2, $3)
        RETURNING *`,
        [userId, entityNameNorm, region]
      );

      // Invalidate cache
      this.lastCacheUpdate = null;

      console.log(`Alert rule created: user=${userId}, entity=${entityNameNorm}, region=${region}`);

      return {
        alert: result.rows[0],
        planInfo: {
          plan,
          currentCount: currentCount + 1,
          limit: limit === Infinity ? 'unlimited' : limit
        }
      };
    } catch (error) {
      throw new DatabaseError(`Failed to create alert rule: ${error.message}`, error);
    }
  }

  /**
   * Check and trigger alerts for a record (optimized with caching)
   */
  async checkAndTriggerAlerts(recordId, actionType, client = null) {
    const executor = client ?? db;
    
    if (!['insert', 'update'].includes(actionType)) {
      throw new ValidationError('actionType must be either "insert" or "update"');
    }

    // Get record details
    const recordResult = await executor.query(
      'SELECT entity_name_norm, region FROM records WHERE id = $1',
      [recordId]
    );

    if (recordResult.rows.length === 0) {
      console.warn(`Record ${recordId} not found for alert triggering`);
      return { triggered: 0 };
    }

    const record = recordResult.rows[0];

    // Find matching alert rules (optimized query)
    const alertsResult = await executor.query(
      `SELECT id, user_id FROM alert_rules 
       WHERE (entity_name_norm IS NULL OR entity_name_norm = $1)
         AND (region IS NULL OR region = $2)`,
      [record.entity_name_norm, record.region]
    );

    if (alertsResult.rows.length === 0) {
      return { triggered: 0 };
    }

    // Batch insert alert logs
    const alertLogValues = alertsResult.rows
      .map((alert, index) => {
        const offset = index * 3;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3})`;
      })
      .join(', ');

    const alertLogParams = alertsResult.rows.flatMap(alert => [
      alert.id,
      recordId,
      actionType
    ]);

    await executor.query(
      `INSERT INTO alert_logs (alert_rule_id, record_id, action_type)
       VALUES ${alertLogValues}`,
      alertLogParams
    );

    const triggeredCount = alertsResult.rows.length;
    
    console.log(
      `Triggered ${triggeredCount} alert(s) for record ${recordId} (${actionType})`
    );

    return { 
      triggered: triggeredCount,
      alertIds: alertsResult.rows.map(a => a.id)
    };
  }

  /**
   * Get all alert rules for a user
   */
  async getUserAlertRules(userId, client = null) {
    const executor = client ?? db;
    
    if (!userId || !Number.isInteger(userId)) {
      throw new ValidationError('userId must be a valid integer');
    }

    // Verify user exists
    await this.getUserPlan(userId, executor);

    const result = await executor.query(
      `SELECT ar.*, 
              (SELECT COUNT(*) FROM alert_logs WHERE alert_rule_id = ar.id) as trigger_count
       FROM alert_rules ar
       WHERE user_id = $1 
       ORDER BY created_at DESC`,
      [userId]
    );

    return result.rows;
  }

  /**
   * Get alert logs with pagination and filtering
   */
  async getAlertLogs(options = {}, client = null) {
    const executor = client ?? db;
    
    const {
      alertRuleId = null,
      userId = null,
      actionType = null,
      limit = 50,
      offset = 0
    } = options;

    if (limit > 100) {
      throw new ValidationError('Limit cannot exceed 100');
    }

    const conditions = [];
    const params = [];
    let paramIndex = 1;

    if (alertRuleId) {
      conditions.push(`al.alert_rule_id = $${paramIndex++}`);
      params.push(alertRuleId);
    }

    if (userId) {
      conditions.push(`ar.user_id = $${paramIndex++}`);
      params.push(userId);
    }

    if (actionType) {
      if (!['insert', 'update'].includes(actionType)) {
        throw new ValidationError('actionType must be either "insert" or "update"');
      }
      conditions.push(`al.action_type = $${paramIndex++}`);
      params.push(actionType);
    }

    const whereClause = conditions.length > 0 
      ? `WHERE ${conditions.join(' AND ')}` 
      : '';

    const query = `
      SELECT 
        al.id,
        al.alert_rule_id,
        al.record_id,
        al.action_type,
        al.triggered_at,
        ar.entity_name_norm,
        ar.region,
        ar.user_id,
        r.source_key,
        r.title,
        r.entity_name_raw
      FROM alert_logs al
      JOIN alert_rules ar ON al.alert_rule_id = ar.id
      JOIN records r ON al.record_id = r.id
      ${whereClause}
      ORDER BY al.triggered_at DESC
      LIMIT $${paramIndex++} OFFSET $${paramIndex++}
    `;

    params.push(limit, offset);

    const result = await executor.query(query, params);

    // Get total count
    const countQuery = `
      SELECT COUNT(*) as total
      FROM alert_logs al
      JOIN alert_rules ar ON al.alert_rule_id = ar.id
      ${whereClause}
    `;

    const countResult = await executor.query(
      countQuery, 
      params.slice(0, params.length - 2)
    );

    return {
      logs: result.rows,
      pagination: {
        limit,
        offset,
        total: parseInt(countResult.rows[0].total)
      }
    };
  }

  /**
   * Delete an alert rule with authorization check
   */
  async deleteAlertRule(alertRuleId, userId, client = null) {
    const executor = client ?? db;
    
    if (!Number.isInteger(alertRuleId) || !Number.isInteger(userId)) {
      throw new ValidationError('alertRuleId and userId must be valid integers');
    }

    const result = await executor.query(
      'DELETE FROM alert_rules WHERE id = $1 AND user_id = $2 RETURNING *',
      [alertRuleId, userId]
    );

    if (result.rows.length === 0) {
      // Check if rule exists but belongs to different user
      const existsResult = await executor.query(
        'SELECT user_id FROM alert_rules WHERE id = $1',
        [alertRuleId]
      );

      if (existsResult.rows.length > 0) {
        throw new AuthorizationError('You do not have permission to delete this alert rule');
      }

      throw new NotFoundError('Alert rule', alertRuleId);
    }

    // Invalidate cache
    this.lastCacheUpdate = null;

    console.log(`Alert rule deleted: id=${alertRuleId}, user=${userId}`);

    return result.rows[0];
  }

  /**
   * Get alert statistics for a user
   */
  async getUserAlertStats(userId, client = null) {
    const executor = client ?? db;
    
    if (!Number.isInteger(userId)) {
      throw new ValidationError('userId must be a valid integer');
    }

    const plan = await this.getUserPlan(userId, executor);

    const statsResult = await executor.query(
      `SELECT 
         COUNT(DISTINCT ar.id) as total_rules,
         COUNT(al.id) as total_triggers,
         COUNT(CASE WHEN al.action_type = 'insert' THEN 1 END) as insert_triggers,
         COUNT(CASE WHEN al.action_type = 'update' THEN 1 END) as update_triggers
       FROM alert_rules ar
       LEFT JOIN alert_logs al ON ar.id = al.alert_rule_id
       WHERE ar.user_id = $1`,
      [userId]
    );

    const stats = statsResult.rows[0];

    return {
      userId,
      plan,
      limits: {
        current: parseInt(stats.total_rules),
        maximum: this.planLimits[plan] === Infinity 
          ? 'unlimited' 
          : this.planLimits[plan],
        remaining: this.planLimits[plan] === Infinity
          ? 'unlimited'
          : this.planLimits[plan] - parseInt(stats.total_rules)
      },
      triggers: {
        total: parseInt(stats.total_triggers),
        inserts: parseInt(stats.insert_triggers),
        updates: parseInt(stats.update_triggers)
      }
    };
  }
}

// Export singleton instance
const alertsService = new AlertsService();

module.exports = alertsService;