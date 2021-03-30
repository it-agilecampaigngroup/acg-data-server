'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(campaignId, actorId, dateStart, dateEnd) {

    // Build the SQL
    var sql = buildSQL(campaignId, actorId, dateStart, dateEnd)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount == 1) {
            const row = dbres.rows[0]
            return {
                campaignId: campaignId
                , actorId: (actorId == 0) ? '' : actorId
                , dateStart: dateStart
                , dateEnd: dateEnd
                , 1: row.rating1
                , 2: row.rating2
                , 3: row.rating3
                , 4: row.rating4
                , 5: row.rating5
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'SupportResultSummary.js', 'SupportResultSummary', `Database error generating Support Result Summary report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, actorId, dateStart, dateEnd) {

    // Increment dateEnd to simplify the query
    var nextDate = new Date(dateEnd)
    nextDate.setDate(nextDate.getDate() + 1);
    dateEnd = new Date(nextDate).toLocaleDateString()

    // Build the SQL
    var sql = `SELECT DISTINCT\r\n`
    
    sql += `(SELECT COUNT(cal.log_id)\r\n`
    sql += `FROM base.contact_action_log cal\r\n`
    sql += `INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `WHERE cal.campaign_id = cal_main.campaign_id\r\n`
    if( actorId != 0 ) {
        sql += `AND cal.actor_id = cal_main.actor_id\r\n`
    }
    sql += `AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `AND cal.detail ->> 'supportResult' ~ '^[0-9\.]'\r\n`
    sql += `AND CAST(cal.detail ->> 'supportResult' AS INTEGER) = 1\r\n`
    sql += `AND cal.date_created >= '${dateStart}'\r\n`
    sql += `AND cal.date_created < '${dateEnd}'\r\n`
    sql += `) rating1\r\n`
    
    sql += `, (SELECT COUNT(cal.log_id)\r\n`
    sql += `FROM base.contact_action_log cal\r\n`
    sql += `INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `WHERE cal.campaign_id = cal_main.campaign_id\r\n`
    if( actorId != 0 ) {
        sql += `AND cal.actor_id = cal_main.actor_id\r\n`
    }
    sql += `AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `AND cal.detail ->> 'supportResult' ~ '^[0-9\.]'\r\n`
    sql += `AND CAST(cal.detail ->> 'supportResult' AS INTEGER) = 2\r\n`
    sql += `AND cal.date_created >= '${dateStart}'\r\n`
    sql += `AND cal.date_created < '${dateEnd}'\r\n`
    sql += `) rating2\r\n`
    
    sql += `, (SELECT COUNT(cal.log_id)\r\n`
    sql += `FROM base.contact_action_log cal\r\n`
    sql += `INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `WHERE cal.campaign_id = cal_main.campaign_id\r\n`
    if( actorId != 0 ) {
        sql += `AND cal.actor_id = cal_main.actor_id\r\n`
    }
    sql += `AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `AND cal.detail ->> 'supportResult' ~ '^[0-9\.]'\r\n`
    sql += `AND CAST(cal.detail ->> 'supportResult' AS INTEGER) = 3\r\n`
    sql += `AND cal.date_created >= '${dateStart}'\r\n`
    sql += `AND cal.date_created < '${dateEnd}'\r\n`
    sql += `) rating3\r\n`
    
    sql += `, (SELECT COUNT(cal.log_id)\r\n`
    sql += `FROM base.contact_action_log cal\r\n`
    sql += `INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `WHERE cal.campaign_id = cal_main.campaign_id\r\n`
    if( actorId != 0 ) {
        sql += `AND cal.actor_id = cal_main.actor_id\r\n`
    }
    sql += `AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `AND cal.detail ->> 'supportResult' ~ '^[0-9\.]'\r\n`
    sql += `AND CAST(cal.detail ->> 'supportResult' AS INTEGER) = 4\r\n`
    sql += `AND cal.date_created >= '${dateStart}'\r\n`
    sql += `AND cal.date_created < '${dateEnd}'\r\n`
    sql += `) rating4\r\n`
    
    sql += `, (SELECT COUNT(cal.log_id)\r\n`
    sql += `FROM base.contact_action_log cal\r\n`
    sql += `INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `WHERE cal.campaign_id = cal_main.campaign_id\r\n`
    if( actorId != 0 ) {
        sql += `AND cal.actor_id = cal_main.actor_id\r\n`
    }
    sql += `AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `AND cal.detail ->> 'supportResult' ~ '^[0-9\.]'\r\n`
    sql += `AND CAST(cal.detail ->> 'supportResult' AS INTEGER) = 5\r\n`
    sql += `AND cal.date_created >= '${dateStart}'\r\n`
    sql += `AND cal.date_created < '${dateEnd}'\r\n`
    sql += `) rating5\r\n`
    
    sql += `FROM base.contact_action_log cal_main\r\n`
    sql += `WHERE cal_main.campaign_id = ${campaignId}\r\n`
    sql += `AND cal_main.date_created >= '${dateStart}'\r\n`
    sql += `AND cal_main.date_created < '${dateEnd}'\r\n`
    if( actorId != 0 ) {
        sql += `AND cal_main.actor_id = ${actorId}\r\n`
    } 
    sql += `;`
    
    return sql
}

