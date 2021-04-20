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
                , contactRequestCount: parseInt(row.contact_request_count)
                , totalReponseCount: parseInt(row.total_reponse_count)
                , contactReachedCount: parseInt(row.contact_reached_count)
                , positiveReponseCount: parseInt(row.positive_reponse_count)
                , negativeReponseCount: parseInt(row.negative_reponse_count)
                , failedAttemptCount: parseInt(row.failed_attempt_count)
                , rejectedContactCount: parseInt(row.rejected_contact_count)
                , totalDonationAmount: parseFloat(row.total_donation_amount)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationCallsToDate.js', 'DonationCallsToDate', `Database error generating Donation Calls To Date report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, actorId, dateStart, dateEnd) {

    // Increment dateEnd to simplify the query
    var nextDate = new Date(dateEnd)
    nextDate.setDate(nextDate.getDate() + 1);
    dateEnd = new Date(nextDate).toLocaleDateString()

    // Build the SQL
    var sql = `SELECT\r\n`
    sql += `(SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact requested'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) contact_request_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE (contact_action ILIKE 'Contact responded'\r\n`
    sql += `    OR contact_action ILIKE 'Contact rejected'\r\n`
    sql += `    OR contact_action ILIKE 'Contact attempt failed')\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) total_reponse_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact responded'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) contact_reached_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact responded'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND contact_result ILIKE 'Positive response'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) positive_reponse_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact responded'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND contact_result ILIKE 'Negative response'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) negative_reponse_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact attempt failed'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) failed_attempt_count\r\n`

    sql += `, (SELECT COUNT(date_created)\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact rejected'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `) rejected_contact_count\r\n`

    sql += `, ROUND( (SELECT SUM(CAST(detail->>'amount' AS decimal))\r\n`
    sql += `FROM base.v_contact_action_log\r\n`
    sql += `WHERE contact_action ILIKE 'Contact responded'\r\n`
    sql += `AND contact_method ILIKE 'Phone call'\r\n`
    sql += `AND contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND contact_result ILIKE 'Positive response'\r\n`
    sql += `AND detail::jsonb ? 'amount'\r\n`
    sql += `AND detail ->> 'amount' ~ '^[0-9\.]+$'\r\n`
    sql += `AND campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) { sql += `AND actor_id = ${actorId}\r\n` }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `), 2) total_donation_amount\r\n`
    sql += `;`
    
    return sql
}

