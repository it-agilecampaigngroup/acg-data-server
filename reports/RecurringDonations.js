'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(campaignId, contactMethod) {

    // Build the SQL
    var sql = buildSQL(campaignId, contactMethod)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        const dbres = await db.query(sql)
        const report = []
        if( dbres.rowCount >= 1) {

            // Process each record
            for( var idx = 0; idx < dbres.rowCount; idx++) {
                let row = dbres.rows[idx]
                
                // Add the record to the report
                report.push(
                    {
                        firstName: row.first_name
                        , middleName: row.middle_name
                        , lastName: row.last_name
                        , suffix: row.suffix
                        , amount: row.amount
                    }
                )
            }
        }
        return report
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'RecurringDonations.js', 'RecurringDonations', `Database error generating Recurring Donations report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, contactMethod) {

    var sql = `SELECT p.first_name, p.middle_name, p.last_name, p.suffix, donations.amount\r\n`
    sql += `FROM (\r\n`
    sql += `    SELECT CAST(cal.detail ->> 'personId' AS bigint) person_id\r\n`
    sql += `        , SUM(CAST(cal.detail ->> 'amount' AS decimal)) amount\r\n`
    sql += `        FROM base.contact_action_log cal\r\n`
    sql += `        INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `        INNER JOIN base.contact_reason cr ON cr.reason_id = cal.contact_reason_id\r\n`
    sql += `        INNER JOIN base.contact_method cm ON cm.method_id = cal.contact_method_id\r\n`
    sql += `        INNER JOIN base.contact_result cres ON cres.result_id = cal.contact_result_id\r\n`
    sql += `        WHERE ca.description ILIKE 'Contact responded'\r\n`
    sql += `        AND cr.description ILIKE 'Donation request'\r\n`
    sql += `        AND cres.description ILIKE 'Positive response'\r\n`
    sql += `        AND cal.campaign_id = ${campaignId}\r\n`
    if( contactMethod !== undefined ) {
        if( contactMethod.toUpperCase() === 'PHONE CALL') {
            sql += `        AND cm.description ILIKE 'Phone call'\r\n`
        }
        else if( contactMethod.toUpperCase() === 'CANVASS') {
            sql += `        AND cm.description ILIKE 'Canvass'\r\n`
        }
        else if( contactMethod.toUpperCase() === 'EMAIL') {
            sql += `        AND cm.description ILIKE 'Email'\r\n`
        }
        else if( contactMethod.toUpperCase() === 'TEXT') {
            sql += `        AND cm.description ILIKE 'Text'\r\n`
        }
    }
    sql += `        AND (cal.detail ->> 'recurring') IS NOT NULL\r\n`
    sql += `        AND UPPER(cal.detail ->> 'recurring') = 'TRUE'\r\n`
    sql += `        AND cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `        GROUP BY person_id\r\n`
    sql += `        ) donations\r\n`
    sql += `INNER JOIN base.person p ON p.person_id = donations.person_id\r\n`
    sql += `;`

    return sql
}

