'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const { callbackPromise } = require('nodemailer/lib/shared')

module.exports = async function generate(campaignId, lowDonationAmount, donationLimitAmount, contactMethod) {

    // Build the SQL
    var sql = buildSQL(campaignId, lowDonationAmount, donationLimitAmount, contactMethod)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount >= 1) {
            const row = dbres.rows[0]
            return {
                totalAmount: parseFloat(row.total_amount)
                , averageAmount: parseFloat(row.average_amount)
                , donationCount: parseInt(row.donation_count)
                , lowDonationCount: parseInt(row.low_amount_count)
                , donationLimitCount: parseInt(row.limit_amount_count)
                , lowDonationAmountPercent: parseInt(row.low_amount_perc)
                , donationLimitAmountPercent: parseInt(row.limit_amount_perc)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationSummary.js', 'DonationSummary', `Database error generating Donation Summary report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, lowDonationAmount, donationLimitAmount, contactMethod) {

    const lowAmount = (lowDonationAmount === undefined) ? 100 : parseFloat(lowDonationAmount)
    const limitAmount = (donationLimitAmount === undefined) ? 2900 : parseFloat(donationLimitAmount)

    var sql = `SELECT SUM(amount) total_amount\r\n`
    sql += `, ROUND(AVG(amount), 2) average_amount\r\n`
    sql += `, SUM(donation_count) donation_count\r\n`
    sql += `, SUM(low_amount_count) low_amount_count\r\n`
    sql += `, SUM(limit_amount_count) limit_amount_count\r\n`
    sql += `, (100 * SUM(low_amount_count)) / SUM(donation_count) low_amount_perc\r\n`
    sql += `, (100 * SUM(limit_amount_count)) / SUM(donation_count) limit_amount_perc\r\n`
    sql += `FROM (\r\n`
    sql += `    SELECT person_id, amount\r\n`
    sql += `    , 1 donation_count\r\n`
    sql += `    , CASE WHEN amount <= ${lowAmount} THEN 1 ELSE 0 END low_amount_count\r\n`
    sql += `    , CASE WHEN amount >= ${limitAmount} THEN 1 ELSE 0 END limit_amount_count\r\n`
    sql += `    FROM (\r\n`
    sql += `        SELECT CAST(cal.detail ->> 'personId' AS bigint) person_id\r\n`
    sql += `            , SUM(CAST(cal.detail ->> 'amount' AS decimal)) amount\r\n`
    sql += `            FROM base.contact_action_log cal\r\n`
    sql += `            INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `            INNER JOIN base.contact_reason cr ON cr.reason_id = cal.contact_reason_id\r\n`
    sql += `            INNER JOIN base.contact_method cm ON cm.method_id = cal.contact_method_id\r\n`
    sql += `            INNER JOIN base.contact_result cres ON cres.result_id = cal.contact_result_id\r\n`
    sql += `            WHERE ca.description ILIKE 'Contact responded'\r\n`
    sql += `            AND cr.description ILIKE 'Donation request'\r\n`
    sql += `            AND cres.description ILIKE 'Positive response'\r\n`
    sql += `            AND cal.campaign_id = ${campaignId}\r\n`
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
    sql += `            AND cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `            GROUP BY person_id\r\n`
    sql += `            ) inner_sql\r\n`
    sql += `    ) middle_sql;`
        
    return sql
}

