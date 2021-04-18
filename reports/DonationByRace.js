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
        if( dbres.rowCount >= 1) {
            const row = dbres.rows[0]
            return {
                totalAmount: parseFloat(row.total_amount)
                , averageAmount: parseFloat(row.average_amount)
                , yesCount: parseInt(row.yes_count)
                , noCount: parseInt(row.no_count)
                , responseCount: parseInt(row.response_count)
                
                , hispanicAmount: parseFloat(row.hispanic_amount)
                , hispanicAverageAmount: parseFloat(row.hispanic_average)
                , hispanicYesCount: parseInt(row.hispanic_yes_count)
                , hispanicNoCount: parseInt(row.hispanic_no_count)
                
                , asianAmount: parseFloat(row.asian_amount)
                , asianAverageAmount: parseFloat(row.asian_average)
                , asianYesCount: parseInt(row.asian_yes_count)
                , asianNoCount: parseInt(row.asian_no_count)
                
                , afamBlackAmount: parseFloat(row.afam_black_amount)
                , afamBlackAverageAmount: parseFloat(row.afam_black_average)
                , afamBlackYesCount: parseInt(row.afam_black_yes_count)
                , afamBlackNoCount: parseInt(row.afam_black_no_count)
                
                , whiteAmount: parseFloat(row.white_amount)
                , whiteAverageAmount: parseFloat(row.white_average)
                , whiteYesCount: parseInt(row.white_yes_count)
                , whiteNoCount: parseInt(row.white_no_count)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationByRace.js', 'DonationByRace', `Database error generating Donation By Race report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, contactMethod) {

    var sql = `SELECT SUM(d.amount) total_amount\r\n`
    sql += `, CASE WHEN SUM(d.yes_count) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.amount)/SUM(d.yes_count), 2)\r\n`
    sql += `    ELSE SUM(d.amount)\r\n`
    sql += `    END average_amount\r\n`
    sql += `, SUM(d.yes_count) yes_count\r\n`
    sql += `, SUM(d.no_count) no_count\r\n`
    sql += `, SUM(d.response_count) response_count\r\n`

    sql += `-- Hispanic\r\n`
    sql += `, ROUND(SUM(d.hispanic_amount), 2) hispanic_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.hispanic_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.hispanic_amount)/ROUND(SUM(d.hispanic_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.hispanic_amount), 2)\r\n`
    sql += `    END hispanic_average\r\n`
    sql += `, ROUND(SUM(d.hispanic_yes_count)) hispanic_yes_count\r\n`
    sql += `, ROUND(SUM(d.hispanic_no_count)) hispanic_no_count\r\n`

    sql += `-- Asian\r\n`
    sql += `, ROUND(SUM(d.asian_amount), 2) asian_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.asian_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.asian_amount)/ROUND(SUM(d.asian_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.asian_amount), 2)\r\n`
    sql += `    END asian_average\r\n`
    sql += `, ROUND(SUM(d.asian_yes_count)) asian_yes_count\r\n`
    sql += `, ROUND(SUM(d.asian_no_count)) asian_no_count\r\n`

    sql += `-- AFAm/Black\r\n`
    sql += `, ROUND(SUM(d.afam_black_amount), 2) afam_black_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.afam_black_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.afam_black_amount)/ROUND(SUM(d.afam_black_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.afam_black_amount), 2)\r\n`
    sql += `    END afam_black_average\r\n`
    sql += `, ROUND(SUM(d.afam_black_yes_count)) afam_black_yes_count\r\n`
    sql += `, ROUND(SUM(d.afam_black_no_count)) afam_black_no_count\r\n`

    sql += `-- White\r\n`
    sql += `, ROUND(SUM(d.white_amount), 2) white_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.white_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.white_amount)/ROUND(SUM(d.white_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.white_amount), 2)\r\n`
    sql += `    END white_average\r\n`
    sql += `, ROUND(SUM(d.white_yes_count)) white_yes_count\r\n`
    sql += `, ROUND(SUM(d.white_no_count)) white_no_count\r\n`

    sql += `FROM (\r\n`
    sql += `    SELECT donations.amount, donations.yes_count, donations.no_count, donations.response_count\r\n`
    sql += `    -- Hispanic\r\n`
    sql += `	, donations.amount * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_amount\r\n`
    sql += `	, donations.yes_count * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_yes_count\r\n`
    sql += `	, donations.no_count * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_no_count\r\n`
    sql += `    -- Asian\r\n`    
    sql += `	, donations.amount * CAST(census.asian_pop AS decimal)/census.total_pop asian_amount\r\n`
    sql += `	, donations.yes_count * CAST(census.asian_pop AS decimal)/census.total_pop asian_yes_count\r\n`
    sql += `	, donations.no_count * CAST(census.asian_pop AS decimal)/census.total_pop asian_no_count\r\n`
    sql += `    -- AFAm/Black\r\n`    
    sql += `	, donations.amount * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_amount\r\n`
    sql += `	, donations.yes_count * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_yes_count\r\n`
    sql += `	, donations.no_count * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_no_count\r\n`
    sql += `    -- White\r\n`    
    sql += `	, donations.amount * CAST(census.white_pop AS decimal)/census.total_pop white_amount\r\n`
    sql += `	, donations.yes_count * CAST(census.white_pop AS decimal)/census.total_pop white_yes_count\r\n`
    sql += `	, donations.no_count * CAST(census.white_pop AS decimal)/census.total_pop white_no_count\r\n`
    sql += `    FROM (\r\n`
    sql += `        SELECT CAST(cal.detail ->> 'personId' AS bigint) person_id\r\n`
    sql += `        , SUM(CASE WHEN cal.detail ->> 'amount' <> '' THEN CAST(cal.detail ->> 'amount' AS decimal) ELSE 0 END) amount\r\n`
    sql += `        , 1 response_count\r\n`
    sql += `        , CASE WHEN cres.description ILIKE 'Positive response' THEN 1 ELSE 0 END yes_count\r\n`
    sql += `        , CASE WHEN cres.description NOT ILIKE 'Positive response' THEN 1 ELSE 0 END no_count\r\n`
    sql += `        FROM base.contact_action_log cal\r\n`
    sql += `        INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `        INNER JOIN base.contact_reason cr ON cr.reason_id = cal.contact_reason_id\r\n`
    sql += `        INNER JOIN base.contact_method cm ON cm.method_id = cal.contact_method_id\r\n`
    sql += `        INNER JOIN base.contact_result cres ON cres.result_id = cal.contact_result_id\r\n`
    sql += `        WHERE ca.description ILIKE 'Contact responded'\r\n`
    sql += `        AND cr.description ILIKE 'Donation request'\r\n`
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
    sql += `        AND cal.campaign_id = ${campaignId}\r\n`
    sql += `        GROUP BY person_id, cres.description\r\n`
    sql += `        ) donations\r\n`
    sql += `    INNER JOIN base.person_address pa ON pa.person_id = donations.person_id AND pa.is_primary = true\r\n`
    sql += `    INNER JOIN base.address addr ON addr.address_id = pa.address_id\r\n`
    sql += `    INNER JOIN census.census_block blk ON blk.block_id = addr.census_block_id\r\n`
    sql += `	INNER JOIN census.census_2010 census ON census.census_block_id = addr.census_block_id\r\n`
    sql += `) d\r\n`
    sql += `;`
    
    return sql
}

