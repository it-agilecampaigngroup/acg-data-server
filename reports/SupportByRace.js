'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(campaignId) {

    // Build the SQL
    var sql = buildSQL(campaignId)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount >= 1) {
            const row = dbres.rows[0]
            return {
                totalSupportCount: parseInt(row.total_supportive_count)
                , totalNeutralCount: parseInt(row.total_neutral_count)
                , totalOpposedCount: parseInt(row.total_opposed_count)
                , totalSupportPerc: parseFloat(row.total_supportive_perc)
                , totalNeutralPerc: parseFloat(row.total_neutral_perc)
                , totalOpposedPerc: parseFloat(row.total_opposed_perc)
                , hispanicSupportPerc: parseFloat(row.hispanic_supportive_perc)
                , hispanicNeutralPerc: parseFloat(row.hispanic_neutral_perc)
                , hispanicOpposedPerc: parseFloat(row.hispanic_opposed_perc)
                , asianSupportPerc: parseFloat(row.asian_supportive_perc)
                , asianNeutralPerc: parseFloat(row.asian_neutral_perc)
                , asianOpposedPerc: parseFloat(row.asian_opposed_perc)
                , afamBlackSupportPerc: parseFloat(row.afam_black_supportive_perc)
                , afamBlackNeutralPerc: parseFloat(row.afam_black_neutral_perc)
                , afamBlackOpposedPerc: parseFloat(row.afam_black_opposed_perc)
                , whiteSupportPerc: parseFloat(row.white_supportive_perc)
                , whiteNeutralPerc: parseFloat(row.white_neutral_perc)
                , whiteOpposedPerc: parseFloat(row.white_opposed_perc)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'SupportByRace.js', 'SupportByRace', `Database error generating Support By Race report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId) {

    var sql = `SELECT SUM(supportive) total_supportive_count, SUM(neutral) total_neutral_count, SUM(opposed) total_opposed_count\r\n`
    sql += `    , ROUND(100 * SUM(supportive) / SUM(supportive + neutral + opposed)) total_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(neutral) / SUM(supportive + neutral + opposed)) total_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(opposed) / SUM(supportive + neutral + opposed)) total_opposed_perc\r\n`
    
    sql += `	, ROUND(100 * SUM(hispanic_supportive) / SUM(hispanic_supportive + hispanic_neutral + hispanic_opposed)) hispanic_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(hispanic_neutral) / SUM(hispanic_supportive + hispanic_neutral + hispanic_opposed)) hispanic_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(hispanic_opposed) / SUM(hispanic_supportive + hispanic_neutral + hispanic_opposed)) hispanic_opposed_perc\r\n`
    
    sql += `	, ROUND(100 * SUM(asian_supportive) / SUM(asian_supportive + asian_neutral + asian_opposed)) asian_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(asian_neutral) / SUM(asian_supportive + asian_neutral + asian_opposed)) asian_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(asian_opposed) / SUM(asian_supportive + asian_neutral + asian_opposed)) asian_opposed_perc\r\n`
    
    sql += `	, ROUND(100 * SUM(afam_black_supportive) / SUM(afam_black_supportive + afam_black_neutral + afam_black_opposed)) afam_black_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(afam_black_neutral) / SUM(afam_black_supportive + afam_black_neutral + afam_black_opposed)) afam_black_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(afam_black_opposed) / SUM(afam_black_supportive + afam_black_neutral + afam_black_opposed)) afam_black_opposed_perc\r\n`
    
    sql += `	, ROUND(100 * SUM(white_supportive) / SUM(white_supportive + white_neutral + white_opposed)) white_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(white_neutral) / SUM(white_supportive + white_neutral + white_opposed)) white_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(white_opposed) / SUM(white_supportive + white_neutral + white_opposed)) white_opposed_perc\r\n`
    sql += `FROM (\r\n`
    sql += `	SELECT act.supportive, act.neutral, act.opposed\r\n`
    sql += `	, act.supportive * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_supportive\r\n`
    sql += `	, act.neutral * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_neutral\r\n`
    sql += `	, act.opposed * CAST(census.hispanic_pop AS decimal)/census.total_pop hispanic_opposed\r\n`
    
    sql += `	, act.supportive * CAST(census.asian_pop AS decimal)/census.total_pop asian_supportive\r\n`
    sql += `	, act.neutral * CAST(census.asian_pop AS decimal)/census.total_pop asian_neutral\r\n`
    sql += `	, act.opposed * CAST(census.asian_pop AS decimal)/census.total_pop asian_opposed\r\n`
    
    sql += `	, act.supportive * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_supportive\r\n`
    sql += `	, act.neutral * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_neutral\r\n`
    sql += `	, act.opposed * CAST(census.afam_black_pop AS decimal)/census.total_pop afam_black_opposed\r\n`
    
    sql += `	, act.supportive * CAST(census.white_pop AS decimal)/census.total_pop white_supportive\r\n`
    sql += `	, act.neutral * CAST(census.white_pop AS decimal)/census.total_pop white_neutral\r\n`
    sql += `	, act.opposed * CAST(census.white_pop AS decimal)/census.total_pop white_opposed\r\n`
    sql += `	FROM (\r\n`
    sql += `		SELECT DISTINCT CAST(cal.detail ->> 'personId' AS BIGINT) person_id\r\n`
    sql += `			, CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) < 3 THEN 1 ELSE 0 END supportive\r\n`
    sql += `			, CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) = 3 THEN 1 ELSE 0 END neutral\r\n`
    sql += `			, CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) > 3 THEN 1 ELSE 0 END opposed\r\n`
    sql += `			FROM base.contact_action_log cal\r\n`
    sql += `			INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `			WHERE cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `			AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `			AND cal.campaign_id = ${campaignId}\r\n`    
    sql += `		) act\r\n`
    sql += `	INNER JOIN base.person_address pa ON pa.person_id = act.person_id AND pa.is_primary = true\r\n`
    sql += `	INNER JOIN base.address addr ON addr.address_id = pa.address_id\r\n`
    sql += `	INNER JOIN census.census_block blk ON blk.block_id = addr.census_block_id\r\n`
    sql += `	INNER JOIN census.census_2010 census ON census.census_block_id = addr.census_block_id\r\n`
    sql += `	) metrics`
    sql += `;`
    
    return sql
}

