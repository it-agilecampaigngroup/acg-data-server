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

                , under10KSupportPerc: parseFloat(row.under_10k_supportive_perc)
                , under10KNeutralPerc: parseFloat(row.under_10k_neutral_perc)
                , under10KOpposedPerc: parseFloat(row.under_10k_opposed_perc)

                , tenTo24kSupportPerc: parseFloat(row.ten_to_24k_supportive_perc)
                , tenTo24kNeutralPerc: parseFloat(row.ten_to_24k_neutral_perc)
                , tenTo24kOpposedPerc: parseFloat(row.ten_to_24k_opposed_perc)

                , twentyFiveTo49kSupportPerc: parseFloat(row.twenty_five_to_49k_supportive_perc)
                , twentyFiveTo49kNeutralPerc: parseFloat(row.twenty_five_to_49k_neutral_perc)
                , twentyFiveTo49kOpposedPerc: parseFloat(row.twenty_five_to_49k_opposed_perc)

                , fiftyTo74kSupportPerc: parseFloat(row.fifty_to_74k_supportive_perc)
                , fiftyTo74kkNeutralPerc: parseFloat(row.fifty_to_74k_neutral_perc)
                , fiftyTo74kOpposedPerc: parseFloat(row.fifty_to_74k_opposed_perc)

                , seventyFivePlusSupportPerc: parseFloat(row.seventy_five_and_up_supportive_perc)
                , seventyFivePlusNeutralPerc: parseFloat(row.seventy_five_and_up_neutral_perc)
                , seventyFivePlusOpposedPerc: parseFloat(row.seventy_five_and_up_opposed_perc)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'SupportByIncome.js', 'SupportByIncome', `Database error generating Support By Income report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId) {

    var sql = `SELECT SUM(supportive) total_supportive_count, SUM(neutral) total_neutral_count, SUM(opposed) total_opposed_count\r\n`
    sql += `	, ROUND(100 * SUM(supportive) / SUM(supportive + neutral + opposed)) total_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(neutral) / SUM(supportive + neutral + opposed)) total_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(opposed) / SUM(supportive + neutral + opposed)) total_opposed_perc\r\n`
        
    sql += `	, ROUND(100 * SUM(under_10k_supportive) / SUM(under_10k_supportive + under_10k_neutral + under_10k_opposed)) under_10k_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(under_10k_neutral) / SUM(under_10k_supportive + under_10k_neutral + under_10k_opposed)) under_10k_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(under_10k_opposed) / SUM(under_10k_supportive + under_10k_neutral + under_10k_opposed)) under_10k_opposed_perc\r\n`
        
    sql += `	, ROUND(100 * SUM(ten_to_24k_supportive) / SUM(ten_to_24k_supportive + ten_to_24k_neutral + ten_to_24k_opposed)) ten_to_24k_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(ten_to_24k_neutral) / SUM(ten_to_24k_supportive + ten_to_24k_neutral + ten_to_24k_opposed)) ten_to_24k_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(ten_to_24k_opposed) / SUM(ten_to_24k_supportive + ten_to_24k_neutral + ten_to_24k_opposed)) ten_to_24k_opposed_perc\r\n`
        
    sql += `	, ROUND(100 * SUM(twenty_five_to_49k_supportive) / SUM(twenty_five_to_49k_supportive + twenty_five_to_49k_neutral + twenty_five_to_49k_opposed)) twenty_five_to_49k_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(twenty_five_to_49k_neutral) / SUM(twenty_five_to_49k_supportive + twenty_five_to_49k_neutral + twenty_five_to_49k_opposed)) twenty_five_to_49k_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(twenty_five_to_49k_opposed) / SUM(twenty_five_to_49k_supportive + twenty_five_to_49k_neutral + twenty_five_to_49k_opposed)) twenty_five_to_49k_opposed_perc\r\n`
        
    sql += `	, ROUND(100 * SUM(fifty_to_74k_supportive) / SUM(fifty_to_74k_supportive + fifty_to_74k_neutral + fifty_to_74k_opposed)) fifty_to_74k_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(fifty_to_74k_neutral) / SUM(fifty_to_74k_supportive + fifty_to_74k_neutral + fifty_to_74k_opposed)) fifty_to_74k_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(fifty_to_74k_opposed) / SUM(fifty_to_74k_supportive + fifty_to_74k_neutral + fifty_to_74k_opposed)) fifty_to_74k_opposed_perc\r\n`
        
    sql += `	, ROUND(100 * SUM(seventy_five_and_up_supportive) / SUM(seventy_five_and_up_supportive + seventy_five_and_up_neutral + seventy_five_and_up_opposed)) seventy_five_and_up_supportive_perc\r\n`
    sql += `	, ROUND(100 * SUM(seventy_five_and_up_neutral) / SUM(seventy_five_and_up_supportive + seventy_five_and_up_neutral + seventy_five_and_up_opposed)) seventy_five_and_up_neutral_perc\r\n`
    sql += `	, ROUND(100 * SUM(seventy_five_and_up_opposed) / SUM(seventy_five_and_up_supportive + seventy_five_and_up_neutral + seventy_five_and_up_opposed)) seventy_five_and_up_opposed_perc\r\n`
        
    sql += `FROM (\r\n`
    sql += `	SELECT act.supportive, act.neutral, act.opposed\r\n`
    sql += `	, act.supportive * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_supportive\r\n`
    sql += `	, act.neutral * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_neutral\r\n`
    sql += `	, act.opposed * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_opposed\r\n`

    sql += `	, act.supportive * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_supportive\r\n`
    sql += `	, act.neutral * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_neutral\r\n`
    sql += `	, act.opposed * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_opposed\r\n`

    sql += `	, act.supportive * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_supportive\r\n`
    sql += `	, act.neutral * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_neutral\r\n`
    sql += `	, act.opposed * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_opposed\r\n`

    sql += `	, act.supportive * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_supportive\r\n`
    sql += `	, act.neutral * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_neutral\r\n`
    sql += `	, act.opposed * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_opposed\r\n`

    sql += `	, act.supportive * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_supportive\r\n`
    sql += `	, act.neutral * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_neutral\r\n`
    sql += `	, act.opposed * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_opposed\r\n`

    sql += `	FROM (\r\n`
    sql += `		SELECT CAST(cal.detail ->> 'personId' AS BIGINT) person_id\r\n`
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
    sql += `	INNER JOIN census.acs_income_2017 income ON income.block_group_id = blk.block_group_id\r\n`
    sql += `	) metrics\r\n`
    sql += `;`
    
    return sql
}

