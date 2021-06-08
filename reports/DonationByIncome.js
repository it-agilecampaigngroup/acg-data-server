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
                , yesPercent: parseInt(row.yes_perc)
                , noPercent: parseInt(row.no_perc)
                
                , under10KAmount: parseFloat(row.under_10k_amount)
                , under10KAverageAmount: parseFloat(row.under_10k_average)
                , under10KYesCount: parseFloat(row.under_10k_yes_count)
                , under10KNoCount: parseFloat(row.under_10k_no_count)
                , under10KYesPercent: parseInt(row.under_10k_yes_perc)
                , under10KNoPercent: parseInt(row.under_10k_no_perc)
                
                , tenTo24KAmount: parseFloat(row.ten_to_24k_amount)
                , tenTo24KAverageAmount: parseFloat(row.ten_to_24k_average)
                , tenTo24KYesCount: parseFloat(row.ten_to_24k_yes_count)
                , tenTo24KNoCount: parseFloat(row.ten_to_24k_no_count)
                , tenTo24KYesPercent: parseInt(row.ten_to_24k_yes_perc)
                , tenTo24KNoPercent: parseInt(row.ten_to_24k_no_perc)
                
                , twentyFiveTo49KAmount: parseFloat(row.twenty_five_to_49k_amount)
                , twentyFiveTo49KAverageAmount: parseFloat(row.twenty_five_to_49k_average)
                , twentyFiveTo49KYesCount: parseFloat(row.twenty_five_to_49k_yes_count)
                , twentyFiveTo49KNoCount: parseFloat(row.twenty_five_to_49k_no_count)
                , twentyFiveTo49KYesPercent: parseInt(row.twenty_five_to_49k_yes_perc)
                , twentyFiveTo49KNoPercent: parseInt(row.twenty_five_to_49k_no_perc)
                
                , fiftyTo74KAmount: parseFloat(row.fifty_to_74k_amount)
                , fiftyTo74KAverageAmount: parseFloat(row.fifty_to_74k_average)
                , fiftyTo74KYesCount: parseFloat(row.fifty_to_74k_yes_count)
                , fiftyTo74KNoCount: parseFloat(row.fifty_to_74k_no_count)
                , fiftyTo74KYesPercent: parseInt(row.fifty_to_74k_yes_perc)
                , fiftyTo74KNoPercent: parseInt(row.fifty_to_74k_no_perc)
                
                , seventyFiveKAndUpAmount: parseFloat(row.seventy_five_and_up_amount)
                , seventyFiveKAndUpAverageAmount: parseFloat(row.seventy_five_and_up_average)
                , seventyFiveKAndUpYesCount: parseFloat(row.seventy_five_and_up_yes_count)
                , seventyFiveKAndUpNoCount: parseFloat(row.seventy_five_and_up_no_count)
                , seventyFiveKAndUpYesPercent: parseInt(row.seventy_five_and_up_yes_perc)
                , seventyFiveKAndUpNoPercent: parseInt(row.seventy_five_and_up_no_perc)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationByIncome.js', 'DonationByIncome', `Database error generating Donation By Income report: ${sql}`, e))
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
    
    sql += `, CASE WHEN SUM(d.yes_count) + SUM(d.no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.yes_count) AS decimal)) / ( SUM(d.yes_count) + SUM(d.no_count) ))\r\n`
    sql += `  ELSE 0 END yes_perc\r\n`
    
    sql += `, CASE WHEN SUM(d.yes_count) + SUM(d.no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.no_count) AS decimal)) / ( SUM(d.yes_count) + SUM(d.no_count) ))\r\n`
    sql += `  ELSE 0 END no_perc\r\n`
    
    sql += `-- Under 10k\r\n`
    sql += `, ROUND(SUM(d.under_10k_amount), 2) under_10k_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.under_10k_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.under_10k_amount)/ROUND(SUM(d.under_10k_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.under_10k_amount), 2)\r\n`
    sql += `    END under_10k_average\r\n`
    sql += `, ROUND(SUM(d.under_10k_yes_count), 2) under_10k_yes_count\r\n`
    sql += `, ROUND(SUM(d.under_10k_no_count), 2) under_10k_no_count\r\n`

    sql += `, CASE WHEN SUM(d.under_10k_yes_count) + SUM(d.under_10k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.under_10k_yes_count) AS decimal)) / ( SUM(d.under_10k_yes_count) + SUM(d.under_10k_no_count) ))\r\n`
    sql += `  ELSE 0 END under_10k_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.under_10k_yes_count) + SUM(d.under_10k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.under_10k_no_count) AS decimal)) / ( SUM(d.under_10k_yes_count) + SUM(d.under_10k_no_count) ))\r\n`
    sql += `  ELSE 0 END under_10k_no_perc\r\n`
    
    sql += `-- 10K to 24K\r\n`
    sql += `, ROUND(SUM(d.ten_to_24k_amount), 2) ten_to_24k_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.ten_to_24k_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.ten_to_24k_amount)/ROUND(SUM(d.ten_to_24k_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.ten_to_24k_amount), 2)\r\n`
    sql += `    END ten_to_24k_average\r\n`
    sql += `, ROUND(SUM(d.ten_to_24k_yes_count), 2) ten_to_24k_yes_count\r\n`
    sql += `, ROUND(SUM(d.ten_to_24k_no_count), 2) ten_to_24k_no_count\r\n`
    
    sql += `, CASE WHEN SUM(d.ten_to_24k_yes_count) + SUM(d.ten_to_24k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ten_to_24k_yes_count) AS decimal)) / ( SUM(d.ten_to_24k_yes_count) + SUM(d.ten_to_24k_no_count) ))\r\n`
    sql += `  ELSE 0 END ten_to_24k_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.ten_to_24k_yes_count) + SUM(d.ten_to_24k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ten_to_24k_no_count) AS decimal)) / ( SUM(d.ten_to_24k_yes_count) + SUM(d.ten_to_24k_no_count) ))\r\n`
    sql += `  ELSE 0 END ten_to_24k_no_perc\r\n`
    
    sql += `-- 25K to 49K\r\n`
    sql += `, ROUND(SUM(d.twenty_five_to_49k_amount), 2) twenty_five_to_49k_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.twenty_five_to_49k_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.twenty_five_to_49k_amount)/ROUND(SUM(d.twenty_five_to_49k_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.twenty_five_to_49k_amount), 2)\r\n`
    sql += `    END twenty_five_to_49k_average\r\n`
    sql += `, ROUND(SUM(d.twenty_five_to_49k_yes_count), 2) twenty_five_to_49k_yes_count\r\n`
    sql += `, ROUND(SUM(d.twenty_five_to_49k_no_count), 2) twenty_five_to_49k_no_count\r\n`
    
    sql += `, CASE WHEN SUM(d.twenty_five_to_49k_yes_count) + SUM(d.twenty_five_to_49k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.twenty_five_to_49k_yes_count) AS decimal)) / ( SUM(d.twenty_five_to_49k_yes_count) + SUM(d.twenty_five_to_49k_no_count) ))\r\n`
    sql += `  ELSE 0 END twenty_five_to_49k_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.twenty_five_to_49k_yes_count) + SUM(d.twenty_five_to_49k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.twenty_five_to_49k_no_count) AS decimal)) / ( SUM(d.twenty_five_to_49k_yes_count) + SUM(d.twenty_five_to_49k_no_count) ))\r\n`
    sql += `  ELSE 0 END twenty_five_to_49k_no_perc\r\n`
    
    sql += `-- 50K to 74K\r\n`
    sql += `, ROUND(SUM(d.fifty_to_74k_amount), 2) fifty_to_74k_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.fifty_to_74k_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.fifty_to_74k_amount)/ROUND(SUM(d.fifty_to_74k_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.fifty_to_74k_amount), 2)\r\n`
    sql += `    END fifty_to_74k_average\r\n`
    sql += `, ROUND(SUM(d.fifty_to_74k_yes_count), 2) fifty_to_74k_yes_count\r\n`
    sql += `, ROUND(SUM(d.fifty_to_74k_no_count), 2) fifty_to_74k_no_count\r\n`

    sql += `, CASE WHEN SUM(d.fifty_to_74k_yes_count) + SUM(d.fifty_to_74k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.fifty_to_74k_yes_count) AS decimal)) / ( SUM(d.fifty_to_74k_yes_count) + SUM(d.fifty_to_74k_no_count) ))\r\n`
    sql += `  ELSE 0 END fifty_to_74k_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.fifty_to_74k_yes_count) + SUM(d.fifty_to_74k_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.fifty_to_74k_no_count) AS decimal)) / ( SUM(d.fifty_to_74k_yes_count) + SUM(d.fifty_to_74k_no_count) ))\r\n`
    sql += `  ELSE 0 END fifty_to_74k_no_perc\r\n`
    
    sql += `-- 75K and up\r\n`
    sql += `, ROUND(SUM(d.seventy_five_and_up_amount), 2) seventy_five_and_up_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.seventy_five_and_up_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.seventy_five_and_up_amount)/ROUND(SUM(d.seventy_five_and_up_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.seventy_five_and_up_amount), 2)\r\n`
    sql += `    END seventy_five_and_up_average\r\n`
    sql += `, ROUND(SUM(d.seventy_five_and_up_yes_count), 2) seventy_five_and_up_yes_count\r\n`
    sql += `, ROUND(SUM(d.seventy_five_and_up_no_count), 2) seventy_five_and_up_no_count\r\n`

    sql += `, CASE WHEN SUM(d.seventy_five_and_up_yes_count) + SUM(d.seventy_five_and_up_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.seventy_five_and_up_yes_count) AS decimal)) / ( SUM(d.seventy_five_and_up_yes_count) + SUM(d.seventy_five_and_up_no_count) ))\r\n`
    sql += `  ELSE 0 END seventy_five_and_up_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.seventy_five_and_up_yes_count) + SUM(d.seventy_five_and_up_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.seventy_five_and_up_no_count) AS decimal)) / ( SUM(d.seventy_five_and_up_yes_count) + SUM(d.seventy_five_and_up_no_count) ))\r\n`
    sql += `  ELSE 0 END seventy_five_and_up_no_perc\r\n`
    
    sql += `FROM (\r\n`
    sql += `    SELECT donations.amount, donations.yes_count, donations.no_count, donations.response_count\r\n`
    sql += `    -- Under 10K\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.amount * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop ELSE 0 END under_10k_amount\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.yes_count * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop ELSE 0 END under_10k_yes_count\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.no_count * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop ELSE 0 END under_10k_no_count\r\n`

    sql += `-- 10K to 24K\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.amount * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ELSE 0 END ten_to_24k_amount\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.yes_count * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ELSE 0 END ten_to_24k_yes_count\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.no_count * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ELSE 0 END ten_to_24k_no_count\r\n`

    sql += `-- 25K to 49K\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.amount * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop ELSE 0 END twenty_five_to_49k_amount\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.yes_count * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop ELSE 0 END twenty_five_to_49k_yes_count\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.no_count * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop ELSE 0 END twenty_five_to_49k_no_count\r\n`

    sql += `-- 50K to 74K\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.amount * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop ELSE 0 END fifty_to_74k_amount\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.yes_count * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop ELSE 0 END fifty_to_74k_yes_count\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.no_count * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop ELSE 0 END fifty_to_74k_no_count\r\n`

    sql += `-- 75K and up\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.amount * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop ELSE 0 END seventy_five_and_up_amount\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.yes_count * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop ELSE 0 END seventy_five_and_up_yes_count\r\n`
    sql += `	, CASE WHEN income.est_income_total_pop > 0 THEN donations.no_count * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop ELSE 0 END seventy_five_and_up_no_count\r\n`

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
    sql += `	INNER JOIN census.acs_income_2017 income ON income.block_group_id = blk.block_group_id\r\n`
    sql += `) d\r\n`
    sql += `;`
    
    return sql
}

