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
                
                , hsAndUnderAmount: parseFloat(row.hs_and_under_amount)
                , hsAndUnderAverageAmount: parseFloat(row.hs_and_under_average)
                , hsAndUnderYesCount: parseFloat(row.hs_and_under_yes_count)
                , hsAndUnderNoCount: parseFloat(row.hs_and_under_no_count)
                , hsAndUnderYesPercent: parseInt(row.hs_and_under_yes_perc)
                , hsAndUnderNoPercent: parseInt(row.hs_and_under_no_perc)
                
                , someCollegeAmount: parseFloat(row.some_college_amount)
                , someCollegeAverageAmount: parseFloat(row.some_college_average)
                , someCollegeYesCount: parseFloat(row.some_college_yes_count)
                , someCollegeNoCount: parseFloat(row.some_college_no_count)
                , someCollegeYesPercent: parseInt(row.some_college_yes_perc)
                , someCollegeNoPercent: parseInt(row.some_college_no_perc)
                
                , bachelorsAmount: parseFloat(row.ba_amount)
                , bachelorsAverageAmount: parseFloat(row.ba_average)
                , bachelorsYesCount: parseFloat(row.ba_yes_count)
                , bachelorsNoCount: parseFloat(row.ba_no_count)
                , bachelorsYesPercent: parseInt(row.ba_yes_perc)
                , bachelorsNoPercent: parseInt(row.ba_no_perc)
                
                , mastersPlusAmount: parseFloat(row.ma_plus_amount)
                , mastersPlusAverageAmount: parseFloat(row.ma_plus_average)
                , mastersPlusYesCount: parseFloat(row.ma_plus_yes_count)
                , mastersPlusNoCount: parseFloat(row.ma_plus_no_count)
                , mastersPlusYesPercent: parseInt(row.ma_plus_yes_perc)
                , mastersPlusNoPercent: parseInt(row.ma_plus_no_perc)
            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationByEducation.js', 'DonationByEducation', `Database error generating Donation By Education report: ${sql}`, e))
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

    sql += `-- HS and under\r\n`
    sql += `, ROUND(SUM(d.hs_and_under_amount), 2) hs_and_under_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.hs_and_under_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.hs_and_under_amount)/ROUND(SUM(d.hs_and_under_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.hs_and_under_amount), 2)\r\n`
    sql += `    END hs_and_under_average\r\n`
    sql += `, ROUND(SUM(d.hs_and_under_yes_count), 2) hs_and_under_yes_count\r\n`
    sql += `, ROUND(SUM(d.hs_and_under_no_count), 2) hs_and_under_no_count\r\n`

    sql += `, CASE WHEN SUM(d.hs_and_under_yes_count) + SUM(d.hs_and_under_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.hs_and_under_yes_count) AS decimal)) / ( SUM(d.hs_and_under_yes_count) + SUM(d.hs_and_under_no_count) ))\r\n`
    sql += `  ELSE 0 END hs_and_under_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.hs_and_under_yes_count) + SUM(d.hs_and_under_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.hs_and_under_no_count) AS decimal)) / ( SUM(d.hs_and_under_yes_count) + SUM(d.hs_and_under_no_count) ))\r\n`
    sql += `  ELSE 0 END hs_and_under_no_perc\r\n`

    sql += `-- Some college\r\n`
    sql += `, ROUND(SUM(d.some_college_amount), 2) some_college_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.some_college_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.some_college_amount)/ROUND(SUM(d.some_college_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.some_college_amount), 2)\r\n`
    sql += `    END some_college_average\r\n`
    sql += `, ROUND(SUM(d.some_college_yes_count), 2) some_college_yes_count\r\n`
    sql += `, ROUND(SUM(d.some_college_no_count), 2) some_college_no_count\r\n`
    
    sql += `, CASE WHEN SUM(d.some_college_yes_count) + SUM(d.some_college_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.some_college_yes_count) AS decimal)) / ( SUM(d.some_college_yes_count) + SUM(d.some_college_no_count) ))\r\n`
    sql += `  ELSE 0 END some_college_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.some_college_yes_count) + SUM(d.some_college_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.some_college_no_count) AS decimal)) / ( SUM(d.some_college_yes_count) + SUM(d.some_college_no_count) ))\r\n`
    sql += `  ELSE 0 END some_college_no_perc\r\n`

    sql += `-- BA\r\n`
    sql += `, ROUND(SUM(d.ba_amount), 2) ba_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.ba_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.ba_amount)/ROUND(SUM(d.ba_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.ba_amount), 2)\r\n`
    sql += `    END ba_average\r\n`
    sql += `, ROUND(SUM(d.ba_yes_count), 2) ba_yes_count\r\n`
    sql += `, ROUND(SUM(d.ba_no_count), 2) ba_no_count\r\n`

    sql += `, CASE WHEN SUM(d.ba_yes_count) + SUM(d.ba_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ba_yes_count) AS decimal)) / ( SUM(d.ba_yes_count) + SUM(d.ba_no_count) ))\r\n`
    sql += `  ELSE 0 END ba_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.ba_yes_count) + SUM(d.ba_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ba_no_count) AS decimal)) / ( SUM(d.ba_yes_count) + SUM(d.ba_no_count) ))\r\n`
    sql += `  ELSE 0 END ba_no_perc\r\n`

    sql += `-- MA plus\r\n`
    sql += `, ROUND(SUM(d.ma_plus_amount), 2) ma_plus_amount\r\n`
    sql += `, CASE WHEN ROUND(SUM(d.ma_plus_yes_count)) > 0\r\n`
    sql += `    THEN ROUND(SUM(d.ma_plus_amount)/ROUND(SUM(d.ma_plus_yes_count)), 2)\r\n`
    sql += `    ELSE ROUND(SUM(d.ma_plus_amount), 2)\r\n`
    sql += `    END ma_plus_average\r\n`
    sql += `, ROUND(SUM(d.ma_plus_yes_count), 2) ma_plus_yes_count\r\n`
    sql += `, ROUND(SUM(d.ma_plus_no_count), 2) ma_plus_no_count\r\n`

    sql += `, CASE WHEN SUM(d.ma_plus_yes_count) + SUM(d.ma_plus_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ma_plus_yes_count) AS decimal)) / ( SUM(d.ma_plus_yes_count) + SUM(d.ma_plus_no_count) ))\r\n`
    sql += `  ELSE 0 END ma_plus_yes_perc\r\n`

    sql += `, CASE WHEN SUM(d.ma_plus_yes_count) + SUM(d.ma_plus_no_count) > 0\r\n`
    sql += `  THEN ROUND( (100 * CAST(SUM(d.ma_plus_no_count) AS decimal)) / ( SUM(d.ma_plus_yes_count) + SUM(d.ma_plus_no_count) ))\r\n`
    sql += `  ELSE 0 END ma_plus_no_perc\r\n`

    sql += `FROM (\r\n`
    sql += `    SELECT donations.amount, donations.yes_count, donations.no_count, donations.response_count\r\n`
    sql += `    -- High school and under\r\n`
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.amount * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END hs_and_under_amount\r\n`

    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.yes_count * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END hs_and_under_yes_count\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.no_count * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END hs_and_under_no_count\r\n`

    sql += `    -- Some college\r\n`
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.amount * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END some_college_amount\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.yes_count * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END some_college_yes_count\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.no_count * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END some_college_no_count\r\n`

    sql += `    -- BA\r\n`
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.amount * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ba_amount\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`    
    sql += `      THEN donations.yes_count * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ba_yes_count\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.no_count * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ba_no_count\r\n`

    sql += `    -- MA plus\r\n`
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.amount * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ma_plus_amount\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.yes_count * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ma_plus_yes_count\r\n`
    
    sql += `    , CASE WHEN edu.est_schooling_total_pop > 0\r\n`
    sql += `      THEN donations.no_count * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop\r\n`
    sql += `      ELSE 0 END ma_plus_no_count\r\n`

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
    sql += `    INNER JOIN census.acs_education_2017 edu ON edu.block_group_id = blk.block_group_id\r\n`
    sql += `) d\r\n`
    sql += `;`
    
    return sql
}

