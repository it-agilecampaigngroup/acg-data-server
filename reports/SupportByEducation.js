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

                , hsAndUnderSupportPerc: parseFloat(row.hs_and_under_supportive_perc)
                , hsAndUnderNeutralPerc: parseFloat(row.hs_and_under_neutral_perc)
                , hsAndUnderOpposedPerc: parseFloat(row.hs_and_under_opposed_perc)

                , someCollegeSupportPerc: parseFloat(row.some_college_supportive_perc)
                , someCollegeNeutralPerc: parseFloat(row.some_college_neutral_perc)
                , someCollegeOpposedPerc: parseFloat(row.some_college_opposed_perc)

                , bachelorsSupportPerc: parseFloat(row.ba_supportive_perc)
                , bachelorsNeutralPerc: parseFloat(row.ba_neutral_perc)
                , bachelorsOpposedPerc: parseFloat(row.ba_opposed_perc)

                , mastersPlusSupportPerc: parseFloat(row.ma_plus_supportive_perc)
                , mastersPlusNeutralPerc: parseFloat(row.ma_plus_neutral_perc)
                , mastersPlusOpposedPerc: parseFloat(row.ma_plus_opposed_perc)

            }
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'SupportByEducation.js', 'SupportByEducation', `Database error generating Support By Education report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId) {

    var sql = `    SELECT SUM(supportive) total_supportive_count, SUM(neutral) total_neutral_count, SUM(opposed) total_opposed_count\r\n`
    sql += `    , ROUND(100 * SUM(supportive) / SUM(supportive + neutral + opposed)) total_supportive_perc\r\n`
    sql += `    , ROUND(100 * SUM(neutral) / SUM(supportive + neutral + opposed)) total_neutral_perc\r\n`
    sql += `    , ROUND(100 * SUM(opposed) / SUM(supportive + neutral + opposed)) total_opposed_perc\r\n`
        
    sql += `    , ROUND(100 * SUM(hs_and_under_supportive) / SUM(hs_and_under_supportive + hs_and_under_neutral + hs_and_under_opposed)) hs_and_under_supportive_perc\r\n`
    sql += `    , ROUND(100 * SUM(hs_and_under_neutral) / SUM(hs_and_under_supportive + hs_and_under_neutral + hs_and_under_opposed)) hs_and_under_neutral_perc\r\n`
    sql += `    , ROUND(100 * SUM(hs_and_under_opposed) / SUM(hs_and_under_supportive + hs_and_under_neutral + hs_and_under_opposed)) hs_and_under_opposed_perc\r\n`

    sql += `    , ROUND(100 * SUM(some_college_supportive) / SUM(some_college_supportive + some_college_neutral + some_college_opposed)) some_college_supportive_perc\r\n`
    sql += `    , ROUND(100 * SUM(some_college_neutral) / SUM(some_college_supportive + some_college_neutral + some_college_opposed)) some_college_neutral_perc\r\n`
    sql += `    , ROUND(100 * SUM(some_college_opposed) / SUM(some_college_supportive + some_college_neutral + some_college_opposed)) some_college_opposed_perc\r\n`

    sql += `    , ROUND(100 * SUM(ba_supportive) / SUM(ba_supportive + ba_neutral + ba_opposed)) ba_supportive_perc\r\n`
    sql += `    , ROUND(100 * SUM(ba_neutral) / SUM(ba_supportive + ba_neutral + ba_opposed)) ba_neutral_perc\r\n`
    sql += `    , ROUND(100 * SUM(ba_opposed) / SUM(ba_supportive + ba_neutral + ba_opposed)) ba_opposed_perc\r\n`

    sql += `    , ROUND(100 * SUM(ma_plus_supportive) / SUM(ma_plus_supportive + ma_plus_neutral + ma_plus_opposed)) ma_plus_supportive_perc\r\n`
    sql += `    , ROUND(100 * SUM(ma_plus_neutral) / SUM(ma_plus_supportive + ma_plus_neutral + ma_plus_opposed)) ma_plus_neutral_perc\r\n`
    sql += `    , ROUND(100 * SUM(ma_plus_opposed) / SUM(ma_plus_supportive + ma_plus_neutral + ma_plus_opposed)) ma_plus_opposed_perc\r\n`
    sql += `FROM (\r\n`
    sql += `    SELECT act.supportive, act.neutral, act.opposed\r\n`
    sql += `    , act.supportive * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop hs_and_under_supportive\r\n`

    sql += `    , act.neutral * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop hs_and_under_neutral\r\n`

    sql += `    , act.opposed * CAST(\r\n`
    sql += `        edu.est_male_no_schooling_pop + edu.est_male_nursery_to_4th_grade_pop + edu.est_male_5th_and_6th_pop + edu.est_male_7th_and_8th_pop + edu.est_male_9th_pop + edu.est_male_10th_pop + edu.est_male_11th_pop + edu.est_male_12th_no_diploma_pop + edu.est_male_hs_grad_pop\r\n`
    sql += `        + edu.est_female_no_schooling_pop + edu.est_female_nursery_to_4th_grade_pop + edu.est_female_5th_and_6th_pop + edu.est_female_7th_and_8th_pop + edu.est_female_9th_pop + edu.est_female_10th_pop + edu.est_female_11th_pop + edu.est_female_12th_no_diploma_pop + edu.est_female_hs_grad_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop hs_and_under_opposed\r\n`

    sql += `    , act.supportive * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop some_college_supportive\r\n`

    sql += `    , act.neutral * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop some_college_neutral\r\n`

    sql += `    , act.opposed * CAST(\r\n`
    sql += `        edu.est_male_less_than_1_year_college_pop + edu.est_male_some_college_no_degree_pop + edu.est_male_associates_degree_pop\r\n`
    sql += `        + edu.est_female_less_than_1_year_college_pop + edu.est_female_some_college_no_degree_pop + edu.est_female_associates_degree_pop as decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop some_college_opposed\r\n`

    sql += `    , act.supportive * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ba_supportive\r\n`

    sql += `    , act.neutral * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ba_neutral\r\n`

    sql += `    , act.opposed * CAST(\r\n`
    sql += `        edu.est_male_bachelors_degree_pop\r\n`
    sql += `        + edu.est_female_bachelors_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ba_opposed\r\n`

    sql += `    , act.supportive * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ma_plus_supportive\r\n`

    sql += `    , act.neutral * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ma_plus_neutral\r\n`

    sql += `    , act.opposed * CAST(\r\n`
    sql += `        edu.est_male_masters_degree_pop + edu.est_male_professional_degree_pop + edu.est_male_doctorate_degree_pop\r\n`
    sql += `        + edu.est_female_masters_degree_pop + edu.est_female_professional_degree_pop + edu.est_female_doctorate_degree_pop AS decimal)\r\n`
    sql += `        / edu.est_schooling_total_pop ma_plus_opposed\r\n`

    sql += `    FROM (\r\n`
    sql += `            SELECT DISTINCT CAST(cal.detail ->> 'personId' AS BIGINT) person_id\r\n`
    sql += `                    , CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) < 3 THEN 1 ELSE 0 END supportive\r\n`
    sql += `                    , CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) = 3 THEN 1 ELSE 0 END neutral\r\n`
    sql += `                    , CASE WHEN CAST(cal.detail ->> 'supportResult' AS INTEGER) > 3 THEN 1 ELSE 0 END opposed\r\n`
    sql += `                    FROM base.contact_action_log cal\r\n`
    sql += `                    INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id\r\n`
    sql += `                    WHERE cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `                    AND ca.description ILIKE 'Contact responded'\r\n`
    sql += `			        AND cal.campaign_id = ${campaignId}\r\n`
    sql += `            ) act\r\n`
    sql += `    INNER JOIN base.person_address pa ON pa.person_id = act.person_id AND pa.is_primary = true\r\n`
    sql += `    INNER JOIN base.address addr ON addr.address_id = pa.address_id\r\n`
    sql += `    INNER JOIN census.census_block blk ON blk.block_id = addr.census_block_id\r\n`
    sql += `    INNER JOIN census.acs_education_2017 edu ON edu.block_group_id = blk.block_group_id\r\n`
    sql += `    ) metrics\r\n`
    sql += `;`
    
    return sql
}

