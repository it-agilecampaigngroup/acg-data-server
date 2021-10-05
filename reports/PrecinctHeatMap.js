'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(election, districtList) {

    // Build the SQL
    var sql = buildSQL(election, districtList)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the report
    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount >= 0) {
            return dbres.rows[0]        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'PrecinctHeatMap.js', 'PrecinctHeatMap', `Database error generating Precinct Heat Map report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(election, districtList) {

    // Build the SQL
    var sql = `SELECT jsonb_build_object(\r\n`
    sql += `    'type',     'FeatureCollection'\r\n`
    sql += `    , 'name', 'Precinct Heat Map'\r\n`
    sql += `    , 'crs', jsonb_build_object(\r\n`
    sql += `        'type', 'name'\r\n`
    sql += `        , 'properties', json_build_object(\r\n`
    sql += `            'name', 'urn:ogc:def:crs:EPSG::4269'\r\n`
    sql += `           )\r\n`
    sql += `        )\r\n`
    sql += `    , 'features', jsonb_agg(precinct)\r\n`
    sql += `)\r\n`
    sql += `FROM (\r\n`
    sql += `    SELECT jsonb_build_object(\r\n`
    sql += `    'type',       'Feature',\r\n`
    sql += `    'name',         name,\r\n`
    sql += `    'geometry',   ST_AsGeoJSON(geom)::jsonb,\r\n`
    sql += `    'properties', to_jsonb(row) - 'gid' - 'geom'\r\n`
    sql += `    ) AS precinct\r\n`
    sql += `    FROM (\r\n`
    
    sql += `WITH voting_results (precinct_id, dem_vote_count, rep_vote_count, other_vote_count) AS (\r\n`
    sql += `    SELECT p.precinct_id, c.dem_vote_count, c.rep_vote_count, c.other_vote_count\r\n`
    sql += `        , c.dem_vote_count + c.rep_vote_count + c.other_vote_count total_votes\r\n`
    sql += `    FROM election.precinct p\r\n`
    sql += `    INNER JOIN election.ballot b ON b.precinct_id = p.precinct_id\r\n`
    sql += `    INNER JOIN election.contest c ON c.ballot_id = b.ballot_id\r\n`
    sql += `    INNER JOIN election.office o ON o.office_id = c.office_id\r\n`
    sql += `    INNER JOIN election.election e ON e.election_id = b.election_id\r\n`
    sql += `    INNER JOIN election.election_type et ON et.type_id = e.election_type_id\r\n`
    
    sql += `    WHERE e.election_date = '${election.date}'\r\n`
    sql += `    AND et.description ILIKE '${election.type}'\r\n`
    sql += `    AND o.description ILIKE '${election.office}'\r\n`
    
    // Build the district filter
    sql += `    AND (\r\n`
    for (let idx in districtList) {
        if(idx > 0) {
            sql += `        OR\r\n`
        }
        sql += `        ST_CONTAINS(\r\n`
        sql += `        ( SELECT d.geom\r\n`
        sql += `          FROM election.district d\r\n`
        sql += `          INNER JOIN base.admin_area aa ON aa.admin_area_id = d.admin_area_id\r\n`
        sql += `          INNER JOIN election.district_type dt ON dt.type_id = d.district_type_id\r\n`
        sql += `          WHERE aa.name ILIKE '${districtList[idx].adminArea}'\r\n`
        sql += `          AND dt.description ILIKE '${districtList[idx].districtType}'\r\n`
        sql += `          AND d.sequential_identifier = '${districtList[idx].identifier}' )\r\n`
        sql += `        , ST_CENTROID(p.geom))\r\n`
    }
    sql += `    )\r\n` // End if district filter
    sql += `)\r\n` // End of With voting results
    
    sql += `, donations (precinct_id, total_donation_amount, avg_donation_amount, num_donations) AS (\r\n`
    sql += `    SELECT (SELECT precinct_id\r\n`
    sql += `        FROM election.precinct p\r\n`
    sql += `        WHERE p.admin_area_id = addr.admin_area_id\r\n`
    sql += `        AND p.valid_from_date <= '${election.date}'\r\n`
    sql += `        AND '${election.date}' < COALESCE(p.valid_to_date, '12/31/2199')\r\n`
    sql += `        AND ST_CONTAINS(p.geom, ST_SetSRID(ST_MakePoint(addr.lng, addr.lat), 4326))\r\n`
    sql += `        ) precinct_id\r\n`
    sql += `    , SUM(CAST(vcal.detail->>'amount' AS decimal)) total_donation_amount\r\n`
    sql += `    , AVG(CAST(vcal.detail->>'amount' AS decimal)) avg_donation_amount\r\n`
    sql += `    , COUNT(CAST(vcal.detail->>'amount' AS decimal)) num_donations\r\n`
    sql += `    FROM base.v_contact_action_log vcal\r\n`
    sql += `    INNER JOIN base.person_address pa ON pa.person_id = CAST(vcal.detail->>'personId' AS INTEGER) AND pa.is_primary = true\r\n`
    sql += `    INNER JOIN base.address addr ON addr.address_id = pa.address_id\r\n`
    sql += `    WHERE vcal.contact_result ILIKE 'Positive response'\r\n`
    sql += `    GROUP BY precinct_id, addr.admin_area_id, addr.lng, addr.lat\r\n`
    sql += `)\r\n` // End of With donations

    // Build Population by Race WITH
    sql += `, pop_by_race (precinct_id, total_pop, hispanic_pop, asian_pop, afam_black_pop, white_pop) AS (\r\n`
    sql += `    SELECT p.precinct_id\r\n`
    sql += `    , SUM(COALESCE(c.total_pop, 0)) total_pop\r\n`
    sql += `    , SUM(c.hispanic_pop) hispanic_pop\r\n`
    sql += `    , SUM(c.asian_pop) asian_pop\r\n`
    sql += `    , SUM(c.afam_black_pop) black_pop\r\n`
    sql += `    , SUM(c.white_pop) white_pop\r\n`
    sql += `    FROM election.precinct p\r\n`
    sql += `    LEFT OUTER JOIN census.census_block blk ON blk.precinct_id = p.precinct_id\r\n`
    sql += `    LEFT OUTER JOIN census.census_2010 c ON c.census_block_id = blk.block_id\r\n`
    sql += `    WHERE p.valid_to_date IS NULL\r\n`
    sql += `    AND ST_CONTAINS(\r\n`
    sql += `        ( SELECT d.geom\r\n`
    sql += `            FROM election.district d\r\n`
    sql += `            INNER JOIN base.admin_area aa ON aa.admin_area_id = d.admin_area_id\r\n`
    sql += `            INNER JOIN election.district_type dt ON dt.type_id = d.district_type_id\r\n`
    sql += `            WHERE aa.name ILIKE 'Ohio'\r\n`
    sql += `            AND dt.description ILIKE 'Congressional District'\r\n`
    sql += `            AND d.sequential_identifier = '05' )\r\n`
    sql += `        , ST_CENTROID(p.geom))\r\n`
    sql += `    GROUP BY p.precinct_id\r\n`
    sql += `)\r\n` // End of Population by Race WITH
    
    // Build demographics WITH for values recorded at the block *group* level
    sql += `, demographics (precinct_id, edu_total_pop, high_school_and_under_perc, some_college_perc, ba_perc, ma_and_up_pop\r\n`
    sql += `, income_total_pop, under_10k_perc) AS (\r\n`
        sql += `    SELECT p.precinct_id\r\n`
        sql += `    -- Education\r\n`
        sql += `    , edu.edu_total_pop\r\n`
        sql += `    -- High school and under\r\n`
        sql += `    , CASE WHEN edu.edu_total_pop > 0 THEN\r\n`
        sql += `        CAST (edu.male_no_schooling_pop\r\n`
        sql += `        + edu.male_nursery_to_4th_grade_pop\r\n`
        sql += `        + edu.male_5th_and_6th_pop\r\n`
        sql += `        + edu.male_7th_and_8th_pop\r\n`
        sql += `        + edu.male_9th_pop\r\n`
        sql += `        + edu.male_10th_pop\r\n`
        sql += `        + edu.male_11th_pop\r\n`
        sql += `        + edu.male_12th_no_diploma_pop\r\n`
        sql += `        + edu.male_hs_grad_pop\r\n`
        sql += `        + edu.female_no_schooling_pop\r\n`
        sql += `        + edu.female_nursery_to_4th_grade_pop\r\n`
        sql += `        + edu.female_5th_and_6th_pop\r\n`
        sql += `        + edu.female_7th_and_8th_pop\r\n`
        sql += `        + edu.female_9th_pop\r\n`
        sql += `        + edu.female_10th_pop\r\n`
        sql += `        + edu.female_11th_pop\r\n`
        sql += `        + edu.female_12th_no_diploma_pop\r\n`
        sql += `        + edu.female_hs_grad_pop\r\n`
        sql += `        AS DECIMAL)\r\n`
        sql += `        / edu.edu_total_pop \r\n`
        sql += `        ELSE 0\r\n`
        sql += `        END high_school_and_under_perc\r\n`

        sql += `    , CASE WHEN edu.edu_total_pop > 0 THEN\r\n`
        sql += `        CAST(\r\n`
        sql += `        edu.male_less_than_1_year_college_pop\r\n`
        sql += `        + edu.male_some_college_no_degree_pop\r\n`
        sql += `        + edu.male_associates_degree_pop\r\n`
        sql += `        + edu.female_less_than_1_year_college_pop\r\n`
        sql += `        + edu.female_some_college_no_degree_pop\r\n`
        sql += `        + edu.female_associates_degree_pop\r\n`
        sql += `        AS DECIMAL)\r\n`
        sql += `        / edu.edu_total_pop\r\n`
        sql += `        ELSE 0\r\n`
        sql += `        END some_college_perc\r\n`

        sql += `    , CASE WHEN edu.edu_total_pop > 0 THEN\r\n`
        sql += `        CAST(\r\n`
        sql += `        edu.male_bachelors_degree_pop\r\n`
        sql += `        + edu.female_bachelors_degree_pop\r\n`
        sql += `        AS DECIMAL)\r\n`
        sql += `        / edu.edu_total_pop\r\n`
        sql += `        ELSE 0\r\n`
        sql += `        END ba_perc\r\n`

        sql += `    , CASE WHEN edu.edu_total_pop > 0 THEN\r\n`
        sql += `        CAST(\r\n`
        sql += `        edu.male_masters_degree_pop\r\n`
        sql += `        + edu.male_professional_degree_pop\r\n`
        sql += `        + edu.male_doctorate_degree_pop\r\n`
        sql += `        + edu.female_masters_degree_pop\r\n`
        sql += `        + edu.female_professional_degree_pop\r\n`
        sql += `        + edu.female_doctorate_degree_pop\r\n`
        sql += `        AS DECIMAL)\r\n`
        sql += `        / edu.edu_total_pop\r\n`
        sql += `        ELSE 0\r\n`
        sql += `        END ma_plus_perc\r\n`
        sql += `    -- End of education\r\n`
    
        sql += `    -- Income\r\n`
        sql += `    -- Under $10k\r\n`
        sql += `    , inc.income_total_pop\r\n`
        sql += `    , CASE WHEN inc.income_total_pop > 0 THEN\r\n`
        sql += `        CAST(inc.under_10k_pop AS DECIMAL) / inc.income_total_pop\r\n`
        sql += `    ELSE 0\r\n`
        sql += `    END under_10k_perc\r\n`

        sql += `    -- 10k to 24k\r\n`
        sql += `    , CASE WHEN inc.income_total_pop > 0 THEN\r\n`
        sql += `        CAST(inc.ten_to_14k_pop\r\n`
        sql += `            + inc.fifteen_to_19k_pop\r\n`
        sql += `            + inc.twenty_to_24k_pop\r\n`
        sql += `            AS DECIMAL)\r\n`
        sql += `        / inc.income_total_pop\r\n`
        sql += `        ELSE 0\r\n`
        sql += `        END ten_to_24k_perc\r\n`

        sql += `    -- 25k to 49k\r\n`
        sql += `    , CASE WHEN inc.income_total_pop > 0 THEN\r\n`
        sql += `        CAST(inc.twenty_five_to_29k_pop\r\n`
        sql += `            + inc.thirty_to_34k_pop\r\n`
        sql += `            + inc.thirty_five_to_39k_pop\r\n`
        sql += `            + inc.forty_to_44k_pop\r\n`
        sql += `            + inc.forty_five_to_49k_pop\r\n`
        sql += `            AS DECIMAL)\r\n`
        sql += `        / inc.income_total_pop\r\n`
        sql += `    ELSE 0\r\n`
        sql += `    END twenty_five_to_49k_perc\r\n`

        sql += `    -- 50k to 74k\r\n`
        sql += `    , CASE WHEN inc.income_total_pop > 0 THEN\r\n`
        sql += `        CAST(inc.fifty_to_59k_pop\r\n`
        sql += `            + inc.sixty_to_74k_pop\r\n`
        sql += `            AS DECIMAL)\r\n`
        sql += `        / inc.income_total_pop\r\n`
        sql += `    ELSE 0\r\n`
        sql += `    END fifty_to_74k_perc\r\n`

        sql += `    -- 75k and up\r\n`
        sql += `    , CASE WHEN inc.income_total_pop > 0 THEN\r\n`
        sql += `        CAST(inc.seventy_five_to_99k_pop\r\n`
        sql += `            + inc.one_hundred_to_124k_pop\r\n`
        sql += `            + inc.one_twenty_five_to_149k_pop\r\n`
        sql += `            + inc.one_fifty_to_199k_pop\r\n`
        sql += `            + inc.two_hundred_and_up_pop\r\n`
        sql += `            AS DECIMAL)\r\n`
        sql += `        / inc.income_total_pop\r\n`
        sql += `    ELSE 0\r\n`
        sql += `    END seventy_five_and_up_perc\r\n`
        sql += `    -- End of income\r\n`

        sql += `    FROM election.precinct p\r\n`
        sql += `    INNER JOIN election.precinct_education_pop edu ON edu.precinct_id = p.precinct_id\r\n`
        sql += `    INNER JOIN election.precinct_income_pop inc ON inc.precinct_id = p.precinct_id\r\n`
        sql += `    WHERE p.valid_to_date IS NULL\r\n`
        // Build the district filter
        sql += `    AND (\r\n`
        for (let idx in districtList) {
            if(idx > 0) {
                sql += `        OR\r\n`
            }
            sql += `        ST_CONTAINS(\r\n`
            sql += `        ( SELECT d.geom\r\n`
            sql += `          FROM election.district d\r\n`
            sql += `          INNER JOIN base.admin_area aa ON aa.admin_area_id = d.admin_area_id\r\n`
            sql += `          INNER JOIN election.district_type dt ON dt.type_id = d.district_type_id\r\n`
            sql += `          WHERE aa.name ILIKE '${districtList[idx].adminArea}'\r\n`
            sql += `          AND dt.description ILIKE '${districtList[idx].districtType}'\r\n`
            sql += `          AND d.sequential_identifier = '${districtList[idx].identifier}' )\r\n`
            sql += `        , ST_CENTROID(p.geom))\r\n`
        }
        sql += `    )\r\n` // End if district filter
        // sql += `    GROUP BY p.precinct_id\r\n` 
        sql += `    ) -- End of demographics WITH\r\n`

    // The main SELECT:
    sql += `SELECT p.precinct_id, p.name, vote_res.total_votes\r\n`
    sql += `, ROUND(CAST(vote_res.dem_vote_count AS decimal) * 100 / vote_res.total_votes) dem_vote_percent\r\n`
    sql += `, ROUND(CAST(vote_res.rep_vote_count AS decimal) * 100 / vote_res.total_votes) rep_vote_percent\r\n`
    sql += `, ROUND(CAST(vote_res.other_vote_count AS decimal) * 100 / vote_res.total_votes) other_vote_percent\r\n`
    sql += `, COALESCE(d.total_donation_amount, 0) total_donation_amount\r\n`
    sql += `, COALESCE(d.avg_donation_amount, 0) avg_donation_amount\r\n`
    sql += `, COALESCE(d.num_donations, 0) num_donations\r\n`

    // Demographics
    sql += `, CASE WHEN race.total_pop = 0 THEN 0 ELSE ROUND(CAST(race.hispanic_pop AS decimal) * 100 / race.total_pop) END hispanic_percent\r\n`
    sql += `, CASE WHEN race.total_pop = 0 THEN 0 ELSE ROUND(CAST(race.asian_pop AS decimal) * 100 / race.total_pop) END asian_percent\r\n`
    sql += `, CASE WHEN race.total_pop = 0 THEN 0 ELSE ROUND(CAST(race.afam_black_pop AS decimal) * 100 / race.total_pop) END afam_black_percent\r\n`
    sql += `, CASE WHEN race.total_pop = 0 THEN 0 ELSE ROUND(CAST(race.white_pop AS decimal) * 100 / race.total_pop) END white_percent\r\n`
    sql += `, demogs.edu_total_pop, demogs.high_school_and_under_perc, demogs.some_college_perc, demogs.ba_perc, demogs.ma_and_up_pop\r\n`
    sql += `, demogs.income_total_pop, demogs.under_10k_perc, demogs.ten_to_24k_perc, demogs.twenty_five_to_49k_perc, demogs.fifty_to_74k_perc, demogs.seventy_five_and_up_perc\r\n`
    
    sql += `, p.geom\r\n`
    sql += `FROM election.precinct p\r\n`
    sql += `INNER JOIN base.admin_area aa ON aa.admin_area_id = p.admin_area_id\r\n`
    sql += `LEFT OUTER JOIN voting_results vote_res ON vote_res.precinct_id = p.precinct_id\r\n`
    sql += `LEFT OUTER JOIN donations d ON d.precinct_id = p.precinct_id\r\n`
    sql += `LEFT OUTER JOIN pop_by_race race ON race.precinct_id = p.precinct_id\r\n`
    sql += `LEFT OUTER JOIN demographics demogs ON demogs.precinct_id = p.precinct_id\r\n`
    sql += `WHERE p.valid_to_date IS NULL -- Active precincts only\r\n`

    // Build the district filter
    sql += `    AND (\r\n`
    for (let idx in districtList) {
        if(idx > 0) {
            sql += `        OR\r\n`
        }
        sql += `        ST_CONTAINS(\r\n`
        sql += `        ( SELECT d.geom\r\n`
        sql += `          FROM election.district d\r\n`
        sql += `          INNER JOIN base.admin_area aa ON aa.admin_area_id = d.admin_area_id\r\n`
        sql += `          INNER JOIN election.district_type dt ON dt.type_id = d.district_type_id\r\n`
        sql += `          WHERE aa.name ILIKE '${districtList[idx].adminArea}'\r\n`
        sql += `          AND dt.description ILIKE '${districtList[idx].districtType}'\r\n`
        sql += `          AND d.sequential_identifier = '${districtList[idx].identifier}' )\r\n`
        sql += `        , ST_CENTROID(p.geom))\r\n`
    }
    sql += `    )\r\n` // End if district filter

    // sql += `AND ST_CONTAINS(\r\n`
    // sql += `    (SELECT d.geom\r\n`
    // sql += `    FROM election.district d\r\n`
    // sql += `    INNER JOIN base.admin_area aa ON aa.admin_area_id = d.admin_area_id\r\n`
    // sql += `    INNER JOIN election.district_type dt ON dt.type_id = d.district_type_id\r\n`
    // sql += `    WHERE aa.name ILIKE '${adminArea}'\r\n`
    // sql += `    AND dt.description ILIKE '${districtType}'\r\n`
    // sql += `    AND d.sequential_identifier = '${districtSequentialIdentifier}')\r\n`
    // sql += `    , ST_CENTROID(p.geom))\r\n`

    sql += `) row) features;`
    
    return sql
}

