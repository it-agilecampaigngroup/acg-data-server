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
            return dbres.rows[0]

            // Set up the report array
            const report = []            
            
            // Process each record
            for( var idx = 0; idx < dbres.rowCount; idx++) {
                let row = dbres.rows[idx]
                
                // Add the record to the report
                report.push(
                    {
                        precinctId: row.precinct_id
                        , precinctName: row.name
                        , totalVotes: row.total_votes
                        , demVotePercent: row.dem_vote_percent
                        , repVotePercent: row.rep_vote_percent
                        , otherVotePercent: row.other_vote_percent
                        , totalDonationAmount: row.total_donation_amount
                        , averageDonationAmount: row.avg_donation_amount
                        , numDonations: row.num_donations
                        , precinctGeometry: row.geom
                    }
                )
            }
            return report
        }
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
    sql += `),\r\n` // End of With voting results
    
    sql += `donations (precinct_id, total_donation_amount, avg_donation_amount, num_donations) AS (\r\n`
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

    sql += `SELECT p.precinct_id, p.name, vote_res.total_votes\r\n`
    sql += `, ROUND(CAST(vote_res.dem_vote_count AS decimal) * 100 / vote_res.total_votes) dem_vote_percent\r\n`
    sql += `, ROUND(CAST(vote_res.rep_vote_count AS decimal) * 100 / vote_res.total_votes) rep_vote_percent\r\n`
    sql += `, ROUND(CAST(vote_res.other_vote_count AS decimal) * 100 / vote_res.total_votes) other_vote_percent\r\n`
    sql += `, COALESCE(d.total_donation_amount, 0) total_donation_amount\r\n`
    sql += `, COALESCE(d.avg_donation_amount, 0) avg_donation_amount\r\n`
    sql += `, COALESCE(d.num_donations, 0) num_donations\r\n`
    sql += `, p.geom\r\n`
    sql += `FROM election.precinct p\r\n`
    sql += `INNER JOIN base.admin_area aa ON aa.admin_area_id = p.admin_area_id\r\n`
    sql += `LEFT OUTER JOIN voting_results vote_res ON vote_res.precinct_id = p.precinct_id\r\n`
    sql += `LEFT OUTER JOIN donations d ON d.precinct_id = p.precinct_id\r\n`
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

