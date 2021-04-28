'use strict'

const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const campaign = require('../modules/Campaign')

module.exports = async function generate(campaignId, actorId, dateStart, dateEnd) {

    // Build the SQL
    var sql = buildSQL(campaignId, actorId, dateStart, dateEnd)
    
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount >= 0) {
            // Set up the report array
            const report = []            
            
            // Create a cash for the actors so we don't have
            // to visit the security server so many times.
            var HashMap = require('hashmap');
            const actors = new HashMap()

            // Process each record
            for( var idx = 0; idx < dbres.rowCount; idx++) {
                let row = dbres.rows[idx]
                
                // Get the actor from the cache...
                let actorId = row.actor_id
                let actor = actors.get(actorId)
                
                //... or retrieve it from the security server
                if( actor === undefined ) {
                    // Actor is not in cache. Retrieve and cache it.
                    await campaign.getActor(actorId, (the_actor) => {
                        actor = the_actor
                        actors.set(actorId, actor)
                    })
                }

                // Parse the callback timestamp
                var callbackDate = ''
                var callbackTime = ''
                if( row.callback_timestamp != '' ) {
                    callbackDate = new Date(row.callback_timestamp).toLocaleDateString()
                    if( callbackDate !== 'Invalid Date') {
                        callbackTime = new Date(row.callback_timestamp).toLocaleTimeString()
                    } else {
                        callbackDate = ''
                    }
                }
                
                // Add the record to the report
                report.push(
                    {
                        date: new Date(row.date_created).toLocaleDateString()
                        , time: new Date(row.date_created).toLocaleTimeString()
                        , actor: actor.displayName
                        , contactReason: row.contact_reason
                        , contactMethod: row.contact_method
                        , contactResult: row.contact_result
                        , voterId: row.voter_id
                        , firstName: row.first_name
                        , lastName: row.last_name
                        , street: row.street1
                        , city: row.city
                        , state: row.state
                        , zip: row.zip
                        , email: row.email
                        , phoneNumber: row.phoneNumber
                        , donationAmount: row.amount
                        , donationIsRecurring: row.recurring
                        , callbackDate: callbackDate
                        , callbackTime: callbackTime
                        , supportResult: row.support_result
                        , valueResult: row.value_result
                        , note: row.note
                    }
                )
            }
            return report
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'ContactResponseList.js', 'ContactResponseList', `Database error generating Contact Response List report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, actorId, dateStart, dateEnd) {

    // Increment dateEnd to simplify the query
    var nextDate = new Date(dateEnd)
    nextDate.setDate(nextDate.getDate() + 1);
    dateEnd = new Date(nextDate).toLocaleDateString()

    // Build the SQL
    var sql = `SELECT cal.date_created\r\n`
    sql += `, cal.contact_reason, cal.contact_method, cal.contact_result\r\n`
    sql += `, v.external_voter_key voter_id, p.first_name, p.last_name\r\n`
    sql += `, addr.street1, addr.city, addr.state, addr.zip\r\n`
    sql += `, NULL email\r\n`
    sql += `, CASE WHEN (cal.detail->'phone') IS NOT NULL THEN COALESCE(cal.detail->>'phone', '') ELSE '' END phone_number\r\n`
    sql += `, CASE WHEN (cal.detail->'amount') IS NOT NULL THEN COALESCE(cal.detail->>'amount', '') ELSE '' END amount\r\n`
    sql += `, CASE WHEN (cal.detail->'recurring') IS NOT NULL THEN COALESCE(cal.detail->>'recurring', 'false') ELSE 'false' END recurring\r\n`
    sql += `, CASE WHEN (cal.detail->'callbackTimestamp') IS NOT NULL THEN COALESCE(cal.detail->>'callbackTimestamp', '') ELSE '' END callback_timestamp\r\n`
    sql += `, CASE WHEN (cal.detail->'supportResult') IS NOT NULL THEN COALESCE(cal.detail->>'supportResult', '') ELSE '' END support_result\r\n`
    sql += `, CASE WHEN (cal.detail->'valueResult') IS NOT NULL THEN COALESCE(cal.detail->>'valueResult', '') ELSE '' END value_result\r\n`
    sql += `, CASE WHEN (cal.detail->'note') IS NOT NULL THEN cal.detail->>'note' ELSE '' END note\r\n`
    sql += `, cal.actor_id\r\n`
    sql += `FROM base.v_contact_action_log cal\r\n`
    sql += `INNER JOIN base.person p ON p.person_id = CAST(cal.detail ->> 'personId' AS bigint)\r\n`
    sql += `LEFT OUTER JOIN base.person_address pa ON pa.person_id = p.person_id AND pa.is_primary = true\r\n`
    sql += `LEFT OUTER JOIN base.v_us_address_detail addr ON addr.address_id = pa.address_id\r\n`
    sql += `LEFT OUTER JOIN election.voter v ON v.person_id = p.person_id\r\n`
    sql += `WHERE UPPER(cal.contact_action) NOT IN ('CONTACT LEASED', 'CONTACT REQUESTED', 'CONTACT REJECTED')\r\n`
    sql += `AND cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `AND cal.campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) {
        sql += `AND actor_id = ${actorId}\r\n`
    }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'\r\n`
    sql += `ORDER BY cal.date_created DESC;\r\n`
    
    return sql
}

