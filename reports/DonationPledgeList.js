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
                        , firstName: row.first_name
                        , lastName: row.last_name
                        , middleName: row.middle_name
                        , nameSuffix: row.suffix
                        , phoneNumber: row.phone_number
                        , actor: actor.displayName
                        , donationAmount: parseFloat(row.donation_amount)
                        , numberDonationsReported: row.num_donations_reported
                        , isRecurring: row.is_recurring
                        , isMaximumDonation: row.is_maximum_allowed
                        , note: row.note
                    }
                )
            }
            return report
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'DonationPledgeList.js', 'DonationPledgeList', `Database error generating Donation Pledge List report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, actorId, dateStart, dateEnd) {

    // Increment dateEnd to simplify the query
    var nextDate = new Date(dateEnd)
    nextDate.setDate(nextDate.getDate() + 1);
    dateEnd = new Date(nextDate).toLocaleDateString()

    // Build the SQL
    var sql = `SELECT vcal.date_created, p.first_name, p.last_name, p.middle_name, p.suffix\r\n`
    sql += `, vcal.detail->>'phone' phone_number\r\n`
    sql += `, vcal.actor_id\r\n`
    sql += `, CAST(vcal.detail->>'amount' as decimal) donation_amount\r\n`
    sql += `, COALESCE((SELECT ds.num_donations FROM base.donation_summary ds WHERE ds.person_id = p.person_id), 0) num_donations_reported\r\n`
    sql += `, CAST(COALESCE(vcal.detail->>'recurring', 'false') AS boolean) is_recurring\r\n`
    sql += `, CASE WHEN CAST(vcal.detail->>'amount' as decimal) >= 2900 THEN true ELSE false END is_maximum_allowed\r\n`
    sql += `, COALESCE(vcal.detail->>'note', '') note\r\n`
    sql += `FROM base.v_contact_action_log vcal\r\n`
    sql += `INNER JOIN base.person p ON p.person_id = CAST(vcal.detail->>'personId' AS bigint)\r\n`
    sql += `WHERE vcal.contact_reason ILIKE 'Donation request'\r\n`
    sql += `AND vcal.contact_result ILIKE 'Positive response'\r\n`
    sql += `AND vcal.campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) {
        sql += `AND vcal.actor_id = ${actorId}\r\n`
    }
    sql += `AND vcal.date_created >= '${dateStart}'\r\n`
    sql += `AND vcal.date_created < '${dateEnd}'\r\n`
    sql += `ORDER BY vcal.date_created DESC\r\n`

    return sql
}

