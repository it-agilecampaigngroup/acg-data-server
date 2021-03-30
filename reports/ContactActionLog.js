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

                // Add the record to the report
                report.push(
                    {
                        date: new Date(row.date_created).toLocaleDateString(),
                        time: new Date(row.date_created).toTimeString(),
                        campaign: actor.campaign.name, 
                        actor: actor.displayName,
                        contactAction: row.contact_action,
                        contactReason: row.contact_reason,
                        contactMethod: row.contact_method,
                        contactResult: row.contact_result,
                        detail: row.detail
                    }
                )
            }
            return report
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'SupportResultSummary.js', 'SupportResultSummary', `Database error generating Support Result Summary report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(campaignId, actorId, dateStart, dateEnd) {

    // Increment dateEnd to simplify the query
    var nextDate = new Date(dateEnd)
    nextDate.setDate(nextDate.getDate() + 1);
    dateEnd = new Date(nextDate).toLocaleDateString()

    // Build the SQL
    var sql = "SELECT date_created, client_id, campaign_id, actor_id\r\n"
    sql += ", contact_action, contact_reason, contact_method\r\n"
    sql += ", contact_result, detail\r\n"
    sql += "FROM base.v_contact_action_log\r\n"
    sql += `WHERE campaign_id = ${campaignId}\r\n`
    if( actorId != 0 ) {
        sql += `AND actor_id = ${actorId}\r\n`
    }
    sql += `AND date_created >= '${dateStart}'\r\n`
    sql += `AND date_created < '${dateEnd}'`
    //sql += "\r\nLIMIT 10"
    sql += `;`
    
    return sql
}

