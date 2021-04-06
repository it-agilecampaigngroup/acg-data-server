'use strict'

const db = require('../db/db')
const campaign = require('../modules/Campaign')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(personId) {

    // Build the SQL
    var sql = buildSQL(personId)
    
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
                let actor = actors.get(row.actor_id)
                
                //... or retrieve it from the security server
                if( actor === undefined ) {
                    // Actor is not in cache. Retrieve and cache it.
                    await campaign.getActor(row.actor_id, (the_actor) => {
                        actor = the_actor
                        actors.set(row.actor_id, actor)
                    })
                }

                // Add the record to the report
                report.push(
                    {
                        date: new Date(row.date_created).toLocaleDateString()
                        , time: new Date(row.date_created).toLocaleTimeString()
                        , contactAction: row.contact_action
                        , contactReason: row.contact_reason
                        , contactMethod: row.contact_method
                        , contactResult: row.contact_result
                        , detail: row.detail
                        , actor: actor.displayName
                    }
                )
            }
            return report
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'PersonContactHistory.js', 'PersonContactHistory', `Database error generating Person Contact History report: ${sql}`, e))
        throw new Error(e.message)
    }
}

function buildSQL(personId) {

    var sql = `SELECT date_created, contact_action, contact_reason, contact_method, contact_result, detail, actor_id\r\n`
    sql += `FROM base.v_contact_action_log cal\r\n`
    sql += `WHERE cal.detail IS NOT NULL\r\n`
    sql += `AND UPPER(cal.contact_action) IN ('CONTACT RESPONDED', 'CONTACT ATTEMPT FAILED', 'CONTACT REJECTED')\r\n`
    sql += `AND cal.detail ->> 'personId' <> '{}'\r\n`
    sql += `AND CAST(cal.detail ->> 'personId' AS BIGINT) = ${personId}\r\n`
    sql += `ORDER BY date_created DESC;`
    
    return sql
}

