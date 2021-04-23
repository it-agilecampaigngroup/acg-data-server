'use strict'

const db = require('../db/db')
const campaign = require('../modules/Campaign')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

module.exports = async function generate(actorId) {

    // Build the SQL
    var sql = await buildSQL(actorId)
    
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
                let actor = actors.get(row.callback_actor_id)
                
                //... or retrieve it from the security server
                if( actor === undefined ) {
                    // Actor is not in cache. Retrieve and cache it.
                    await campaign.getActor(row.callback_actor_id, (the_actor) => {
                        actor = the_actor
                        actors.set(row.callback_actor_id, actor)
                    })
                }

                // Add the record to the report
                report.push(
                    {
                        callbackDate: new Date(row.callback_timestamp).toLocaleDateString()
                        , callbackTime: new Date(row.callback_timestamp).toLocaleTimeString()
                        , callbackFirstName: row.first_name
                        , callbackMiddleName: row.middle_name
                        , callbackLastName: row.last_name
                        , callbackSuffix: row.suffix
                        , callbackPhone: row.phone_number
                        , callbackPhoneType: row.phone_type
                        , callbackStreet: row.number + " " + row.street_name
                        , callbackUnit: row.unit_number + " " + row.unit_type
                        , scheduledDate: new Date(row.contact_date).toLocaleDateString()
                        , contactReason: row.contact_reason
                        , contactMethod: row.contact_method
                        , callbackActor: actor.displayName
                        , callbackActorId: row.callback_actor_id
                    }
                )
            }
            return report
        }
        return undefined
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'ActorCallbackList.js', 'ActorCallbackList', `Database error generating Actor Callback List report: ${sql}`, e))
        throw new Error(e.message)
    }
}

async function buildSQL(actorId) {

    // If the specified actor is a manager we must return their
    // callbacks and all of the callbacks for the actors they
    // manage. 
    
    // Build a list of the actors to include:
    var actorIdList = actorId // Always include the specified actor
    
    // Get the actors the specified actor manages
    var managedActors = []
    await campaign.getManagedActors(actorId, (the_actors) => {
        managedActors = the_actors
    })

    // Add the manage actors to the list
    if( managedActors.length > 0 ) {
        for( var idx = 0; idx < managedActors.length; idx++) {
            actorIdList += "," + managedActors[idx].actorId
        }
    }

    // Build and return the query
    var sql = `SELECT stat.callback_timestamp\r\n`
    sql += `, p.first_name, p.middle_name, p.last_name, p.suffix\r\n`
    sql += `, phon.phone_number, pt.description phone_type\r\n`
    sql += `, addr."number", addr.street_name, addr.unit_number, addr.unit_type\r\n`
    sql += `, MAX(clog.date_created) contact_date\r\n`
    sql += `, reas.description contact_reason\r\n`
    sql += `, meth.description contact_method\r\n`
    sql += `, stat.callback_actor_id\r\n`
    sql += `FROM base.contact_status stat\r\n`
    sql += `INNER JOIN base.person p ON p.person_id = stat.person_id\r\n`
    sql += `LEFT OUTER JOIN base.person_phone phon ON phon.person_id = p.person_id AND phon.is_primary = true\r\n`
    sql += `INNER JOIN base.phone_type pt ON pt.type_id = phon.phone_type_id\r\n`
    sql += `LEFT OUTER JOIN base.person_address pa ON pa.person_id = p.person_id AND pa.is_primary = true\r\n`
    sql += `INNER JOIN base.address addr ON addr.address_id = pa.address_id\r\n`
    sql += `INNER JOIN base.contact_action_log clog ON CAST(clog.detail ->> 'personId' AS BIGINT) = p.person_id\r\n`
    sql += `INNER JOIN base.contact_action act ON act.action_id = clog.contact_action_id\r\n`
    sql += `INNER JOIN base.contact_reason reas ON reas.reason_id = clog.contact_reason_id\r\n`
    sql += `INNER JOIN base.contact_method meth ON meth.method_id = clog.contact_method_id\r\n`
    sql += `INNER JOIN base.contact_result res ON res.result_id = clog.contact_result_id\r\n`
    sql += `WHERE stat.callback_actor_id IN (${actorIdList})\r\n`
    sql += `AND stat.callback_timestamp IS NOT NULL\r\n`
    sql += `AND clog.detail ->> 'personId' <> '{}'\r\n`
    sql += `AND UPPER(res.description) = 'CALLBACK SCHEDULED'\r\n`
    sql += `GROUP BY stat.callback_timestamp\r\n`
    sql += `, p.first_name, p.middle_name, p.last_name, p.suffix\r\n`
    sql += `, phon.phone_number, pt.description\r\n`
    sql += `, addr."number", addr.street_name, addr.unit_number, addr.unit_type\r\n`
    sql += `, reas.description\r\n`
    sql += `, meth.description\r\n`
    sql += `, stat.callback_actor_id\r\n`
    sql += `ORDER BY stat.callback_timestamp;\r\n`
   
    return sql
}

