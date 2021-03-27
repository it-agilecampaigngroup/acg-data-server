'use strict'

const db = require('./db')
const AppError = require('../modules/AppError')
const errlogger = require('./errorRecorder')
const msging = require('../messaging/kafka')

module.exports = {

    //====================================================================================
    // Record a contact action
    //
    // action: A fully populated ContactAction object
    //
    // Returns an object with the following properties:
    //      statusCode - an HTTP status code value
    //      err - an Error object, if an error occurs
    // 
    //===================================================================================
    async recordContactAction(action) {

        let insert = "INSERT INTO base.contact_action_log ("
        insert += "date_created, client_id, campaign_id, actor_id"
        insert += ", contact_action_id, contact_reason_id, contact_method_id, contact_result_id"
        insert += ", detail, modified_by, date_modified)\r\n"
        insert += "VALUES ("
        insert += `Now(), ${action.clientId}, ${action.campaignId}, ${action.actor.actorId}`
        insert += `, (SELECT action_id FROM base.contact_action WHERE description ILIKE '${action.action}')`
        insert += `, (SELECT reason_id FROM base.contact_reason WHERE description ILIKE '${action.reason}')`
        insert += `, (SELECT method_id FROM base.contact_method WHERE description ILIKE '${action.method}')`
        insert += `, (SELECT result_id FROM base.contact_result WHERE description ILIKE '${action.result}')`
        if( action.detail != "" ) {
            insert += `, '${JSON.stringify(action.detail)}'`
        } else {
            insert += ", NULL"
        }
        insert += `, '${action.actor.username}', Now()`
        insert += ");"
            
        // For debugging
        //console.log(insert)

        try {
            await db.query(insert)
        } catch (e) {
            errlogger.recordAppError(new AppError('data-server', 'actionRecorder', 'recordContactAction', 'Database error recording contact action', e))
            return { statusCode: 500, err: new Error(`Database error recording contact action:\r\n${e.message}`) }
        }

        try {
            // Send a ContactActions message
            let msg = {
                action: action.action,
                timestamp: action.timestamp,
                clientId: action.clientId,
                campaignId: action.campaignId,
                actor: {
                    actorId: action.actor.actorId,
                    firstName: action.actor.firstName,
                    lastName: action.actor.lastName,
                    middleName: action.actor.middleName,
                    nameSuffix: action.actor.nameSuffix,
                    namePrefix: action.actor.namePrefix
                },
                reason: action.reason,
                method: action.method,
                detail: action.detail
            }
            msging.sendContactActionsMsg(msg)
        
        } catch(e) {
            errlogger.recordAppError(new AppError('data-server', 'actionRecorder', 'recordContactAction', 'Error sending ContactActions message', e))
            return { statusCode: 500, err: new Error(`Error sending ContactActions message:\r\n${e.message}`) }
        }

        return {statusCode: 200}
    },

} // End of module.exports