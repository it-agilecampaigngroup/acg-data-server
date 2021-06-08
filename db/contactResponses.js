'use strict'

const db = require('./db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('./errorRecorder')
const ActionRecorder = require('./actionRecorder')
const ContactAction = require('../modules/ContactAction')

module.exports = {
    async processContactResponse(response) {
        
        // Record the response and send a Contact Response 
        // message to other campaigns
        ActionRecorder.recordContactAction(
            new ContactAction(
                response.contactAction
                , response.actor
                , response.contactReason
                , response.contactMethod
                , response.contactResult
                , response.detail
            )
        )

        // Perform the tasks necessary for the various
        // response types.
        switch(response.contactAction.toUpperCase()) {
            case "CONTACT RESPONDED":
                await processContactRespondedResponse(response)
                break;
            case "CONTACT ATTEMPT FAILED":
                await processContactAttemptFailedResponse(response)
                break;
            case "CONTACT REJECTED":
                await processContactRejectedResponse(response)
                break;
            default:
                let msg = `Error processing contact response: Action '${response.contactAction}' is not a valid action`
                let e = new Error(msg)
                ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactResponse', msg, e))
                throw e
        }

        return
    }

} // End of module.exports

//==================================================================================
// Private functions
//==================================================================================

//============================================================
// Process Contact Responded responses
//
// response: A fully populated ContactResponse object
//
//============================================================
async function processContactRespondedResponse(response) {
    var sql = ""

    switch(response.contactResult.toUpperCase()) {
        case "POSITIVE RESPONSE":
        case "NEGATIVE RESPONSE":
            switch(response.contactReason.toUpperCase()) {
                
                case "DONATION REQUEST": 
                    // Determine the appropriate interval for "donation request allowed" date
                    var interval = "NOW() + INTERVAL '6 months'" // This is the default interval
                    if( response.contactResult.toUpperCase() == "POSITIVE RESPONSE") {
                        if( response.detail.recurring !== undefined ) {
                            if( response.detail.recurring === true ) {
                            // If the donation is recurring set the interval way out
                            interval = "NOW() + INTERVAL '4 years'" 
                            }
                        }
                    }

                    // Update the existing status record
                    sql = "UPDATE base.contact_status\r\n"
                    sql += `SET donation_request_allowed_date = ${interval}\r\n`
                    sql += `, persuasion_attempt_allowed_date = NOW() + INTERVAL '2 weeks'\r\n`
                    // Reset callback fields
                    sql += `, callback_timestamp = NULL\r\n`
                    sql += `, callback_actor_id = NULL\r\n`
                    sql += `, modified_by = '${response.actor.username}'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${response.detail.personId};`
                    executeSQL(sql)
                    break;

                case "PERSUASION":
                    // Set donation_request_allowed_date = "today" + 7 days
                    // and set persuasion_attempt_allowed_date = "today" + 7 days
                    sql = "UPDATE base.contact_status\r\n"
                    sql += `SET donation_request_allowed_date = NOW() + INTERVAL '7 days'\r\n`
                    sql += `, persuasion_attempt_allowed_date = NOW() + INTERVAL '7 days'\r\n`
                    // Reset callback fields
                    sql += `, callback_timestamp = NULL\r\n`
                    sql += `, callback_actor_id = NULL\r\n`
                    sql += `, modified_by = '${response.actor.username}'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${response.detail.personId};`
                    executeSQL(sql)
                    break;

                case "TURNOUT":
                    // Nothing to do
                    break;

                default:
                    let msg = `Error processing contact response: Reason '${response.contactReason}' is not a valid contact reason`
                    let e = new Error(msg)
                    ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactRespondedResponse', msg, e))
                    throw e
            }
            break;
        case "CALLBACK SCHEDULED":
            // Update the existing status record
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET callback_timestamp = '${new Date(response.detail.callbackTimestamp).toISOString()}'\r\n`
            sql += `, callback_actor_id = ${response.detail.callbackActorId}\r\n`
            sql += `, modified_by = '${response.actor.username}'\r\n`
            sql += `, date_modified = NOW()\r\n`
            sql += `WHERE person_id = ${response.detail.personId};`
            executeSQL(sql)
            break;
            
        default:
            let msg = `Error processing contact response: Result '${response.contactResult}' is not a valid contact result`
            let e = new Error(msg)
            ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactRespondedResponse', msg, e))
            throw e
        }
    return
}

//============================================================
// Process Contact Attempt Failed responses
//============================================================
async function processContactAttemptFailedResponse(response) {
    var sql = ""

    switch(response.contactResult.toUpperCase()) {
        
        case "NO ANSWER/NOT HOME":
            // Set lease_time to NULL in contact_status 
            // so contact will be contacted again
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET lease_time = NULL\r\n`
            sql += `, donation_request_allowed_date = NOW() + interval '7 days'\r\n`
            sql += `, modified_by = '${response.actor.username}'\r\n`
            sql += `, date_modified = NOW()\r\n`
            sql += `WHERE person_id = ${response.detail.personId};`
            executeSQL(sql)
            break;

        case "CONTACT HAS MOVED":
            // Remove the contact's address from the 
            // person_address table
            if( response.detail.addressId !== undefined ) {
                // Address ID was provided
                sql = `DELETE FROM base.person_address\r\n`
                sql += `WHERE person_id = ${response.detail.personId}\r\n`
                sql += `AND address_id = ${response.detail.addressId};`
            } else {
                // Address ID not provided. Get it
                // from the person_address table.
                sql = `DELETE FROM base.person_address\r\n`
                sql += `WHERE person_id = ${response.detail.personId}\r\n`
                sql += `AND address_id = (\r\n`
                sql += `    SELECT address_id\r\n`
                sql += `    FROM base.person_address\r\n`
                sql += `    WHERE person_id = ${response.detail.personId}\r\n`
                sql += `    AND is_primary = true\r\n`
                sql += `    );\r\n`
            }
            executeSQL(sql)
            break;

        case "CONTACT IS DECEASED":
            // Remove the person from contact_rating and
            // contact status. This will prevent attempts
            // to call this person again.
            sql = `DELETE FROM base.contact_rating WHERE person_id = ${response.detail.personId};\r\n`
            sql += `DELETE FROM base.contact_status WHERE person_id = ${response.detail.personId};\r\n`
            executeSQL(sql)
            break;

        case "PHONE/ADDRESS IS INVALID":
            switch(response.contactMethod.toUpperCase()) {

                case "PHONE CALL":
                    // Remove the contact's phone from person_phone.
                    sql = `DELETE FROM base.person_phone\r\n`
                    sql += `WHERE person_id = ${response.detail.personId}\r\n`
                    sql += `AND phone_id = ${response.detail.phoneId};`
                    executeSQL(sql)
                    break;

                case "CANVASS":
                    // Remove contract's address from person_address.
                    sql = `DELETE FROM base.person_address\r\n`
                    sql += `WHERE person_id = ${response.detail.personId}\r\n`
                    sql += `AND address_id = ${response.detail.addressId};`
                    executeSQL(sql)
                    break;

                default:
                    let msg = `Error processing contact response: Method '${response.contactMethod}' is not a valid contact method`
                    let e = new Error(msg)
                    ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactAttemptFailedResponse', msg, e))
                    throw e
            }
            break;

        case "REFUSED (DO NOT CALL AGAIN)":
            switch(response.contactMethod.toUpperCase()) {

                case "PHONE CALL":
                    // If the method is "Phone call" then increment
                    // the do_not_call_count value and set the 
                    // last_do_not_call_date to "today" in the person_phone 
                    // table. This will prevent any calls to this number for
                    // a period of time (i.e 6 months as of this writing).
                    sql = "UPDATE base.person_phone\r\n"
                    sql += `SET do_not_call_count = do_not_call_count + 1\r\n`
                    sql += `, last_do_not_call_date = NOW()\r\n`
                    sql += `, modified_by = '${response.actor.username}'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${response.detail.personId}\r\n`
                    sql += `AND phone_id = ${response.detail.phoneId};`
                    executeSQL(sql)
                    break;

                default:
                    // If the method is not "Phone call" then set the 
                    // appropriate "allowed" date in the contact_status
                    // table to "today" + 1 month so that the person is
                    // not contacted for at least 1 month:
                    //  If this is a donation request, set donation_request_allowed_date
                    //  If this is a persuasion visit set persuasion_attempt_allowed_date
                    //  If this is a turnout request, set turnout_request_allowed_date
                    sql = "UPDATE base.contact_status\r\n"
                    switch(response.reason.toUpperCase()) {
                        case "DONATION REQUEST":
                            sql += `SET donation_request_allowed_date = NOW() + INTERVAL '1 month'\r\n`
                            break;
                        
                        case "PERSUASION":
                            sql += `SET persuasion_attempt_allowed_date = NOW() + INTERVAL '1 month'\r\n`
                            break;
                            
                        case "TURNOUT":
                            sql += `SET turnout_request_allowed_date = NOW() + INTERVAL '1 month'\r\n`
                            break;
                        
                        default:
                            let msg = `Error processing contact response: Reason '${response.contactReason}' is not a valid contact reason`
                            let e = new Error(msg)
                            ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactAttemptFailedResponse', msg, e))
                            throw e
                        }
                    sql += `, modified_by = '${response.actor.username}'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${response.detail.personId};`
                    executeSQL(sql)
                    break;
            }
            break;

        default:
            let msg = `Error processing contact response: Result '${response.contactResult}' is not a valid contact result`
            let e = new Error(msg)
            ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactAttemptFailedResponse', msg, e))
            throw e
    }
    return
}

//============================================================
// Process Contact Rejected responses
//============================================================
async function processContactRejectedResponse(response) {
    var sql = ""

    switch(response.contactResult.toUpperCase()) {

        case "CONFLICT OF INTEREST":
            // Set lease_time to NULL in contact_status 
            // so contact will be contacted again (by some other
            // actor, hopefully).
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET lease_time = NULL\r\n`
            sql += `, modified_by = '${response.actor.username}'\r\n`
            sql += `, date_modified = NOW()\r\n`
            sql += `WHERE person_id = ${response.detail.personId};`
            executeSQL(sql)
            break;

        case "CALLBACK CANCELLED":
            // Cancel the callback chain
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET callback_timestamp = NULL\r\n`
            sql += `, callback_actor_id = NULL\r\n`
            sql += `, modified_by = '${response.actor.username}'\r\n`
            sql += `, date_modified = NOW()\r\n`
            sql += `WHERE person_id = ${response.detail.personId};`
            executeSQL(sql)
            break;

        case "CONTACT IS ELECTED OFFICIAL":
        case "OTHER":
            // Set review_required and review_required_note in
            // contact_status table so that the person is not contacted
            // until their status has been reviewed and modified.
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET review_required = true\r\n`
            sql += `, review_required_note = '${db.formatTextForSQL(response.detail.note)}'\r\n`
            sql += `, modified_by = '${response.actor.username}'\r\n`
            sql += `, date_modified = NOW()\r\n`
            sql += `WHERE person_id = ${response.detail.personId};`
            executeSQL(sql)
            break;

        case "CONTACT INFORMATION IS INVALID":
            switch(response.contactMethod.toUpperCase()) {
                case "PHONE CALL":
                    // Remove the contact's phone from person_phone.
                    sql = `DELETE FROM base.person_phone\r\n`
                    sql += `WHERE person_id = ${response.detail.personId}\r\n`
                    sql += `AND phone_id = ${response.detail.phoneId};`
                    executeSQL(sql)
                    break;

                case "CANVASS":
                    // Remove contract's address from person_address.
                    sql = `DELETE FROM base.person_address\r\n`
                    sql += `WHERE person_id = ${response.detail.personId}\r\n`
                    sql += `AND address_id = ${response.detail.addressId};`
                    executeSQL(sql)
                    break;

                default:
                    let msg = `Error processing contact response: Method '${response.contactMethod}' is not a valid contact method`
                    let e = new Error(msg)
                    ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactRejectedResponse', msg, e))
                    throw e
            }
            break;

        default:
            let msg = `Error processing contact response: Result '${response.contactResult}' is not a valid result`
            let e = new Error(msg)
            ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'processContactRejectedResponse', msg, e))
            throw e
    }
    return
}

async function executeSQL(sql) {
    // For debugging
    //console.log(sql)

    // Execute the query and return the client
    try {
        await db.query(sql)
        return // Success
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'db/contactResponses.js', 'executeSql', `Database error executing SQL: ${sql}`, e))
        throw new Error(e.message)
    }
}