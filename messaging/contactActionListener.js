const db = require('../db/db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const { Kafka, logLevel } = require('kafkajs')
const kafkaConfig = require('./kafkaConnection')
const kafka = new Kafka(kafkaConfig)
require('dotenv').config()

// Connect the consumer
const contactActionsGroup = 'Campaign-Actions-Group-' + process.env.VG_DATA_SERVER_CAMPAIGN_ID
const consumer = kafka.consumer({ groupId: contactActionsGroup })
const runConsumer = async () => {
    try {
        await consumer.connect()
        await consumer.subscribe({ topic: 'ContactActions'})
        await consumer.run( {
            autoCommitInterval: 10000,
            eachMessage: async ({ topic, partition, message }) => {
                const msg = JSON.parse(message.value.toString())
                if( msg.campaignId != process.env.VG_DATA_SERVER_CAMPAIGN_ID ) {
                    await processMessage(msg)
                } else {
                    // Ignore messages sent for our own campaign.
                    // We only need to process messages from
                    // other campaigns.
                }
            }
        })
    } catch (e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'runConsumer', 'Error starting ContactActions listener', e))
        throw new Error(e.message)
    }
}
runConsumer()

//===============================================
// Message processors
//===============================================

    //====================================================================================
    //
    // Processes a message
    //
    // msg: The message to process
    //
    // Returns nothing
    // 
    //===================================================================================
    async function processMessage(msg) {
        switch( msg.contactAction.toUpperCase() ) {
            case "CONTACT LEASED" :
                await markContactAsLeased(msg.detail.personId)
                break
            case "CONTACT RESPONDED" :
                await processContactResponse(msg)
                break
            case "CONTACT REJECTED" :
                await processContactRejected(msg)
                break
            default:
                // There's nothing to do
                break
        }
        return
    }

    //====================================================================================
    //
    // Processes a "Contact responded" message
    //
    // msg: The response message
    //
    // Returns nothing
    // 
    //===================================================================================
    async function processContactResponse(msg) {
        try {
            // Get the contact's status
            const contactStatus = await getContactStatus(msg.detail.personId)
            if( contactStatus === undefined ) {
                // Contact is not in our database. There's nothing to do.
                return
            }

            // Process the message
            switch(response.contactResult.toUpperCase()) {
                case "POSITIVE RESPONSE":
                case "NEGATIVE RESPONSE":
                    switch( msg.contactReason.toUpperCase() ) {
                        case "DONATION REQUEST" :
                            // If another campaign has made a donation request
                            // then we are not allowed to request a donation
                            // for at least 4 weeks AND we are not allowed
                            // to make a persuasion call for at least 24 hours.
        
                            // Make sure the donation allowed date isn't already 
                            // set beyond 28 days from now.
                            var newDate = new Date()
                            var donationUpdateNeeded = false
                            newDate.setDate( newDate.getDate() + 28 ) // 4 weeks from today
                            if( contactStatus.donationRequestAllowedDate < newDate ) {
                                donationUpdateNeeded = true
                            }
        
                            // Make sure the persuasion allowed date isn't already 
                            // set beyond 24 hours from now from now.
                            var persuasionUpdateNeeded = false
                            newDate = new Date()
                            newDate.setDate( newDate.getDate() + 1 ) // +24 hours
                            if( contactStatus.persuasionAttemptAllowedDate < newDate ) {
                                persuasionUpdateNeeded = true
                            }
        
                            // Update the dates
                            sql = "UPDATE base.contact_status\r\n"
                            sql += `SET callback_timestamp = NULL\r\n` // Reset callback info
                            sql += `, callback_actor_id = NULL\r\n`
                            sql += `, modified_by = 'system'\r\n`
                            sql += `, date_modified = NOW()\r\n`
                            if( donationUpdateNeeded ) {
                                sql += `, donation_request_allowed_date = NOW() + INTERVAL '28 days'\r\n`
                            }
                            if( persuasionUpdateNeeded ) {
                                sql += `, persuasion_attempt_allowed_date = NOW() + INTERVAL '24 hours'\r\n`
                            }
                            sql += `WHERE person_id = ${response.detail.personId};`
    
                            try {
                                await db.query(sql)
                            } catch(e) {
                                ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactResponse', `Database error updating donation request and/or persuasion attempt allowed dates:\r\n${sql}`, e))
                            }
 
                            break
 
                            case "PERSUASION" :
                            // If another campaign has made a persuasion call
                            // then we are not allowed to request a donation
                            // for at least 14 days.
                            var newDate = new Date()
        
                            // Make sure the donation allowed date isn't already 
                            // set beyond 14 days from now.
                            var donationUpdateNeeded = false
                            newDate.setDate( newDate.getDate() + 14 ) // 14 days from today
                            if( contactStatus.donationRequestAllowedDate < newDate ) {
                                donationUpdateNeeded = true
                            }
        
                            sql = "UPDATE base.contact_status\r\n"
                            sql += `SET callback_timestamp = NULL\r\n` // Reset callback info
                            sql += `, callback_actor_id = NULL\r\n`
                            if( donationUpdateNeeded ) {
                                sql += `, donation_request_allowed_date = NOW() + INTERVAL '14 days'\r\n`
                            }                            
                            sql += `, modified_by = 'system'\r\n`
                            sql += `, date_modified = NOW()\r\n`
                            sql += `WHERE person_id = ${response.detail.personId};`
    
                            try {
                                await db.query(sql)
                            } catch(e) {
                                ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactResponse', `Database error updating donation request allowed date:\r\n${sql}`, e))
                            }
                            
                            break
                       
                        case "TURNOUT" :
                            // Nothing to do for Turnout responses
                            break
                        
                        default:
                            break
                    }
                    break
                case "CALLBACK SCHEDULED":
                    sql = "UPDATE base.contact_status\r\n"
                    sql += `SET callback_timestamp = TO_TIMESTAMP(${msg.detail.callbackTimeStamp}, 'mm/dd/yyyy HH:MI:SS')\r\n`
                    sql += `, callback_actor_id = ${msg.detail.callbackActorId}\r\n`
                    sql += `, modified_by = 'system'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${msg.detail.personId};`
                
                    try {
                        await db.query(sql)
                    } catch(e) {
                        ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactResponse', `Database error updating donation request allowed date:\r\n${sql}`, e))
                    }
                    break

                default:
                    break
            }
        
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactResponse', `Unknown error processing Contact Response message`, e))
        }

        return
    }

    //====================================================================================
    //
    // Mark a contact as "leased"
    //
    // personId: The ID of the contact/person to mark as leased
    //
    // Returns nothing
    // 
    //===================================================================================
    async function markContactAsLeased(personId) {
       
        // Get the contact's status
        const contact = await getContactStatus(personId)
        if( contact === undefined ) {
            // Contact is not in our database. There's nothing to do.
            return
        }

        // Build the SQL for inserts
        var insertSQL = "INSERT INTO base.contact_status (person_id, lease_time, last_contact_attempt_time, modified_by)\r\n"
        insertSQL += `VALUES ( ${personId}`
        insertSQL += `, NOW(), NOW(), 'system'`
        insertSQL += ");"

        // Build the SQL for updates
        var updateSQL = "UPDATE base.contact_status\r\n"
        updateSQL += `SET lease_time = NOW()\r\n`
        updateSQL += `, last_contact_attempt_time = NOW()\r\n`
        updateSQL += `, modified_by = 'system'\r\n`
        updateSQL += `, date_modified = NOW()\r\n`
        updateSQL += `WHERE person_id = ${personId};`

        if( contact.isVirtual ) {
            // The status record doesn't exist so we attempt an insert
            try {
                // For debugging
                //console.log(insertSQL)
                await db.query(insertSQL)
                return // Success
            } catch(e) {
                // Insert failed. If it's a primary key error then try an update instead.
                if( e.message.includes('duplicate key value violates unique constraint') ) {
                    try {
                        // For debugging
                        //console.log(updateSQL)
                        await db.query(updateSQL)
                        return // Success
                    } catch(e) {
                        ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'markContactAsLeased', 'Database error updating contact as leased', e))
                        throw new Error(e.message)
                    }
                } else {
                    ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'markContactAsLeased', 'Database error inserting contact leased record', e))
                    throw new Error(e.message)
                }
            }
        } else {
            // Update the existing status record
            // For debugging
            try {
                await db.query(updateSQL)
                return // Success
            } catch(e) {
                ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'markContactAsLeased', 'Database error updating contact as leased', e))
                throw new Error(e.message)
            }
        }
   }

    //====================================================================================
    //
    // Processes a "Contact rejected" message
    //
    // msg: The response message
    //
    // Returns nothing
    // 
    //===================================================================================
    async function processContactRejected(msg) {
        try {
            // Get the contact's status
            const contactStatus = await getContactStatus(msg.detail.personId)
            if( contactStatus === undefined ) {
                // Contact is not in our database. There's nothing to do.
                return
            }

            // Process the message
            switch(response.contactResult.toUpperCase()) {
                case "CALLBACK CANCELLED":
                    // Cancel the callback chain
                    sql = "UPDATE base.contact_status\r\n"
                    sql += `SET callback_timestamp = NULL\r\n`
                    sql += `, callback_actor_id = NULL\r\n`
                    sql += `, modified_by = 'system'\r\n`
                    sql += `, date_modified = NOW()\r\n`
                    sql += `WHERE person_id = ${msg.detail.personId};`

                    try {
                        await db.query(sql)
                    } catch(e) {
                        ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactRejected', `Database error cancelling callback chain:\r\n${sql}`, e))
                    }

                    break
            
                default:
                    break
            }
        
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'processContactRejected', `Unknown error processing Contact Rejected message`, e))
        }

        return
    }

    //====================================================================================
    //
    // Retrieve a contact's status from the database
    //
    // personId: The Id of the contact.
    //
    // Returns the contact's status or "undefined" if the contact is not
    // in the database.
    // 
    //===================================================================================
    async function getContactStatus(personId) {

        var sql = `SELECT p.person_id, cs.lease_time, cs.last_contact_attempt_time\r\n`
        sql += `, COALESCE(cs.donation_request_allowed_date, TO_DATE('01/01/2000', 'mm/dd/yyyy')) donation_request_allowed_date\r\n`
        sql += `, COALESCE(cs.persuasion_attempt_allowed_date, TO_DATE('01/01/2000', 'mm/dd/yyyy')) persuasion_attempt_allowed_date\r\n`
        sql += `, COALESCE(cs.turnout_request_allowed_date, TO_DATE('01/01/2000', 'mm/dd/yyyy')) turnout_request_allowed_date\r\n`
        sql += `, COALESCE(cs.callback_timestamp, TO_DATE('01/01/2000', 'mm/dd/yyyy')) callback_timestamp\r\n`
        sql += `, COALESCE(cs.callback_actor_id, 0)) callback_actor_id\r\n`
        sql += `, cs.review_required\r\n`
        sql += `, CASE WHEN cs.person_id IS NULL THEN true ELSE false END is_virtual\r\n`
        sql += `FROM base.person p\r\n`
        sql += `LEFT OUTER JOIN base.contact_status cs ON cs.person_id = p.person_id\r\n`
        sql += `WHERE p.person_id = ${personId};\r\n`

        // For debugging
        //console.log( { getContactStatus: sql } )

        // Execute the query and return the contact
        try {
            const dbres = await db.query(sql)
            if( dbres.rowCount == 1) {
                const row = dbres.rows[0]
                return {
                    personId: row.person_id
                    , isVirtual: row.is_virtual
                    , leaseTime: row.lease_time
                    , lastContactAttemptTime: row.last_contact_attempt_time
                    , donationRequestAllowedDate: new Date(row.donation_request_allowed_date)
                    , persuasionAttemptAllowedDate: new Date(row.persuasion_attempt_allowed_date)
                    , turnoutRequestAllowedDate: new Date(row.turnout_request_allowed_date)
                    , callbackTimestamp: new Date(row.callback_timestamp)
                    , callbackActorId: parseInt(row.callback_actor_id)
                    , reviewRequired: row.review_required
                }
            }
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contactActionListener.js', 'getContactStatus', 'Database error retrieving contact status', e))
        }
        return undefined
    }

