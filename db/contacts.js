'use strict'

const db = require('./db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('./errorRecorder')
const ActionRecorder = require('./actionRecorder')
const ContactAction = require('../modules/ContactAction')
const Contact = require('../modules/Contact')
const Phone = require('../modules/Phone')
const Address = require('../modules/Address')
const Queue = require('../modules/Queue')

let donationCallContactQueue = new Queue()
let persuasionCallContactQueue = new Queue()
let turnoutCallContactQueue = new Queue()
let donationCanvassContactQueue = new Queue()
let persuasionCanvassContactQueue = new Queue()
let turnoutCanvassContactQueue = new Queue()

initializeContactQueues()

module.exports = {
    
    async getBestContact(actor, contactReason, contactMethod) {
        // Make sure the contact reason is valid
        if (contactReason === undefined ) {
            throw new Error("A contact reason must be specified")
        }
        switch(contactReason.toUpperCase()) {
            case "DONATION REQUEST":
                break; // All is well
            case "PERSUASION":
            case "TURNOUT":
            default:
            throw new Error(`'${contactReason}' is not a valid contact reason`)
        }
        
        // Make sure the contact method is valid
        if( contactMethod === undefined ) {
            throw new Error("A contact method must be specified")
        }
        switch(contactMethod.toUpperCase()) {
            case "PHONE CALL":
                break; // All is well
            case "CANVASS":
            case "EMAIL":
            case "TEXT":
            default:
            throw new Error(`'${contactMethod}' is not a valid contact method`)
        }

        // Things look good so let's try to grab a contact
        try {
            // Get the highest rated contact
            let contact = await this.getContact(contactReason, contactMethod)

            // Record a "Contact requested" action
            let action = new ContactAction("Contact requested", actor
                , contactReason, contactMethod, "Contact requested", "")
            ActionRecorder.recordContactAction(action)

            // Record a "Contact leased" action. We send the message before
            // marking the contact as leased in our database to minimize
            // the possibility that some other campaign will grab the contact
            // before we finish marking the contact as leased.
            action = new ContactAction("Contact leased", actor
                , contactReason, contactMethod, "Contact leased"
                , {personId: contact.personId, firstName: contact.firstName, lastName: contact.lastName})
            ActionRecorder.recordContactAction(action)

            // Mark the contact as leased
            this.markContactAsLeased(contact, actor)

            // Return the contact
            return contact    
        
        } catch(e) {
            // Any error we get should already be recorded so we don't 
            // need to do anything here.
            console.log(e)
        }
    },

    //====================================================================================
    // Retrieve a contact
    //
    // Returns: A populated Contact object 
    // 
    //===================================================================================
    async getContact(contactReason, contactMethod) {

        // Get the appropriate queue
        var queue = getContactQueue(contactReason, contactMethod)
        if( queue == undefined ) {
            let err_msg = `Queue for ${contactReason}/${contactMethod} is not supported.`
            ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'getContact', err_msg, new Error(err_msg)))
            return undefined
        }

        var contact = undefined
        while( contact == undefined ) {
        // If queue is empty then initialize it
            if( queue.isEmpty() ) 
                await initializeContactQueue(contactReason, contactMethod)

            // Get a contact
            let contact = queue.pop()

            if( contact != undefined ) {
                // Make sure the contact is not leased and return it.
                // We need to make sure the contact isn't leased
                // because some other campaign may have leased it
                // after it was put into the queue.
                if( await isContactLeased(contact.personId) == false ) {
                    return contact
                }

                // Contact is leased. Try another one.
                contact = undefined // Keep the loop alive.

            } else {
                // There are no more contacts for the reason/method
                let err_msg = `No contacts were found for ${contactReason}/${contactMethod}.`
                ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'getContact', err_msg, new Error(err_msg)))
                return undefined
            }
        }

        return undefined // This should never happen
    },

    // //====================================================================================
    // // Retrieve a contact
    // //
    // // Returns: A populated Contact object 
    // // 
    // //===================================================================================
    // async getContact(contactReason, contactMethod) {
       
    //     // Build the SQL SELECT stmt
    //     let select = buildContactRequestSelect(contactReason, contactMethod)

    //     // For debugging
    //     //console.log(select)
    
    //     // Execute the query and return the contact
    //     try {
    //         const dbres = await db.query(select)
    //         if( dbres.rowCount == 1) {
    //             const row = dbres.rows[0]
    //             //Populate the donation summary if no info
    //             // was returned by the query
    //             var donationSummary = row.donation_summary
    //             if( donationSummary === null) {
    //                 donationSummary = {
    //                     lastDonationAmount: 0
    //                     , recommendedAsk: 500.00
    //                 }
    //             }

    //             //Create and return the contact
    //             return new Contact(
    //             row.first_name
    //             , row.last_name
    //             , row.middle_name
    //             , row.prefix
    //             , row.suffix
    //             , row.rating
    //             , translatePhones(row.phones)
    //             , translateAddresses(row.addresses)
    //             , donationSummary
    //             , row.districts
    //             , row.lease_time
    //             , row.last_contact_attempt_time
    //             , row.donation_request_allowed_date
    //             , row.persuasion_attempt_allowed_date
    //             , row.turnout_request_allowed_date
    //             , {}
    //             , []
    //             , row.person_id
    //             , row.is_virtual
    //             )
    //         }
    //         return undefined
    //     } catch(e) {
    //         ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'getContact', 'Database error retrieving random contact', e))
    //         throw new Error(e.message)
    //     }
    // },

    //====================================================================================
    // Mark a contact as "leased"
    //
    //
    // contact: A Contact object populated from the database
    //
    // Returns nothing
    // 
    //===================================================================================
    async markContactAsLeased(contact, actor) {
       
        // Build the SQL for inserts
        var insertSQL = "INSERT INTO base.contact_status (person_id, lease_time, last_contact_attempt_time, modified_by)\r\n"
        insertSQL += `VALUES ( ${contact.personId}`
        insertSQL += `, NOW(), NOW(), '${actor.username}'`
        insertSQL += ");"

        // Build the SQL for updates
        var updateSQL = "UPDATE base.contact_status\r\n"
        updateSQL += `SET lease_time = NOW()\r\n`
        updateSQL += `, last_contact_attempt_time = NOW()\r\n`
        updateSQL += `, modified_by = '${actor.username}'\r\n`
        updateSQL += `, date_modified = NOW()\r\n`
        updateSQL += `WHERE person_id = ${contact.personId};`

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
                        ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'markContactAsLeased', 'Database error updating contact as leased', e))
                        throw new Error(e.message)
                    }
                } else {
                    ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'markContactAsLeased', 'Database error inserting contact leased record', e))
                    throw new Error(e.message)
                }
            }
        } else {
            // Update the existing status record
            // For debugging
            //console.log(updateSQL)
            try {
                await db.query(updateSQL)
                return // Success
            } catch(e) {
                ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'markContactAsLeased', 'Database error updating contact as leased', e))
                throw new Error(e.message)
            }
        }

   }

} // End of module.exports

//===========================================================================================================
//
// Private functions
//
//===========================================================================================================
function buildContactRequestSelect(contactReason, contactMethod) {
    let select = "SELECT first_name, last_name, middle_name, suffix, prefix\r\n"
    select += ", rating, phones, addresses\r\n"
    select += ", lease_time, last_contact_attempt_time\r\n"
    select += ", donation_request_allowed_date\r\n"
    select += ", persuasion_attempt_allowed_date\r\n"
    select += ", turnout_request_allowed_date\r\n"
    select += ", person_id, is_virtual, donation_summary\r\n"
    select += ", districts\r\n"

    switch(contactReason.toUpperCase()) {
        case "DONATION REQUEST":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    select += "FROM base.v_donation_phone_contact\r\n"
                    break;
                case "canvass":
                    break;
                default:
                    break;
            }
            break;
        case "PERSUASION":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    select += "FROM base.v_persuasion_phone_contact\r\n"
                    break;
                case "canvass":
                    break;
                default:
                    break;
            }
            break;                
        case "TURNOUT":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    select += "FROM base.v_turnout_request_phone_contact\r\n"
                    break;
                case "canvass":
                    break;
                default:
                    break;
            }
        default:
            break;
    }
    // // Finally, add a limit of one record because we only need one
    // select += "LIMIT 1;"

    // Finally, add a limit of 100 records
    select += "LIMIT 100;"

    return select;
}

// Function to intitalize any one of the contact queues
async function initializeContactQueue(contactReason, contactMethod) {

    var targetQueue = getContactQueue(contactReason, contactMethod)

    // Make sure we support the stack to be initialized
    if( targetQueue == undefined ) {
        let err_msg = `Queue for ${contactReason}/${contactMethod} is not supported.`
        ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'initializeContactQueue', err_msg, new Error(err_msg)))
        return false
    }
        
    // Build the SQL SELECT stmt
    let select = buildContactRequestSelect(contactReason, contactMethod)

    // For debugging
    //console.log(select)

    // Execute the query and populate the queue
    try {
        const dbres = await db.query(select)
        if( dbres.rowCount > 0 ) {
            // Push the contacts into the queue
            for( var idx = 0; idx < dbres.rowCount; idx++ ) {
                let row = dbres.rows[idx]

                //Populate the donation summary if no info
                // was returned by the query
                var donationSummary = row.donation_summary
                if( donationSummary === null) {
                    donationSummary = {
                        lastDonationAmount: 0
                        , recommendedAsk: 500.00
                    }
                }

                //Create and push the contact
                targetQueue.push(
                    new Contact(
                    row.first_name
                    , row.last_name
                    , row.middle_name
                    , row.prefix
                    , row.suffix
                    , row.rating
                    , translatePhones(row.phones)
                    , translateAddresses(row.addresses)
                    , donationSummary
                    , row.districts
                    , row.lease_time
                    , row.last_contact_attempt_time
                    , row.donation_request_allowed_date
                    , row.persuasion_attempt_allowed_date
                    , row.turnout_request_allowed_date
                    , {}
                    , []
                    , row.person_id
                    , row.is_virtual
                    )
                )
            }
            return true
        }
        else {
            let err_msg = `No contacts returned from the database for ${contactReason}/${contactMethod}`
            ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'initializeContactQueue', err_msg, new Error(err_msg)))
            return false
        }
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'initializeContactQueue', `Database error initializing contact queue for ${contactReason}/${contactMethod}`, e))
        throw new Error(e.message)
    }    
}

// Function to get the appropriate contact queue
function getContactQueue(contactReason, contactMethod) {
    switch(contactReason.toUpperCase()) {
        case "DONATION REQUEST":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    return donationCallContactQueue
                    break;
                case "canvass":
                    return donationCanvassContactQueue
                    break;
                default:
                    break;
            }
            break;
        case "PERSUASION":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    return persuasionCallContactQueue
                    break;
                case "canvass":
                    return persuasionCanvassContactQueue
                    break;
                default:
                    break;
            }
            break;                
        case "TURNOUT":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    return turnoutCallContactQueue
                    break;
                case "canvass":
                    return turnoutCanvassContactQueue
                    break;
                default:
                    break;
            }
        default:
            break;
    }
    
    return undefined // Reason/method not currently supported
}

function initializeContactQueues() {
    initializeContactQueue("Donation request", "phone call")
    // initializeContactQueue("Donation request", "canvass")
    //initializeContactQueue("Persuasion", "phone call")
    // initializeContactQueue("Persuasion", "canvass")
    //initializeContactQueue("Turnout", "phone call")
    // initializeContactQueue("Turnout", "canvass")
}

// Function to check if a contact has been leased
async function isContactLeased(personId) {

    var sql = `SELECT CASE WHEN COALESCE(lease_time, NOW() - INTERVAL '1 day') < NOW() - INTERVAL '23 hours'\r\n`
    sql += `	THEN false\r\n`
    sql += `	ELSE true\r\n`
    sql += `	END is_leased\r\n`
    sql += `FROM base.contact_status\r\n`
    sql += `WHERE person_id = ${personId};\r\n`

    try {
        const dbres = await db.query(sql)
        if( dbres.rowCount == 0) {
            // If no row is returned then there's
            // no record in the contact_status
            // table and the contact is not leased.
            return false;
        }
        if( dbres.rowCount == 1) {
            const row = dbres.rows[0]
            return row.is_leased
        }
        return true; // If something is wrong assume that the contact is leased.
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'isContactLeased', 'Database error checking if contact is leased', e))
        throw new Error(e.message)
    }

}

function translatePhones( phones ) {
    var phone_array = []
    for( let idx = 0; idx < phones.length; idx++ ) {
        let phone = phones[idx]
        phone_array.push( new Phone(
            phone.phone_number
            , phone.phone_type
            , phone.is_primary
            , phone.do_not_call_count
            , phone.lastDoNotCallDate
            , phone.phone_id
            )
        )
    }
    return phone_array
}

function translateAddresses( addresses ) {
    var address_array = []
    for( let idx = 0; idx < addresses.length; idx++ ) {
        let address = addresses[idx]
        address_array.push( new Address(
            address.street1
            , address.street2
            , address.city
            , address.state
            , address.zip
            , address.address_type
            , address.is_primary
            , address.address_id
            )
        )
    }
    return address_array
}


