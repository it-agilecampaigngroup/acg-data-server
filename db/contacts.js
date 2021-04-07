'use strict'

const db = require('./db')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('./errorRecorder')
const ActionRecorder = require('./actionRecorder')
const ContactAction = require('../modules/ContactAction')
const Contact = require('../modules/Contact')
const Phone = require('../modules/Phone')
const Address = require('../modules/Address')

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
            // Record a "Contact requested" action
            let action = new ContactAction("Contact requested", actor
                , contactReason, contactMethod, "Contact requested", "")
            ActionRecorder.recordContactAction(action)

            // Get the best contact
            let contact = await this.getContact(contactReason, contactMethod)
            
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
       
        // Build the SQL SELECT stmt
        let select = buildContactRequestSelect(contactReason, contactMethod)

        // For debugging
        //console.log(select)
    
        // Execute the query and return the contact
        try {
            const dbres = await db.query(select)
            if( dbres.rowCount == 1) {
                const row = dbres.rows[0]
                return new Contact(
                row.first_name
                , row.last_name
                , row.middle_name
                , row.prefix
                , row.suffix
                , row.rating
                , translatePhones(row.phones)
                , translateAddresses(row.addresses)
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
            }
            return undefined
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'getContact', 'Database error retrieving random contact', e))
            throw new Error(e.message)
        }
    },

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

        // if( contact.isVirtual ) {
        //     // The status record doesn't exist so we do an insert
        //     sql = "INSERT INTO base.contact_status (person_id, lease_time, last_contact_attempt_time, modified_by)\r\n"
        //     sql += `VALUES ( ${contact.personId}`
        //     sql += `, NOW(), NOW(), '${actor.username}'`
        //     sql += ");"
        // } else {
        //     // Update the existing status record
        //     sql = "UPDATE base.contact_status\r\n"
        //     sql += `SET lease_time = NOW()\r\n`
        //     sql += `, last_contact_attempt_time = NOW()\r\n`
        //     sql += `, modified_by = '${actor.username}'\r\n`
        //     sql += `, date_modified = NOW()\r\n`
        //     sql += `WHERE person_id = ${contact.personId};`
        // }
        
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

        // // Execute the query and return the client
        // try {
        //     const dbres = await db.query(sql)
        //     return // Success
        // } catch(e) {
        //     ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'markContactAsLeased', 'Database error marking contact as leased', e))
        //     throw new Error(e.message)
        // }
    }

   } // End of module.export

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
    select += ", person_id, is_virtual\r\n"
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
                    select += "FROM base.v_turnout_requst_phone_contact\r\n"
                    break;
                case "canvass":
                    break;
                default:
                    break;
            }
        default:
            break;
    }
    // Finally, add a limit of one record
    select += "LIMIT 1;"

    return select;
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


