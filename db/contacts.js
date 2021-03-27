'use strict'

const db = require('./db')
const Person = require('../modules/Person')
const Phone = require('../modules/Phone')
const StreetAddress = require('../modules/StreetAddress')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('./errorRecorder')
const ActionRecorder = require('./actionRecorder')
const ContactAction = require('../modules/ContactAction')
//const campaign = require('../modules/Campaign')
const Contact = require('../modules/Contact')

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
            let action = new ContactAction("Contact requested", actor, contactReason, contactMethod, "Contact requested", "")
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
            // The error should already be recorded so we don't 
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
                , row.phone_number
                , row.phone_type
                , row.do_not_call_count
                , row.last_do_not_call_date
                , new Object
                , row.least_time
                , row.last_contact_attempt_time
                , row.donation_request_allowed_date
                , row.persuasion_attempt_allowed_date
                , row.turnout_request_allowed_date
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
       
        // Build the SQL
        var sql = ""
        if( contact.isVirtual ) {
            // The status record doesn't exist so we do an insert
            sql = "INSERT INTO base.contact_status (person_id, lease_time, last_contact_attempt_time, modified_by)\r\n"
            sql += `VALUES ( ${contact.personId}`
            sql += `, NOW(), NOW(), '${actor.username}'`
            sql += ");"
        } else {
            // Update the existing status record
            sql = "UPDATE base.contact_status\r\n"
            sql += `SET lease_time = NOW()\r\n`
            sql += `, last_contact_attempt_time = NOW()\r\n`
            sql += `, modified_by = '${actor.username}'\r\n`
            sql += `WHERE person_id = ${contact.personId};`
        }
        
        // For debugging
        //console.log(sql)
    
        // Execute the query and return the client
        try {
            const dbres = await db.query(sql)
            return // Success
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'markContactAsLeased', 'Database error marking contact as leased', e))
            throw new Error(e.message)
        }
    },

    //====================================================================================
    // Retrieve a random person from base.person
    //
    // Returns: A populated Person object 
    // 
    //===================================================================================
    async getRandomPerson() {
       
        // Build the SQL SELECT stmt
        let select = buildSelect() // Builds the complicate SELECT portion of the SQL
        select += "WHERE p.person_id = " + Math.floor(Math.random() * 1000000) + ";"
          
        // For debugging
        console.log(select)
    
        // Execute the query and return the client
        try {
            const dbres = await db.query(select)
            if( dbres.rowCount == 1) {
                const row = dbres.rows[0]
                const address = new StreetAddress(row.street1, row.street2, row.city, row.state, row.postal_code, row.county)
                const phones = [
                    new Phone("419-555-5555", "Mobile", true),
                    new Phone("419-444-5555", "Home", false),
                    new Phone("419-333-5555", "Other", false)
                ]
                return new Person(row.first_name, row.last_name, row.middle_name, row.prefix, row.suffix, address, phones)
            }
            return undefined
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'contacts.js', 'getRandomPerson', 'Database error retrieving random person', e))
            throw new Error(e.message)
        }
    }

} // End of module.export

//===========================================================================================================
//
// Private functions
//
//===========================================================================================================
function buildContactRequestSelect(contactReason, contactMethod) {
    let select = "SELECT first_name, last_name, middle_name, suffix, prefix\r\n"
    select += ", rating, phone_number, phone_type\r\n"
    select += ", do_not_call_count, last_do_not_call_date\r\n"
    select += ", lease_time, last_contact_attempt_time\r\n"
    select += ", donation_request_allowed_date\r\n"
    select += ", persuasion_attempt_allowed_date\r\n"
    select += ", turnout_request_allowed_date\r\n"
    select += ", person_id, is_virtual\r\n"
    switch(contactReason.toLowerCase()) {
        case "donation request":
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
        case "persuasion":
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
        case "turnout":
            switch(contactMethod.toLowerCase()) {
                case "phone call":
                    select += "FROM base.v_persuasion_phone_contact\r\n"
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

function buildSelect() {

    let select = "SELECT p.prefix, p.first_name, p.middle_name, p.last_name, p.suffix\r\n"
    select += ", TRIM(CONCAT(addr.number, ' ', addr.street_name)) street1\r\n"
    select += ", TRIM(CONCAT(addr.unit_type, ' ', addr.unit_number)) street2\r\n"
    select += ", loc.common_name city, aa.common_name state, addr.postal_code, sa.common_name county\r\n"
    select += ", (SELECT json_agg(person_phones)\r\n"
    select += "    FROM ( SELECT pp.phone_number, pt.description phone_type, pp.is_primary\r\n"
    select += "           FROM base.person_phone pp\r\n"
    select += "           INNER JOIN base.phone_type pt ON pt.type_id = pp.phone_type_id\r\n"
    select += "           WHERE pp.person_id = p.person_id ) person_phones) AS phones\r\n"
    select += "FROM base.person p\r\n"
    select += "LEFT OUTER JOIN base.person_address pa ON pa.person_id = p.person_id\r\n"
    select += "LEFT OUTER JOIN base.address addr ON addr.address_id = pa.address_id\r\n" 
    select += "LEFT OUTER JOIN base.admin_area aa ON aa.admin_area_id = addr.admin_area_id\r\n"
    select += "LEFT OUTER JOIN base.subadmin_area sa ON sa.subadmin_area_id = addr.subadmin_area_id\r\n"
    select += "LEFT OUTER JOIN base.locality loc ON loc.locality_id = addr.locality_id\r\n"
    
    return select
}

