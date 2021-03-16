'use strict'

const db = require('./index')
const Person = require('../modules/Person')
const Phone = require('../modules/Phone')
const StreetAddress = require('../modules/StreetAddress')

module.exports = {
    
    //====================================================================================
    // Retrieve a random person from base.person
    //
    // Returns: A populated Person object 
    // 
    //===================================================================================
    async getRandomPerson() {
       
        // Build the SQL SELECT stmt
        let select = buildSelect() // Builds the complicate SELECT portion of the SQL
        select += "WHERE p.person_id = " + Math.floor(Math.random() * 1000000) + "\r\n"
          
        // For debugging
        //console.log(select)
    
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
            throw new Error(e.message)
        }
    }

} // End of module.export


//===========================================================================================================
//
// Private functions
//
//===========================================================================================================
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

