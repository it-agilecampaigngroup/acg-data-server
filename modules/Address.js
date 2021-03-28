'use strict'

class Address {
    constructor(street1, street2, city, state, zip, addressType, isPrimary, addressId) {
        this.street1 = street1
        this.street2 = street2
        this.city = city
        this.state = state
        this.zip = zip
        this.addressType = addressType
        this.isPrimary = isPrimary
        this.addressId = addressId
        this.precint = "<not implemented>"
        this.districts = [
            {name: "<not implemented>", type: "Congressional District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "State House District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "State Senate District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Career Center District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "City School District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Local School District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Exempted Village School District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Educational Service Center District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "County Court District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Court of Appeals District", districtId: "<not implemented>" }
            ,{name: "<not implemented>", type: "Municipal Court District", districtId: "<not implemented>" }
        ]
    }
}

module.exports = Address