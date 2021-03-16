'use strict'

class Phone {
    constructor(phoneNumber, phoneType = 'Home', isPrimary = true) {
        this.phoneNumber = phoneNumber
        this.phoneType = phoneType
        this.isPrimary = isPrimary
    }
}

module.exports = Phone