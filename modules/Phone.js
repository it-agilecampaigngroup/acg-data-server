'use strict'

class Phone {
    constructor(phoneNumber, phoneType = 'Home', isPrimary = true
        , doNotCallCount, lastDoNotCallDate, phoneId) {
        this.phoneNumber = phoneNumber
        this.phoneType = phoneType
        this.isPrimary = isPrimary
        this.doNotCallCount = doNotCallCount
        this.lastDoNotCallDate = lastDoNotCallDate
        this.phoneId = phoneId
    }

}

module.exports = Phone