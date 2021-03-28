'use strict'

class Contact {
    constructor(
        firstName, lastName, middleName, namePrefix, nameSuffix, rating
        , phones = [], addresses = []
        , leaseTime, lastContactAttemptTime, donationRequestAllowedDate
        , persuasionAttemptAllowedDate, turnoutRequestAllowedDate
        , donation, email, personId, isVirtual
    ) {
        this.firstName = firstName
        this.lastName = lastName
        this.middleName = middleName
        this.namePrefix = namePrefix
        this.nameSuffix = nameSuffix
        this.rating = rating
        this.phones = phones
        this.addresses = addresses
        this.leaseTime = leaseTime
        this.lastContactAttemptTime = lastContactAttemptTime
        this.donationRequestAllowedDate = donationRequestAllowedDate
        this.persuasionAttemptAllowedDate = persuasionAttemptAllowedDate
        this.turnoutRequestAllowedDate = turnoutRequestAllowedDate
        this.donation = {lastDonationAmount: 0.00, recommendedAsk: 500.00}
        this.email = [{address: 'someguy@dummymail.com'}]
        this.personId = personId
        this.isVirtual = isVirtual
    }
}

module.exports = Contact