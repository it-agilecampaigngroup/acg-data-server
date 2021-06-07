'use strict'

class Contact {
    constructor(
        firstName, lastName, middleName, namePrefix, nameSuffix, rating
        , phones = [], addresses = [], donationSummary, districts
        , leaseTime, lastContactAttemptTime, donationRequestAllowedDate
        , persuasionAttemptAllowedDate, turnoutRequestAllowedDate
        , donation, email, personId, isVirtual, precinctName
    ) {
        this.firstName = (firstName == undefined) ? "" : firstName
        this.lastName = (lastName == undefined) ? "" : lastName
        this.middleName = (middleName == undefined) ? "" : middleName
        this.namePrefix = (namePrefix == undefined) ? "" : namePrefix
        this.nameSuffix = (nameSuffix == undefined) ? "" : nameSuffix
        this.rating = rating
        this.phones = phones
        this.addresses = addresses
        this.donationSummary = donationSummary
        this.districts = districts
        this.leaseTime = leaseTime
        this.lastContactAttemptTime = lastContactAttemptTime
        this.donationRequestAllowedDate = donationRequestAllowedDate
        this.persuasionAttemptAllowedDate = persuasionAttemptAllowedDate
        this.turnoutRequestAllowedDate = turnoutRequestAllowedDate
        this.donation = {lastDonationAmount: 0.00, recommendedAsk: 500.00}
        this.email = [{address: 'someguy@dummymail.com'}]
        this.personId = personId
        this.isVirtual = isVirtual
        this.precinctName = precinctName
    }
}

module.exports = Contact