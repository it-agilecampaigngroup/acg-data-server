'use strict'


class Contact {
    constructor(
        firstName, lastName, middleName, namePrefix, nameSuffix, rating
        , phoneNumber, phoneType, doNotCallCount, lastDoNotCallDate, address
        , leastTime, lastContactAttemptTime, donationRequestAllowedDate
        , persuasionAttemptAllowedDate, turnoutRequestAllowedDate, personId, contactStatusId
    ) {
        this.firstName = firstName
        this.lastName = lastName
        this.middleName = middleName
        this.namePrefix = namePrefix
        this.nameSuffix = nameSuffix
        this.rating = rating
        this.phoneNumber = phoneNumber
        this.phoneType = phoneType
        this.doNotCallCount = doNotCallCount
        this.lastDoNotCallDate = lastDoNotCallDate
        this.address = address
        this.leastTime = leastTime
        this.lastContactAttemptTime = lastContactAttemptTime
        this.donationRequestAllowedDate = donationRequestAllowedDate
        this.persuasionAttemptAllowedDate = persuasionAttemptAllowedDate
        this.turnoutRequestAllowedDate = turnoutRequestAllowedDate
        this.personId = personId
        this.contactStatusId = contactStatusId
    }
}

module.exports = Contact