'use strict'

class Person {
    constructor(firstName, lastName, middleName, namePrefix, nameSuffix, address = null, phones = []) {
        this.firstName = (firstName == undefined) ? "" : firstName
        this.lastName = (lastName == undefined) ? "" : lastName
        this.middleName = (middleName == undefined) ? "" : middleName
        this.prefix = (namePrefix == undefined) ? "" : namePrefix
        this.suffix = (nameSuffix == undefined) ? "" : nameSuffix
        this.address = address
        this.phones = phones
    }
}

module.exports = Person