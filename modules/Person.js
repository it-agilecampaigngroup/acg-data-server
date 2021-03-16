'use strict'

class Person {
    constructor(firstName, lastName, middleName, namePrefix, NameSuffix, address = null, phones = []) {
        this.firstName = firstName
        this.lastName = lastName
        this.middleName = middleName
        this.prefix = namePrefix
        this.suffix = nameSuffix
        this.address = address
        this.phones = phones
    }
}

module.exports = Person