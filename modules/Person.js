'use strict'

class Person {
    constructor(firstName, lastName, middleName, namePrefix, NameSuffix, address = null, phones = []) {
        this.firstName = ''
        this.lastName = ''
        this.middleName = ''
        this.prefix = ''
        this.suffix = ''
        this.address = address
        this.phones = phones
    }
}

module.exports = Person