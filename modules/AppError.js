'use strict'

class AppError {
    constructor(app, module, process, description, error) {
        this.app = app
        this.module = module
        this.process = process
        this.description = (description == undefined) ? "" : description
        this.timestamp = Date()
        this.error = error.stack
    }
}

module.exports = AppError