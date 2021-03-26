'use strict'

const db = require('./db')
const AppError = require('../modules/AppError')
const msging = require('../messaging/kafka')

module.exports = {

    //====================================================================================
    // Record an application error
    //
    // error: A fully populated AppError object
    //
    // Returns an object with the following properties:
    //      statusCode - an HTTP status code value
    //      err - an Error object, if an error occurs
    // 
    //===================================================================================
    async recordAppError(error) {

        let insert = "INSERT INTO base.app_error_log (app, app_module, process, description, error)\r\n"
        insert += `VALUES ('${db.formatTextForSQL(error.app)}'\r\n`
        insert += `, '${db.formatTextForSQL(error.module)}'\r\n`
        insert += `, '${db.formatTextForSQL(error.process)}'\r\n`
        insert += `, '${db.formatTextForSQL(error.description)}'\r\n`
        if( error.error != "" ) {
            insert += `, '${JSON.stringify(error.error)}'`
        } else {
            insert += ", NULL"
        }
        insert += ");"
           
        // For debugging
        //console.log(insert)

        try {
            await db.query(insert)
        } catch (e) {
            msging.sendAppErrorsMsg(new AppError('data-server', 'errorRecorder', 'recordAppError', 'Database error recording application error', e))
            return { statusCode: 500, err: new Error(`Database error recording application error:\r\n${e.message}`) }
        }

        try {
            // Send an AppErrors message
            msging.sendAppErrorsMsg(error)
        } catch(e) {
            return { statusCode: 500, err: new Error(`Error sending AppErrors message:\r\n${e.message}`) }
        }

        return {statusCode: 200}
    },

} // End of module.exports