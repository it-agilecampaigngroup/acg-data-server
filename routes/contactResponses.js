'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const ContactResponse = require('../modules/ContactResponse')
const dbResponses = require('../db/contactResponses')
const dbContacts = require('../db/contacts')

//====================================================================================
//  Process a posted contact response
//
// Returns a new contact (unless the actor is blocked)
//====================================================================================
router.post('/', utils.authenticateToken, async(req, res) => {
    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contactResponses.js', 'Post /', 'Unknown error in GET /', e))
            throw e
        })

        // Translate the request into a ContactResponse object
        const response = new ContactResponse(
            req.body.action
            , actor
            , req.body.contactId
            , req.body.contactReason
            , req.body.contactMethod
            , req.body.contactResult
            , req.body.detail
        )

        //await dbResponses.processContactResponse(response)

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Return a contact
        try {
            let contact = await dbContacts.getBestContact(actor, req.body.contactReason, req.body.contactMethod)
            return res.status(200).send(contact)
        } catch (e) {
            return res.status(400).send(`Error retrieving contact: ${e.message}`)
        }
        
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contactResponses.js', 'POST /', 'Unknown error in POST /', e))
        throw e
    }
});

module.exports = router

