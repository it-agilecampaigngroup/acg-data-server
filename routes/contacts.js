'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const campaign = require('../modules/Campaign')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const dbContacts = require('../db/contacts')

//====================================================================================
//  Get a contact
//====================================================================================
router.get('/', utils.authenticateToken, async(req, res) => {
//router.get('/', async(req, res) => {  
    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /', 'Unknown error in GET /', e))
            throw e
        })

        // // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Return a contact
        try {
            let contact = await dbContacts.getBestContact(actor, req.query.contactReason, req.query.contactMethod)
            return res.status(200).send(contact)
        } catch (e) {
            return res.status(400).send(`Error retrieving contact: ${e.message}`)
        }
        
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /', 'Unknown error in GET /', e))
        throw e
    }
});

module.exports = router
