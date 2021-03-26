'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const campaign = require('../modules/Campaign')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const dbContacts = require('../db/contacts')

//====================================================================================
//  Get a random person
//====================================================================================
router.get('/', utils.authenticateToken, async(req, res) => {
//router.get('/', async(req, res) => {  

    try {
        // Get the actor making the request
        var actor
        await getActor(req)
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
            let contact = await dbContacts.getBestContact(actor, campaign, req.body.contactReason, req.body.contactMethod)
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

//==========================================================================
// Private functions
//==========================================================================
async function isActorBlocked(req) {
    
    // Decode the token
    const decoded = await utils.decodeToken(req)
    if( decoded === undefined ) {
        let e = new Error('Unable to decode token')
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /', 'Unable to decode token', e))
        throw e
    }

    // Get the actor's status
    try {
        const isBlocked = await campaign.isActorBlocked(decoded.user.username)
        //if( isBlocked ) { console.log('True') } else { console.log('False') }
        //console.log(isBlocked)
        return isBlocked
    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contacts.js', 'GET /', 'Error checking if actor is blocked', e))
        throw e
    }
}

const getActor = (req) => {
    return new Promise(async (resolve, reject) => {
        // Decode the token
        const decoded = await utils.decodeToken(req)
        if( decoded === undefined ) {
            let e = new Error('Unable to decode token')
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /', 'Unable to decode token', e))
            reject(e)
        }

        // Get the actor
        try {
            campaign.getActorByUsername(decoded.user.username, (actor) => {
                resolve(actor)
            })

        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contacts.js', 'getActor', 'Error getting actor by username', e))
            reject(e)
        }

    })
}

