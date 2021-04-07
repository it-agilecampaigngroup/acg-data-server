'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

//====================================================================================
// Support Result Summary Report
//
// Returns the counts of each support result rating in a specified date range for
// the campaign or an actor.
// 
//====================================================================================
router.get('/SupportResultSummary', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /SupportResultSummary', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // The requesting actor must be a Campaign Manager if the report
        // is not for a specific actor or it's for some other actor
        var targetActorId = (req.query.actorId === undefined ) ? 0 : parseInt(req.query.actorId)
        if( actor.isCampaignMgr === false ) {
            if( targetActorId !== actor.actorId ) {
               return res.status(401).send("Forbidden: You must be a Campaign Manager to run this report for the campaign or another actor")
            }
        }

        // Prepare the arguments
        var campaignId = parseInt(req.query.campaignId)
        campaignId = (isNaN(campaignId)) ? actor.campaignId : campaignId
        var dateStart = (req.query.dateStart === undefined) ? new Date("1/1/2021") : req.query.dateStart
        dateStart = new Date(dateStart).toLocaleDateString()
        var dateEnd = (req.query.dateEnd === undefined) ? new Date() : req.query.dateEnd
        dateEnd = new Date(dateEnd).toLocaleDateString()

        // Generate and return the report
        try {
            const report = require('../reports/SupportResultSummary')
            return res.status(200).send( await report(campaignId, targetActorId, dateStart, dateEnd) )
        } catch (e) {
            return res.status(400).send(`Error generating Support Result Summary report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportResultSummary', 'Unknown error in GET /SupportResultSummary', e))
        throw e
    }
});

//====================================================================================
//
// Support by Race report
//
// Returns the support by race based on the Support Results values recorded
// in the action log.
// 
//====================================================================================
router.get('/SupportByRace', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /SupportByRace', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByRace')
            return res.status(200).send( await report() )
        } catch (e) {
            return res.status(400).send(`Error generating Support by Race report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByRace', 'Unknown error in GET /SupportByRace', e))
        throw e
    }
});

//====================================================================================
//
// Support by Income report
//
// Returns the support by income based on the Support Results values recorded
// in the action log.
// 
//====================================================================================
router.get('/SupportByIncome', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /SupportByIncome', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByIncome')
            return res.status(200).send( await report() )
        } catch (e) {
            return res.status(400).send(`Error generating Support by Income report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByIncome', 'Unknown error in GET /SupportByIncome', e))
        throw e
    }
});

//====================================================================================
//
// Support by Education report
//
// Returns the support by education based on the Support Results values recorded
// in the action log.
// 
//====================================================================================
router.get('/SupportByEducation', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /SupportByEducation', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByEducation')
            return res.status(200).send( await report() )
        } catch (e) {
            return res.status(400).send(`Error generating Support by Education report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByEducation', 'Unknown error in GET /SupportByEducation', e))
        throw e
    }
});

//====================================================================================
// DonationCallsToDate report
//
// Returns a summary of the contact actions for an actor or a campaign
// 
//====================================================================================
router.get('/DonationCallsToDate', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /DonationCallsToDate', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // The requesting actor must be a Campaign Manager if the report
        // is not for a specific actor or it's for some other actor
        var targetActorId = (req.query.actorId === undefined ) ? 0 : parseInt(req.query.actorId)
        if( actor.isCampaignMgr === false ) {
            if( targetActorId !== actor.actorId ) {
               return res.status(401).send("Forbidden: You must be a Campaign Manager to run this report for the campaign or another actor")
            }
        }

        // Prepare the arguments
        var campaignId = parseInt(req.query.campaignId)
        campaignId = (isNaN(campaignId)) ? actor.campaignId : campaignId
        var dateStart = (req.query.dateStart === undefined) ? new Date("1/1/2021") : req.query.dateStart
        dateStart = new Date(dateStart).toLocaleDateString()
        var dateEnd = (req.query.dateEnd === undefined) ? new Date() : req.query.dateEnd
        dateEnd = new Date(dateEnd).toLocaleDateString()

        // Generate and return the report
        try {
            const report = require('../reports/DonationCallsToDate')
            return res.status(200).send( await report(campaignId, targetActorId, dateStart, dateEnd) )
        } catch (e) {
            return res.status(400).send(`Error generating Donation Calls To Date report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationCallsToDate', 'Unknown error in GET /DonationCallsToDate', e))
        throw e
    }
});

//====================================================================================
// Contact Action Log report
//
// Returns the Contact Action Logs for and actor or a campaign
// 
//====================================================================================
router.get('/ContactActionLog', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /ContactActionLog', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // The requesting actor must be a Campaign Manager if the report
        // is not for a specific actor or it's for some other actor
        var targetActorId = (req.query.actorId === undefined ) ? 0 : parseInt(req.query.actorId)
        if( actor.isCampaignMgr === false ) {
            if( targetActorId !== actor.actorId ) {
               return res.status(401).send("Forbidden: You must be a Campaign Manager to run this report for the campaign or another actor")
            }
        }

        // Prepare the arguments
        var campaignId = parseInt(req.query.campaignId)
        campaignId = (isNaN(campaignId)) ? actor.campaignId : campaignId
        var dateStart = (req.query.dateStart === undefined) ? new Date("1/1/2021") : req.query.dateStart
        dateStart = new Date(dateStart).toLocaleDateString()
        var dateEnd = (req.query.dateEnd === undefined) ? new Date() : req.query.dateEnd
        dateEnd = new Date(dateEnd).toLocaleDateString()

        // Generate and return the report
        try {
            const report = require('../reports/ContactActionLog')
            return res.status(200).send( await report(campaignId, targetActorId, dateStart, dateEnd) )
        } catch (e) {
            return res.status(400).send(`Error generating Contact Action Log report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ContactActionLog', 'Unknown error in GET /ContactActionLog', e))
        throw e
    }
});

//====================================================================================
//
// Person Contact History report
//
// Returns the Contact Action Log response records for a specific person
// 
//====================================================================================
router.get('/PersonContactHistory', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /PersonContactHistory', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Prepare the arguments
        var personId = parseInt(req.query.personId)

        // Generate and return the report
        try {
            const report = require('../reports/PersonContactHistory')
            return res.status(200).send( await report(personId) )
        } catch (e) {
            return res.status(400).send(`Error generating Person Contact History report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /PersonContactHistory', 'Unknown error in GET /PersonContactHistory', e))
        throw e
    }
});

module.exports = router
