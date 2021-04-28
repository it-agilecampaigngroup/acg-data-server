'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const campaign = require('../modules/Campaign')

const getActor = (actorId) => {
    return new Promise(async (resolve, reject) => {
        // Get the actor
        try {
            campaign.getActor(actorId, (actor) => {
                resolve(actor)
            })
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'getActor', 'Error getting actor by Id', e))
            reject(e)
        }

    })
}

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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportResultSummary', 'Unknown error in GET /', e))
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
        if( isNaN(campaignId) ) {
            if( targetActorId != 0) {
                var targetActor = await getActor(targetActorId)
                campaignId = targetActor.campaignId
            } else {
                campaignId = actor.campaignId
            }
        }
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByRace', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByRace')
            return res.status(200).send( await report(actor.campaignId) )
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByIncome', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByIncome')
            return res.status(200).send( await report(actor.campaignId) )
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByEducation', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/SupportByEducation')
            return res.status(200).send( await report(actor.campaignId) )
        } catch (e) {
            return res.status(400).send(`Error generating Support by Education report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportByEducation', 'Unknown error in GET /SupportByEducation', e))
        throw e
    }
});

//====================================================================================
//
// Donations Summary report
//
// Returns a summary of a campaign's donations
// 
//====================================================================================
router.get('/DonationSummary', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationSummary', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/DonationSummary')
            return res.status(200).send( await report(actor.campaignId, req.query.lowDonationAmount, req.query.donationLimitAmount, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Donation Summary report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationSummary', 'Unknown error in GET /DonationSummary', e))
        throw e
    }
});

//====================================================================================
//
// Large Donation Donors report
//
// Returns a list of donors that have made large contributions
// 
//====================================================================================
router.get('/LargeDonationDonors', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /LargeDonationDonors', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/LargeDonationDonors')
            return res.status(200).send( await report(actor.campaignId, req.query.largeDonationThreshold, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Large Donation Donors report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /LargeDonationDonors', 'Unknown error in GET /LargeDonationDonors', e))
        throw e
    }
});

//====================================================================================
//
// Recurring Donations report
//
// Returns the list of donations that are marked as "recurring"
// 
//====================================================================================
router.get('/RecurringDonations', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /RecurringDonations', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/RecurringDonations')
            return res.status(200).send( await report(actor.campaignId, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Recurring Donations report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /RecurringDonations', 'Unknown error in GET /RecurringDonations', e))
        throw e
    }
});

//====================================================================================
//
// Donations by Race report
//
// Returns donation information categorized by race. Informatiom includes
// total donations, average donations, and the number of "yes" and "no" responses.
// 
//====================================================================================
router.get('/DonationByRace', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByRace', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/DonationByRace')
            return res.status(200).send( await report(actor.campaignId, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Donation by Race report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByRace', 'Unknown error in GET /DonationByRace', e))
        throw e
    }
});

//====================================================================================
//
// Donations by Income report
//
// Returns donation information categorized by income level. Informatiom includes
// total donations, average donations, and the number of "yes" and "no" responses.
// 
//====================================================================================
router.get('/DonationByIncome', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByIncome', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/DonationByIncome')
            return res.status(200).send( await report(actor.campaignId, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Donation by Income report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByIncome', 'Unknown error in GET /DonationByIncome', e))
        throw e
    }
});

//====================================================================================
//
// Donations by Education report
//
// Returns donation information categorized by education level. Informatiom includes
// total donations, average donations, and the number of "yes" and "no" responses.
// 
//====================================================================================
router.get('/DonationByEducation', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByEducation', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/DonationByEducation')
            return res.status(200).send( await report(actor.campaignId, req.query.contactMethod) )
        } catch (e) {
            return res.status(400).send(`Error generating Donation by Education report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationByEducation', 'Unknown error in GET /DonationByEducation', e))
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /DonationCallsToDate', 'Unknown error in GET /', e))
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
        if( isNaN(campaignId) ) {
            if( targetActorId != 0) {
                var targetActor = await getActor(targetActorId)
                campaignId = targetActor.campaignId
            } else {
                campaignId = actor.campaignId
            }
        }
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ContactActionLog', 'Unknown error in GET /', e))
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
        if( isNaN(campaignId) ) {
            if( targetActorId != 0) {
                var targetActor = await getActor(targetActorId)
                campaignId = targetActor.campaignId
            } else {
                campaignId = actor.campaignId
            }
        }
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
// Contact Response List report
//
// Returns or emails a report detailing contacts' responses
// 
//====================================================================================
router.get('/ContactResponseList', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ContactResponseList', 'Unknown error in GET /', e))
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
        if( isNaN(campaignId) ) {
            if( targetActorId != 0) {
                var targetActor = await getActor(targetActorId)
                campaignId = targetActor.campaignId
            } else {
                campaignId = actor.campaignId
            }
        }
        var dateStart = (req.query.dateStart === undefined) ? new Date("1/1/2021") : req.query.dateStart
        dateStart = new Date(dateStart).toLocaleDateString()
        var dateEnd = (req.query.dateEnd === undefined) ? new Date() : req.query.dateEnd
        dateEnd = new Date(dateEnd).toLocaleDateString()

        // Generate and return the report
        try {
            const report = require('../reports/ContactResponseList')
            const json = await report(campaignId, targetActorId, dateStart, dateEnd)
            if( req.query.email === undefined ) {
                // If no email address was specified then just return the JSON
                return res.status(200).send( json )
            } else {
                // Convert the JSON to CSV and email it
                const { parseAsync } = require('json2csv');

                const fields = [
                    { label: 'Date', value: 'date' }
                    ,{ label: 'Time', value: 'time' } 
                    , { label: 'Actor', value: 'actor' }
                    , { label: 'Contact Reason', value: 'contactReason' }
                    , { label: 'Contact Method', value: 'contactMethod' }
                    , { label: 'Call Result', value: 'contactResult' }
                    , { label: 'Voter Id', value: 'voterId' }
                    , { label: 'First Name', value:  'firstName' }
                    , { label: 'Last Name', value:  'lastName' }
                    , { label: 'Street', value:  'street' }
                    , { label: 'City', value:  'city' }
                    , { label: 'State', value:  'state' }
                    , { label: 'Zip', value:  'zip' }
                    , { label: 'Email', value:  'email' }
                    , { label: 'Phone Number', value:  'phoneNumber' }
                    , { label: 'Donation Amount', value:  'donationAmount' }
                    , { label: 'Donation Is Recurring', value:  'donationIsRecurring' }
                    , { label: 'Callback Date', value:  'callbackDate' }
                    , { label: 'Callback Time', value:  'callbackTime' }
                    , { label: 'Support Result', value:  'supportResult' }
                    , { label: 'Value Result', value:  'valueResult' }
                    , { label: 'Note', value:  'note' }
                ]
                const opts = { fields };
                
                parseAsync(json, opts)
                  .then(csv => {
                    // Email the CSV
                    const emailer = require ('../messaging/emailer')
                    const email = new emailer.Email(
                        'donotreply@acglogic.com'
                        , req.query.email
                        , 'Contact Response List'
                        , 'Contact Response List email test'
                        , undefined
                        , [{filename: 'ContactResponseList.csv', content: csv}]
                        )
                    emailer.sendEmail(email)
                    return res.status(200).send( `Contact Response List emailed to ${req.query.email}` )
                  })
                  .catch(e => {
                    ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ContactResponseList', 'Error sending email in GET /ContactResponseList', e))
                    return res.status(400).send(`Error emailing Contact Response List report: ${e.message}`)
                  });                
            }
            
        } catch (e) {
            return res.status(400).send(`Error generating Contact Response List report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ContactResponseList', 'Unknown error in GET /ContactResponseList', e))
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /PersonContactHistory', 'Unknown error in GET /', e))
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

//====================================================================================
//
// Actor Callback List report
//
// Returns an actor's list of scheduled callbacks
// 
//====================================================================================
router.get('/ActorCallbackList', utils.authenticateToken, async(req, res) => {

    try {
        // Get the actor making the request
        var actor
        await utils.getActor(req)
        .then( got_actor => {
            actor = got_actor
        })
        .catch( (e) => {
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ActorCallbackList', 'Unknown error in GET /', e))
            throw e
        })

        // Make sure user has not been blocked
        if( actor.isBlocked === true ) {
            return res.status(401).send("Forbidden: Actor has been blocked")
        }

        // Generate and return the report
        try {
            const report = require('../reports/ActorCallbackList')
            return res.status(200).send( await report(actor.actorId) )
        } catch (e) {
            return res.status(400).send(`Error generating Actor Callback List report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /ActorCallbackList', 'Unknown error in GET /ActorCallbackList', e))
        throw e
    }
});

module.exports = router
