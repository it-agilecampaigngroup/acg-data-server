'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

router.post('/SendEmail', async(req, res) => {
    var nodemailer = require('nodemailer');

    var username = 'rlarabee@acglogic.com'
    var password = 'Iam[6445]'

    var transporter = nodemailer.createTransport({
        host: 'smtp.office365.com', // Office 365 server
        port: 587,     // secure SMTP
        secure: false, // false for TLS - as a boolean not string - but the default is false so just remove this completely
        auth: {
            user: username,
            pass: password
        },
        tls: {
            ciphers: 'SSLv3'
        }
    });
    // var transporter = nodemailer.createTransport({
    //   service: 'gmail',
    //   auth: {
    //     user: 'youremail@gmail.com',
    //     pass: 'yourpassword'
    //   }
    // });

    var mailOptions = {
    from: 'rlarabee@acglogic.com',
    to: 'drew@acglogic.com, rlarabee@hotmail.com',
    subject: 'This is a message from our data server',
    text: 'What do you think?'
    };

    transporter.sendMail(mailOptions, function(error, info){
    if (error) {
        console.log(error);
    } else {
        console.log('Email sent: ' + info.response);
    }
    }); 

})

//====================================================================================
// Support Result Summary Report
//
// Returns the counts of each support result rating in a specified date range for
// the campaign or an actor.
//
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
            ErrorRecorder.recordAppError(new AppError('data-server', 'routes/contact.js', 'GET /', 'Unknown error in GET /', e))
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
            const report = require('../reports/SupportResultsummary')
            return res.status(200).send( await report(campaignId, targetActorId, dateStart, dateEnd) )
        } catch (e) {
            return res.status(400).send(`Error generating Support Result Summary report: ${e.message}`)
        }

    } catch(e) {
        ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'GET /SupportResultSummary', 'Unknown error in GET /SupportResultSummary', e))
        throw e
    }
});

module.exports = router
