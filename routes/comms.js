'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')

//====================================================================================
//    Send an email
//====================================================================================
router.post('/SendEmail', utils.authenticateToken, async(req, res) => {
    // router.post('/SendEmail', async(req, res) => {  ///// For tests only !
    //     console.log("Don't forget to re-enable tokens for /SendEmail before deploying to production!")
    //  Don't forget to add NODE_TLS_REJECT_UNAUTHORIZED=0 to.env for localhost tests
        
        var nodemailer = require('nodemailer');

        var transporter = nodemailer.createTransport({
            host: process.env.MAIL_TRANSPORT_HOST
            , name: process.env.MAIL_TRANSPORT_NAME
            , port: process.env.MAIL_TRANSPORT_PORT
            , secure: process.env.MAIL_TRANSPORT_SECURE_VAL
            , auth: {
                user: process.env.MAIL_TRANSPORT_USER,
                pass: process.env.MAIL_TRANSPORT_PASSWORD
            }
            , tls: {
                rejectUnauthorized: process.env.MAIL_TRANSPORT_TLS_REJECT_UNAUTH_VAL
            }
        });

        // var username = 'it@agilecampaigngroup.com'
        // var password = 'MF9[45KR_Njq'
        // var transporter = nodemailer.createTransport({
        //     host: 'mail.agilecampaigngroup.com' // Bluehost server
        //     , name: 'mail.agilecampaigngroup.com' // Bluehost server
        //     , port: 465     // secure SMTP
        //     , secure: true // false for TLS
        //     , auth: {
        //         user: username,
        //         pass: password
        //     }
        //     , tls: {
        //         rejectUnauthorized: false
        //     }
        // });

        transporter.verify((err, success) => {
            if (err) {
              console.log(err);
              ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'POST /SendEmail', 'Error connecting to email server', err))
              return res.status(400).send(`Error connecting to email server : ${err.message}`)
            } else {
    
                var mailOptions = {
                    from: req.body.from,
                    to: req.body.to,
                    subject: req.body.subject,
                    text: (req.body.text === undefined) ? '' : req.body.text,
                    html: (req.body.html === undefined) ? '' : req.body.html
                };
    
                transporter.sendMail(mailOptions, function(error, info){
                if (error) {
                    console.log(error);
                    ErrorRecorder.recordAppError(new AppError('data-server', 'routes/reports.js', 'POST /SendEmail', 'Error sending email', error))
                    return res.status(400).send(`Error sending email: ${error.message}`)
                } else {
                    console.log('Email sent: ' + info.response);
                    return res.status(200).send("Email message was sent without error")
                }
                }); 
            }
        });
    
        // Other transporters. These haven't been made to work so far.
    
        // var transporter = nodemailer.createTransport({
        //     host: 'smtp.office365.com' // Office 365 server
        //     , port: 587     // secure SMTP
        //     , secure: false // false for TLS - as a boolean not string - but the default is false so just remove this completely
        //     , auth: {
        //         user: username,
        //         pass: password
        //     }
        //     , tls: {
        //         ciphers: 'SSLv3'
        //     }
        // });
    
    });

module.exports = router
    
