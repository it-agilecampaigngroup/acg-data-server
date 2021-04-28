const nodemailer = require('nodemailer');
const SMTPConnection = require('nodemailer/lib/smtp-connection');

//
// Email class
//
//  "from": "me@myemailer.com",
//  "to": "someone@somemailserver.com, someoneelse@someothermailserver.com",
//  "subject": "This is the subject line",
//  "text": "This is the plain text body of the message.",
//  "html": "<h2>You can include HTML instead of the plain text</h2>"
//
class Email {
    constructor(from, to, subject, text, html, attachments) {
        this.from = from
        this.to = to
        this.subject = subject
        this.text = (text === undefined) ? '' : text
        this.html = (html === undefined) ? '' : html
        this.attachments = attachments
    }
}

//===================================================================================
//
// Sends an email
//
// email: A fully populated Email object
//
//===================================================================================
function sendEmail(email) {
    //  Don't forget to add NODE_TLS_REJECT_UNAUTHORIZED=0 to.env for localhost tests

    var transporter = nodemailer.createTransport({
        host: process.env.MAIL_TRANSPORT_HOST
        , name: process.env.MAIL_TRANSPORT_NAME
        , port: process.env.MAIL_TRANSPORT_PORT
        , secure: (process.env.MAIL_TRANSPORT_SECURE_VAL === "false") ? false : true
        , auth: {
            user: process.env.MAIL_TRANSPORT_USER,
            pass: process.env.MAIL_TRANSPORT_PASSWORD
        }
        , tls: {                            
            rejectUnauthorized: (process.env.MAIL_TRANSPORT_TLS_REJECT_UNAUTH_VAL === "false") ? false : true
        }
    });

    // // Attempt to make this work with Microsoft:
    // var transporter = nodemailer.createTransport({
    //     host: 'acglogic-com.mail.protection.outlook.com'
    //     , port: 25
    //     , secure: false
    //     , auth: {
    //         user: 'rlarabee@acglogic.com'
    //         , pass: '************'
    //     }
    //     , tls: {                            
    //         cyphers: 'SSLv3'
    //     }
    //     , requireTLS: true
    // });

    transporter.verify((err, success) => {
        if (err) {
            throw new Error(`Error connecting to email server : ${err.message}`)
        } else {

            var the_email = {
                from: email.from,
                to: email.to,
                subject: email.subject,
                text: (email.text === undefined) ? '' : email.text,
                html: (email.html === undefined) ? '' : email.html,
                attachments : email.attachments
            };

            transporter.sendMail(the_email, function(error, info){
            if (error) {
                throw new Error(`Error sending email: ${error.message}`)
            } else {
                // All is well. We are done
            }
            }); 
        }
    });
}

module.exports = {sendEmail, Email}

// Other transporters. These haven't been made to work yet.

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

