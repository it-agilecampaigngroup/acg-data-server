'use strict'

const jwt = require('jsonwebtoken');
require('dotenv').config();
const db = require('../db/db')

//====================================================================================
//    Authenticate a token
//====================================================================================
function authenticateToken(req, res, next) {
    // Get the token
    const token = getRequestHeaderAccessToken(req)

    if( token == null ) return res.sendStatus(401) // Token is invalid

    // Verify the token
    jwt.verify(token, process.env.ACCESS_TOKEN_SECRET, (err, decoded) => {
        if(err) return res.sendStatus(403) // Token is not valid
        next()
    })

}

//====================================================================================
//    Gets the access token from a request header
//====================================================================================
function getRequestHeaderAccessToken(req) {
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]
    return token
}

//====================================================================================
//    Decode a token
//====================================================================================
async function decodeToken(req, next) {

    // Get the token
    const token = getRequestHeaderAccessToken(req)

    if( token !== undefined ) {
        try {
            // Decode it
            const decoded = await jwt.decode(token)
            if( !decoded ) {
                throw new Error(`Provided token does not decode as JWT`);
            }
            return decoded
        }
        catch(err) {
            throw err
        }
    }
}

//====================================================================================
// Authenticates a user as a System Admin
//===================================================================================
function authenticateSystemAdmin(req, res, next) {
    
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]

    if( token == null ) return res.sendStatus(401) // Token is invalid

    jwt.verify(token, process.env.ACCESS_TOKEN_SECRET, (err, decoded) => {
        if (err) {
            res.status(500).send( {function: "utils.authenticateSystemAdmin", err:err} )
        } else {
            if( decoded.user.isSysAdmin ) {
                next()
            } else {
                res.sendStatus(401);
            }
            // if( isSysAdmin(decoded.user.username, req.body.clientId) ) {
            //     next()
            // } else {
            //     res.sendStatus(401);
            // }
        }
    })
}

// //====================================================================================
// // Authenticate a user as a Client Admin
// //===================================================================================
// function authenticateClientAdmin(req, res, next) {

//     const authHeader = req.headers['authorization']
//     const token = authHeader && authHeader.split(' ')[1]

//     if( token == null ) return res.sendStatus(401) // Token is invalid
    
//     jwt.verify(token, process.env.TOKEN_SECRET, (err, decoded) => {
//         if (err) {
//             res.status(500).send(err)
//         } else {
//             if( isClientAdmin(decoded.user.username, req.body.clientId)  ) {
//                 next()
//             } else {
//                 res.sendStatus(403);
//             }
//         }
//     })
// }

// //====================================================================================
// // Authenticate a user as a Campaign Manager
// //===================================================================================
// function authenticateCampaignMgr(req, res, next) {

//     const authHeader = req.headers['authorization']
//     const token = authHeader && authHeader.split(' ')[1]

//     if( token == null ) return res.sendStatus(401) // Token is invalid
    
//     jwt.verify(token, process.env.TOKEN_SECRET, (err, decoded) => {
//         if (err) {
//             res.status(500).send(err)
//         } else {
//             if( isCampaignManager(decoded.user.username, req.body.campaignId)  ) {
//                 next()
//             } else {
//                 res.sendStatus(403);
//             }
//         }
//     })
// }

//====================================================================================
// Checks if a user is a System Admin
//===================================================================================
async function isSysAdmin(username) {
    
    let select = "SELECT EXISTS(\r\n"
    select += "SELECT user_id\r\n"
    select += "FROM users.user\r\n"
    select += `WHERE username = ${username}\r\n`
    select += "AND is_sys_admin = true\r\n"
    select += " LIMIT 1) user_exists;\r\n"
    
    // For debugging
    //console.log(select)

    // Execute the query
    try {
        const res = await db.query(select)
        if( res.rows[0].user_exists ) return true
    } catch (err) {
        return false
    }
    return false
}

//====================================================================================
// Checks is a user is Client Admin
//===================================================================================
async function isClientAdmin(username, clientId) {
    
    // Sys Admins are always Client Admins
    let select = "SELECT EXISTS(\r\n"
    select += "SELECT user_id\r\n"
    select += "FROM users.user\r\n"
    select += `WHERE username = ${username}\r\n`
    select += "AND is_sys_admin = true\r\n"

    // And users in client_admin are too
    select += "UNION ALL\r\n"
    select += "SELECT usr.user_id\r\n"
    select += "FROM users.user usr\r\n"
    select += `INNER JOIN users.client_admin ca ON ca.user_id = usr.user_id AND ca.client_id = ${clientId}\r\n`
    select += `WHERE usr.username = ${username}\r\n`
    select += "LIMIT 1\r\n"
    select += ") user_exists;\r\n"
    
    // For debugging
    //console.log(select)

    // Execute the query
    try {
        const res = await db.query(select)
        if( res.rows[0].user_exists ) return true
    } catch (err) {
        return false
    }
    return false
}

//====================================================================================
// Checks is a user is Campaign Manager
//===================================================================================
async function isCampaignMgr(username, campaignId) {  //<-- Might have to change this to campaignName
    
    // Sys Admins are always Campaign Managers
    let select = "SELECT EXISTS(\r\n"
    select += "SELECT user_id\r\n"
    select += "FROM users.user\r\n"
    select += `WHERE username = '${username}'\r\n`
    select += "AND is_sys_admin = true\r\n"
    
    // Client Admins are also Campaign Managers for their campaigns
    select += "UNION ALL\r\n"
    select += "SELECT usr.user_id\r\n"
    select += "FROM users.user usr\r\n"
    select += `INNER JOIN users.campaign campaign ON campaign.campaign_id = ${campaignId}\r\n`
    select += "INNER JOIN users.client_admin ca ON ca.client_id = campaign.client_id AND ca.user_id = usr.user_id\r\n"
    select += `WHERE usr.username = '${username}'\r\n`
    
    // And finally Actors that are marked as Camapign Managers
    select += "UNION ALL\r\n"
    select += "SELECT usr.user_id\r\n"
    select += "FROM users.user usr\r\n"
    select += `INNER JOIN users.actor act ON act.user_id = usr.user_id AND act.campaign_id = ${campaignId}\r\n`
    select += `WHERE usr.username = '${username}'\r\n`
    select += "AND act.is_campaign_manager = true\r\n"
    select += "LIMIT 1) user_exists;\r\n"
    
    // For debugging
    //console.log(select)

    // Execute the query
    try {
        const res = await db.query(select)
        if( res.rows[0].user_exists ) return true
    } catch (err) {
        return false
    }
    return false
}

module.exports = {
    authenticateToken
    , getRequestHeaderAccessToken
    , decodeToken
//    , authenticateSystemAdmin
//    , authenticateClientAdmin
}
