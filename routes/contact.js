'use strict'

const express = require('express')
const router = express.Router()
const utils = require('./utils') // Provides authenticateToken and authenticateAdmin
const contact = require('../db/contact')

//====================================================================================
//  Get a random user
//====================================================================================
router.get('/', utils.authenticateToken, async (req, res) => {
//    router.get('/', async (req, res) => {
    try {
        return res.status(200).send(await contact.getRandomPerson())
    } catch(e) {
        console.log(e)
        throw e
    }
})

// //====================================================================================
// //  Get a specific user
// //====================================================================================
// router.get('/:username', utils.authenticateToken, async (req, res) => {

//     try {
//         const result = await users.findUser(req.params.username)
//         if( result != undefined ) return res.status(200).send( result )

//         return res.status(404).send( `No user was found with username '${req.params.username}'`)
        
//     } catch(e) {
//         console.log(e)
//         throw e
//     }
    
// })

module.exports = router