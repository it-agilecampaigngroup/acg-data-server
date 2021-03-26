'use strict'

require('dotenv').config();
const jwt = require('jsonwebtoken');

var token = undefined

module.exports = async () => {
        if( token == undefined ) {
            token = await jwt.sign({user: {username: 'data_server', isSysAdmin: true}}, process.env.ACCESS_TOKEN_SECRET)
        }
        return token
    }
 