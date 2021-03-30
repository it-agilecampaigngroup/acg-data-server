const express = require('express')
const bodyParser = require('body-parser');
const app = express()
require('dotenv').config()

app.use(bodyParser.json())
//app.use(bodyParser.urlencoded({ extended: true }))

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,PATCH,HEAD,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'X-Requested-With, Content-type,Accept,X-Access-Token,X-Key,Authorization');
    res.header('Access-Control-Allow-Origin', '*');
    next()
})

//Import Routes
const contactRoutes = require('./routes/contacts');
app.use('/api/contacts', contactRoutes);

const contactResponseRoutes = require('./routes/contactResponses');
app.use('/api/contactresponse', contactResponseRoutes);

const reportRoutes = require('./routes/reports');
app.use('/api/reports', reportRoutes);

//====================================================================================
//    Start listening for requests
//====================================================================================
app.listen(process.env.VG_DATA_SERVER_PORT)

console.log(`Listening on port ${process.env.VG_DATA_SERVER_PORT}`)

