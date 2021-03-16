const express = require('express')
const app = express()
require('dotenv').config()

//app.use(cors());

//Import Routes
const contactRoutes = require('./routes/contact');
app.use('/api/contact', contactRoutes);

//====================================================================================
//    Start listening for requests
//====================================================================================
app.listen(process.env.VG_DATA_SERVER_PORT)

console.log(`Listening on port ${process.env.VG_DATA_SERVER_PORT}`)
