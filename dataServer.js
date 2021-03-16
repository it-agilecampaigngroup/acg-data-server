const express = require('express')
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express()
require('dotenv').config()

// app.use(bodyParser.urlencoded({ extended: true }))
// app.use(bodyParser.json())

// //Middleware
// app.use(
//     cors({
//         origin: "*"
//     })
// );

app.use(cors());

// app.use((req, res, next) => {
//     res.header('Access-Control-Allow-Origin', '*');
//     next();
//   });

//Import Routes
const contactRoutes = require('./routes/contact');
app.use('/api/contact', contactRoutes);

// const donationRoutes = require('./routes/donation');
// app.use('/api/donation', donationRoutes);

// const persuasionRoutes = require('./routes/persuasion');
// app.use('/api/persuasion', persuasionRoutes);

// const turnoutRoutes = require('./routes/turnout');
// app.use('/api/turnout', turnoutRoutes);

//====================================================================================
//    Start listening for requests
//====================================================================================
app.listen(process.env.VG_DATA_SERVER_PORT)

console.log(`Listening on port ${process.env.VG_DATA_SERVER_PORT}`)
