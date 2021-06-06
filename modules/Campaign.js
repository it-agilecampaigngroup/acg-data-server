'use strict'

require('dotenv').config();
const secdb = require('../db/secdb')
const AppError = require('../modules/AppError')
const ErrorRecorder = require('../db/errorRecorder')
const axios = require('axios')
const token = require('./Token')

let campaign = getCampaign(process.env.VG_DATA_SERVER_CAMPAIGN_ID)

module.exports = {

    //*********************************************************
    //
    // Properties
    //
    //*********************************************************
    get campaignId() {return campaign.campaignId},
    get name() {return campaign.name},
    get client() {return campaign.client},

    //*********************************************************
    //
    // Functions
    //
    //*********************************************************

    //=========================================================
    // Detects if a user is blocked in this campaign
    //
    // username: The actor's username
    //
    // Returns true if the actor is blocked
    //
    //==========================================================
    async isActorBlocked(username) {
        // Build the SQL SELECT stmt
        let select = "SELECT actor.is_blocked\r\n"
        select += "FROM users.actor actor\r\n"
        select += "INNER JOIN users.user usr ON usr.user_id = actor.user_id\r\n"
        select += "INNER JOIN users.campaign cmpn ON cmpn.campaign_id = actor.campaign_id\r\n"
        select += `WHERE cmpn.campaign_id = ${campaign.campaignId}\r\n`
        select += `AND usr.username = '${username}';`
        
        // For debugging
        //console.log(select)
    
        // Execute the query and return the client
        try {

            const dbres = await secdb.query(select)
            if( dbres.rowCount > 0) {
                if( dbres.rows[0].is_blocked != 0 ) { 
                    return true 
                }
                return false
            }
            return undefined
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'modules/Campaign.js', 'isUserBlocked', 'Database error checking if actor is blocked', e))
            throw new Error(e.message)
        }
    },

    async getActorByUsername(username, next) {

        let actorId = await this.getActorIdByUsername(username)

        this.getActor(actorId, (actor) => {
            next(actor)
        })
    },

    async getActor(actorId, next) {
        await axios
        .get(`https://vanguardcontact.com/sapi/actors/${actorId}`, {
            headers: {
                'Authorization': `Bearer ${await token()}`
            }
        })
        .then(res => {
            next(res.data)
        })
        .catch(e => {
            throw e
        })
    }, 

    async getManagedActors(managerId, next) {
        await axios
        .get(`http://vanguardcontact.com/sapi/reports/ManagedActorList?campaignId=${this.campaignId}&managerId=${managerId}`, {
            headers: {
                'Authorization': `Bearer ${await token()}`
            }
        })
        .then(res => {
            next(res.data)
        })
        .catch(e => {
            throw e
        })
    }, 

    //=========================================================
    // Retrieves and actor's id based on their username
    //
    // username: The actor's username
    //
    // Returns the actor's ID, if found
    //
    //==========================================================
    async getActorIdByUsername(username) {
        // Build the SQL SELECT stmt
        let select = "SELECT actor.actor_id\r\n"
        select += "FROM users.actor actor\r\n"
        select += "INNER JOIN users.user usr ON usr.user_id = actor.user_id\r\n"
        select += "INNER JOIN users.campaign cmpn ON cmpn.campaign_id = actor.campaign_id\r\n"
        select += `WHERE cmpn.campaign_id = ${campaign.campaignId}\r\n`
        select += `AND usr.username = '${username}';`
        
        // For debugging
        //console.log(select)
    
        // Execute the query and return the client
        try {

            const dbres = await secdb.query(select)
            if( dbres.rowCount > 0) {
                return dbres.rows[0].actor_id
            }
            return undefined
        } catch(e) {
            ErrorRecorder.recordAppError(new AppError('data-server', 'modules/Campaign.js', 'getActorIdFromUsername', "Database error looking up actor's ID by username", e))
            throw new Error(e.message)
        }
    },



} // End of module.exports

//***************************************************************************
//
// Private functions
//
//***************************************************************************
async function getCampaign(campaignId) {
   
    await axios
    .get(`https://vanguardcontact.com/sapi/campaigns/${campaignId}`, {
        headers: {
            'Authorization': `Bearer ${await token()}`
        }
    })
    .then(res => {
        res.data.campaignId = campaignId
        campaign = res.data
    })
    .catch(e => {
        throw e
    })
    
    return undefined
}





