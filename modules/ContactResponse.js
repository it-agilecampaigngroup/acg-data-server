'use strict'

class ContactResponse {
    constructor(contactAction, campaignId, actor, contactReason, contactMethod, contactResult, detail) {
        this.timestamp = Date()
        this.contactAction = contactAction
        this.clientId = actor.campaign.clientId
        this.campaignId = campaignId
        this.contactReason = contactReason
        this.contactMethod = contactMethod
        this.contactResult = contactResult
        this.detail = detail
        this.actor = actor
    }
}

module.exports = ContactResponse