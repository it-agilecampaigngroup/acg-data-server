'use strict'

class ContactResponse {
    constructor(contactAction, actor, contactId, contactReason, contactMethod, contactResult, detail) {
        this.timestamp = Date()
        this.contactAction = contactAction
        this.contactId = contactId
        this.clientId = actor.campaign.clientId
        this.campaignId = actor.campaign.campaignId
        this.contactReason = contactReason
        this.contactMethod = contactMethod
        this.contactResult = contactResult
        this.detail = detail
        this.actor = actor
    }
}

module.exports = ContactResponse