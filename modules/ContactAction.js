'use strict'

class ContactAction {
    constructor(contactAction, actor, contactReason, contactMethod, contactResult, detail) {
        this.contactAction = contactAction
        this.timestamp = Date()
        this.clientId = actor.campaign.client.clientId
        this.campaignId = actor.campaignId
        this.actor = actor
        this.contactReason = contactReason
        this.contactMethod = contactMethod
        this.contactResult = contactResult
        this.detail = detail
    }
}

module.exports = ContactAction