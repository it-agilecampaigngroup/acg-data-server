'use strict'

class ContactResponse {
    constructor(action, actor, contactId, reason, method, result, detail = {}) {
        this.timestamp = Date()
        this.action = action
        this.contactId = contactId
        this.clientId = actor.campaign.clientId
        this.campaignId = actor.campaign.campaignId
        this.reason = reason
        this.method = method
        this.result = result
        this.detail = detail
        this.actor = actor
    }
}

module.exports = ContactResponse