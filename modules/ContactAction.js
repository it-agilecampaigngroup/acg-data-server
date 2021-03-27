'use strict'

class ContactAction {
    constructor(action, actor, reason, method, result, detail = "") {
        this.action = action
        this.timestamp = Date()
        this.clientId = actor.campaign.client.clientId
        this.campaignId = actor.campaignId
        this.actor = actor
        this.reason = reason
        this.method = method
        this.result = result
        this.detail = detail
    }
}

module.exports = ContactAction