'use strict'

class ContactAction {
    constructor(action, campaign, actor, reason, method, result, detail = "") {
        this.action = action
        this.timestamp = Date()
        this.clientId = campaign.client.clientId
        this.campaignId = campaign.campaignId
        this.actor = actor
        this.reason = reason
        this.method = method
        this.result = result
        this.detail = detail
    }
}

module.exports = ContactAction