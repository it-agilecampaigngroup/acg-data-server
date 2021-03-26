const { Kafka, logLevel } = require('kafkajs')
const kafkaConfig = require('./kafkaConnection')
const kafka = new Kafka(kafkaConfig)

// Connect the producer
const producer = kafka.producer()
const runProducer = async () => {
    try {
        await producer.connect()        
    } catch (e) {
        console.log(e.message)
    }
}
runProducer()

module.exports = {
    // Send a UserActions message
    sendUserActionsMsg ( msg ) {
        producer.send({
            topic: 'UserActions',
            messages: [{ value: JSON.stringify(msg) + "\r\n" }]
        })
    },

    // Send a ContactActions message
    sendContactActionsMsg ( msg ) {
        producer.send({
            topic: 'ContactActions',
            messages: [{ value: JSON.stringify(msg) + "\r\n" }]
        })
    },

    // Send an AppErrors message
    sendAppErrorsMsg ( msg ) {
        producer.send({
            topic: 'AppErrors',
            messages: [{ value: JSON.stringify(msg) + "\r\n" }]
        })
    }

}