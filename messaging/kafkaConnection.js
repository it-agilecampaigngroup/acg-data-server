
module.exports = {
    clientId: process.env.KAFKA_CLIENT_ID,
    kafkaTopic: process.env.KAFKA_TOPIC,
    brokers: process.env.KAFKA_BROKERS.split(','),
    connectionTimeOut: process.env.KAFKA_CONNECTION_TIMEOUT,
    authenticationTimeout: process.env.KAFKA_AUTHEMTICATION_TIMEOUT,
    reauthenticationTimeout: process.env.KAFKA_REAUTHENTICATION_TIMEOUT
}


