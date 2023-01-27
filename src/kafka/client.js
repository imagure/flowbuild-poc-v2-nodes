const { Kafka } = require('kafkajs')
const { NodeExecutionManager } = require('../nodes/ExecutionManager')

const kafka = new Kafka({
  clientId: 'flowbuild-v2-nodes',
  brokers: [`${process.env.BROKER_HOST || 'localhost'}:9092`]
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'consumer-nodes-group' })

const connect = async () => {
    const manager = new NodeExecutionManager()
    
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({ topic: 'start-nodes-topic', fromBeginning: true })
    await consumer.subscribe({ topic: 'finish-nodes-topic', fromBeginning: true })
    await consumer.subscribe({ topic: 'http-nodes-topic', fromBeginning: true })

    NodeExecutionManager.producer = producer
    manager.connect(consumer)
}

module.exports = {
    connect
}