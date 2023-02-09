const { Kafka } = require('kafkajs')
const { NodeExecutionManager } = require('../nodes/ExecutionManager')
const { CustomEventNode } = require('../nodes/CustomEventNode')

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

    const topics = [
      'start-nodes-topic',
      'finish-nodes-topic',
      'http-nodes-topic',
      'form-request-nodes-topic',
      'flow-nodes-topic',
      'js-script-task-nodes-topic',
      'user-task-nodes-topic',
      'timer-nodes-topic',
      'system-task-nodes-topic',
      'event-nodes-topic',
    ]

    for (let topic of topics) await consumer.subscribe({ topic , fromBeginning: false })

    NodeExecutionManager.producer = producer
    CustomEventNode.producer = producer
    manager.connect(consumer)
}

module.exports = {
    connect
}