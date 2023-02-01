const {
    Nodes
} = require('@flowbuild/engine')
const {
    CustomTimerSystemTaskNode
} = require('./timer')

class NodeExecutionManager {

    static get instance() {
        return NodeExecutionManager._instance;
    }

    static set instance(instance) {
        NodeExecutionManager._instance = instance;
    }

    static get producer() {
        return NodeExecutionManager._producer;
    }

    static set producer(producer) {
        NodeExecutionManager._producer = producer;
    }


    constructor() {
        if(NodeExecutionManager.instance) {
            return NodeExecutionManager.instance
        }
        NodeExecutionManager.instance = this
        return this
    }

    async runAction(topic, action) {
        const nodeMap = {
            'start-nodes-topic': Nodes.StartNode,
            'http-nodes-topic': Nodes.HttpSystemTaskNode,
            'finish-nodes-topic': Nodes.FinishNode,
            'form-request-nodes-topic': Nodes.FormRequestNode,
            'flow-nodes-topic': Nodes.FlowNode,
            'js-script-task-nodes-topic': Nodes.ScriptTaskNode,
            'timer-nodes-topic': CustomTimerSystemTaskNode,
            'user-task-nodes-topic': Nodes.UserTaskNode,
        }
        const node = new nodeMap[topic](action.node_spec)
        const result = await node.run({...action.execution_data})

        console.info("RESULT: ", { result, timestamp: Date.now() })

        const messageValue = {
            result,
            workflow_name: action.workflow_name,
            process_id: action.process_id
        }

        if(topic==='finish-nodes-topic') {
            await NodeExecutionManager.producer.send({
                topic: 'orchestrator-finish-topic',
                messages: [
                    { value: JSON.stringify(messageValue) },
                ],
            })    
        }

        await NodeExecutionManager.producer.send({
            topic: 'orchestrator-result-topic',
            messages: [
                { value: JSON.stringify(messageValue) },
            ],
        })

    }

    async connect(consumer) {
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const receivedMessage = message.value?.toString() || ''

                console.info(`\nMessage received on NodeExecutionManager.connect -> ${JSON.stringify({ partition, offset: message.offset, value: receivedMessage })}`)

                try {
                    const action = JSON.parse(receivedMessage)
                    this.runAction(topic, action)
                } catch(err) {
                    console.error(err)
                }
            },
        })
    }
}

module.exports = {
    NodeExecutionManager
}