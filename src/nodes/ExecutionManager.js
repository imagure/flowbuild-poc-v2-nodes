const {
    Nodes
} = require('@flowbuild/engine');
const { get } = require('../utils/get');
const { set } = require('../utils/set');
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

    extractResultToBag(source, spec) {
        if(!spec) {
            return {}
        }
        const bag = {}
        const readValues = spec.map((path) => [path, get(source, path)])
        for(let [path, value] of readValues) {
            set(bag, path, value)
        }
        return bag
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
            result: {
                ...result,
                bag: this.extractResultToBag(result.result, action.node_spec.extract)
            },
            workflow_name: action.workflow_name,
            process_id: action.process_id,
            actor: action.actor
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