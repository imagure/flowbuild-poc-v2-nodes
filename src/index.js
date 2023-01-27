const { connect } = require('./kafka/client')

// const orchestrator_consumed_topic = 'orchestrator'

async function main() {
    await connect()
}

main()