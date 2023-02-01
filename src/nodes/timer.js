
const { Nodes } = require('@flowbuild/engine')
const { promisify } = require('util')
const sleep = promisify(setTimeout)

class CustomTimerSystemTaskNode extends Nodes.SystemTaskNode {
  next(result) {
    if(result['$runtimer']) {
      return this._spec["id"];
    }
    return this._spec["next"]
  }

  async _run(execution_data, lisp) {
    if(execution_data['$runtimer']) {
      await sleep(execution_data.parameters?.timeout || 0)
      return [{ ...execution_data, $runtimer: false }, 'running'];
    }
    return [{ ...execution_data, $runtimer: true }, 'pending'];
  }

  _preProcessing({ bag, input, actor_data, environment, parameters }) {
    return { ...bag, ...input, actor_data, environment, parameters };
  }
}

module.exports = {
    CustomTimerSystemTaskNode,
};
