const { createClient } = require('redis');
const { envs } = require('../configs/envs');

class RedisClient {
    static _instance

    static get instance() {
        return RedisClient._instance;
    }

    static set instance(instance) {
        RedisClient._instance = instance;
    }

    constructor() {
        if(RedisClient.instance) {
            return RedisClient.instance
        }
        
        this._client = createClient({
            url: `redis://:${envs.REDIS_PASSWORD}@${envs.REDIS_HOST || 'localhost'}:${envs.REDIS_PORT}`,
        })
        this._client.connect()
        this._client.on('error', err => console.error('Redis Client Error', err))
        
        // for reference:
        // await client.disconnect()

        RedisClient.instance = this
        return this
    }

    async get(key) {
        const data = await this._client.get(key)
        try {
            return JSON.parse(data)
        } catch(e) {
            return data
        }
    }

    async set(key, value, options = {}) {
        if(options?.EX) {
            return await this._client.setEx(key, options.EX, value)    
        }
        return await this._client.set(key, value)
    }

    async del(key) {
        return await this._client.del(key)
    }
}

module.exports = {
    RedisClient
}