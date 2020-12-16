import {EventEmitter} from 'events';
import {randomBytes} from 'crypto';

import {RedisClient} from 'redis';

type RequiredOptions = {
  prefix: string;
  heartbeat: number;
};

export type Options = {
  [K in keyof RequiredOptions]?: RequiredOptions[K];
};

const DEFAULT_OPTIONS: RequiredOptions = {
  prefix: 'cluster',
  heartbeat: 500,
};

export class ClusterNode extends EventEmitter {

  private static readonly NODES_KEY = 'nodes';

  public readonly id: string;

  private readonly _redis: RedisClient;
  private readonly _prefix: string;
  private readonly _heartbeat: number;

  private readonly _nodes: Set<string>;

  private _updater?: NodeJS.Timeout;

  constructor(redis: RedisClient, options: Options = {}) {

    super();

    this._redis = redis;

    const effectiveOptions: RequiredOptions = {
      ...DEFAULT_OPTIONS,
      ...options,
    };

    this._prefix = effectiveOptions.prefix;
    this._heartbeat = effectiveOptions.heartbeat;

    this._nodes = new Set<string>();

    this.id = randomBytes(8).toString('hex');
  }

  public async join(){

    if(this._updater)
      return;

    await this.lookAlive();
    await this.updateNodes();

    this._updater = setInterval(async () => {
      await this.lookAlive();
      await this.updateNodes();
    }, this._heartbeat);
  }

  public async leave(){

    if(!this._updater)
      return;

    clearInterval(this._updater);
    await this.cleanup(this.id);
  }

  public isNodePresent(id: string) {
    return this._nodes.has(id);
  }

  private async lookAlive() {

    const key = `${this._prefix}:${ClusterNode.NODES_KEY}`;
    const property = this.id;

    // До какого момента другие ноды должны считать что мы живы.
    //
    // Мы сами определяем этот момент, чтобы корректно отрабатывал кейс
    // когда выкатывается новая версия с другим значением heartbeat
    const value = (Date.now() + this._heartbeat * 2).toString();

    return new Promise(((resolve, reject) => this._redis.hset(key, property, value, (e, n) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(n);
    })));
  }

  private async cleanup(id: string) {

    const key = `${this._prefix}:${ClusterNode.NODES_KEY}`;
    const property = id;

    return new Promise(((resolve, reject) => this._redis.hdel(key, property, (e, n) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(n);
    })));
  }

  private async updateNodes(){

    // фиксируем текущее время, чтобы не посчитать ноды мертвыми из-за того что мы сами долго обрабатываем
    const now = Date.now();

    const key = `${this._prefix}:${ClusterNode.NODES_KEY}`;

    const response: Record<string, string> = await new Promise(((resolve, reject) => this._redis.hgetall(key, (e, r) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(r);
    })));

    const nodes = new Set<string>();

    for(const [id, value] of Object.entries(response)) {

      const at = parseInt(value);

      if(!(at > now)) {
        await this.cleanup(id);
        continue;
      }

      nodes.add(id);
    }

    // check for nodes left
    for(const id of this._nodes) {

      if(nodes.has(id))
        continue;

      this._nodes.delete(id);
      this.emit('nodeRemoved', id);
    }

    // check for nodes joined
    for(const id of nodes) {

      if(this._nodes.has(id))
        continue;

      this._nodes.add(id);
      this.emit('nodeAdded', id);
    }
  }
}