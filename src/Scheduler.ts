import {randomBytes} from 'crypto';

import debug from 'debug';
import {RedisClient} from 'redis';

import {ClusterNode} from './ClusterNode';


type RequiredOptions = {
  prefix: string;
  heartbeat: number;
};

export type Options = {
  [K in keyof RequiredOptions]?: RequiredOptions[K];
};

const DEFAULT_OPTIONS: RequiredOptions = {
  prefix: 'scheduler',
  heartbeat: 500,
};

export type Handler = (id: string, payload: object) => Promise<void>;

type Task = {
  owner: string;
  at: number;
  payload: object;
}

const TASKS_KEY = 'tasks';

const ns = 'scheduler';

const log = {
  error: debug(`${ns}:error`),
  warn: debug(`${ns}:warn`),
  info: debug(`${ns}:info`),
  debug: debug(`${ns}:debug`),
  trace: debug(`${ns}:trace`),
};

export class Scheduler {

  private readonly _redis: RedisClient;
  private readonly _prefix: string;

  private readonly _node: ClusterNode;
  private readonly _tasks: Map<string, NodeJS.Timeout>;

  private readonly _nodeRemovedHandler = this.collectOrphanTasks.bind(this);

  private _handler?: Handler;

  constructor(redis: RedisClient, options: Options = {}){

    this._redis = redis;

    const effectiveOptions: RequiredOptions = {
      ...DEFAULT_OPTIONS,
      ...options,
    };

    this._prefix = effectiveOptions.prefix;

    this._tasks = new Map<string, NodeJS.Timeout>();

    this._node = new ClusterNode(this._redis, {
      prefix: effectiveOptions.prefix,
      heartbeat: effectiveOptions.heartbeat,
    });
  }

  public async init(handler: Handler){

    // запоминаем обработчик таски
    this._handler = handler;

    // присоединяемся к кластеру
    await this._node.join();

    // начинаем реагировать на выпадение ноды из кластера
    this._node.on('nodeRemoved', this._nodeRemovedHandler);

    // собираем ничейные задачи
    await this.collectOrphanTasks();
  }

  public async uninit(){

    // перестаем реагировать на выпадение ноды из кластера
    this._node.off('nodeRemoved', this._nodeRemovedHandler);

    // отцепляемся от кластера
    await this._node.leave();

    // отменяем вызовы
    for(const [id, timeout] of this._tasks){
      clearTimeout(timeout);
      this._tasks.delete(id);
    }

    // забываем обработчик таски
    delete this._handler;

    // наши таски подберет другой воркер
  }

  public async scheduleTask(delay: number, payload: object): Promise<string> {

    if(!(delay > 0))
      throw new Error('Delay must be positive integer');

    const id = randomBytes(8).toString('hex');

    const task: Task = {
      owner: this._node.id,
      at: Date.now() + delay,
      payload,
    };

    await this.persistTask(id, task);

    this._tasks.set(id, setTimeout(this.processTask.bind(this, id, payload), delay));

    log.info(`Task scheduled in ${delay}ms: ${id} ${JSON.stringify(payload)}`);

    return id;
  }

  public async cancelTask(id: string) {

    await this.unpersistTask(id);

    const timeout = this._tasks.get(id);

    if(timeout) {
      clearTimeout(timeout);
    } else {
      log.warn(`Timeout was not registered for task: ${id}`);
    }

    this._tasks.delete(id);

    log.info(`Task cancelled: ${id}`); // TODO retrieve payload for logging?
  }

  private async processTask(id: string, payload: object) {

    log.debug(`Start processing task: ${id} ${JSON.stringify(payload)}`);

    if(!this._handler)
      throw new Error('No handler present');

    await this._handler(id, payload);

    this._tasks.delete(id);

    await this.unpersistTask(id);

    log.info(`Done processing task: ${id} ${JSON.stringify(payload)}`);
  }

  private async collectOrphanTasks() {

    log.info('Collecting orphan tasks');

    const key = `${this._prefix}:${TASKS_KEY}`;

    const response: Record<string, string> = await new Promise(((resolve, reject) => this._redis.hgetall(key, (e, r) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(r);
    })));

    const {validTasks, invalidTasks} = this.parseTasks(response);

    for(const [id, data] of invalidTasks) {

      log.warn(`Invalid task found, deleting: ${id} ${data}`);

      const key = `${this._prefix}:${TASKS_KEY}`;
      const property = id;

      await new Promise(((resolve, reject) => this._redis.hdel(key, property, (e, n) => {
        if(e) {
          reject(e);
          return;
        }
        resolve(n);
      })));

      invalidTasks.delete(id);
    }

    // log.debug(`Loaded tasks: ${validTasks.size}`);

    for(const [id, task] of validTasks) {

      if(this._node.isNodePresent(task.owner))
        continue;

      // log.debug(`Claiming orphan task from ${task.owner}: ${id} ${JSON.stringify(task)}`)

      const oldOwner = task.owner;

      task.owner = this._node.id;

      const delay = task.at - Date.now();

      await this.persistTask(id, task);

      this._tasks.set(id, setTimeout(this.processTask.bind(this, id, task.payload), delay));

      log.info(`Task claimed from ${oldOwner} and scheduled in ${delay}ms: ${id} ${JSON.stringify(task.payload)}`);
    }
  }

  private parseTasks(rawTasks: Record<string, string>): {validTasks: Map<string, Task>; invalidTasks: Map<string, string>} {

    const validTasks = new Map<string, Task>();
    const invalidTasks = new Map<string, string>();

    for(const id in rawTasks) {

      // compiler wanted this
      if(!rawTasks.hasOwnProperty(id))
        continue;

      try {
        validTasks.set(id, JSON.parse(rawTasks[id]));
      } catch (e) {
        invalidTasks.set(id, rawTasks[id]);
      }
    }

    return {validTasks, invalidTasks};
  }

  private async persistTask(id: string, task: Task) {

    const key = `${this._prefix}:${TASKS_KEY}`;
    const property = id;
    const value = JSON.stringify(task);

    return new Promise(((resolve, reject) => this._redis.hset(key, property, value, (e, n) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(n);
    })));
  }

  private async unpersistTask(id: string) {

    const key = `${this._prefix}:${TASKS_KEY}`;
    const property = id;

    return new Promise(((resolve, reject) => this._redis.hdel(key, property, (e, n) => {
      if(e) {
        reject(e);
        return;
      }
      resolve(n);
    })));
  }
}