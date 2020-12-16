import {RedisClient} from 'redis';
import {Scheduler} from './Scheduler';


(async () => {

  const redis = new RedisClient({
    host: 'localhost'
  });

  const scheduler = new Scheduler(redis);

  await scheduler.init(async (id, payload) => {
    console.log('PROCESS', id, payload);
  });

  await scheduler.scheduleTask(5000, {hello: 'world'});

})();