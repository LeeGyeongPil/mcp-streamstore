// Redis 클라이언트 어댑터. 이 라이브러리는 Redis 클라이언트에 의존하지 않고,
// 사용자가 이미 쓰는 클라이언트(node-redis 또는 ioredis)를 아래 함수로 감싸 넘긴다.
import type { RedisCommand } from './types.js';

/** Minimal shape of a node-redis v4+ client (`createClient()` from the `redis` package). */
export interface NodeRedisLike {
  sendCommand(args: string[]): Promise<unknown>;
}

/** Minimal shape of an ioredis client. */
export interface IoredisLike {
  call(command: string, ...args: string[]): Promise<unknown>;
}

/** Wrap a node-redis client: `new RedisEventStore({ command: nodeRedisCommand(client) })`. */
export function nodeRedisCommand(client: NodeRedisLike): RedisCommand {
  return (args) => client.sendCommand(args);
}

/** Wrap an ioredis client: `new RedisEventStore({ command: ioredisCommand(redis) })`. */
export function ioredisCommand(client: IoredisLike): RedisCommand {
  return ([command, ...rest]) => {
    if (!command) return Promise.reject(new Error('empty Redis command'));
    return client.call(command, ...rest);
  };
}

/** publish/subscribe functions in the shape `RedisServerEventBus` expects. */
export interface PubSub {
  publish: (channel: string, message: string) => Promise<unknown>;
  subscribe: (channel: string, onMessage: (message: string) => void) => Promise<() => Promise<void>>;
}

/** Minimal node-redis shapes for pub/sub. A subscribed connection cannot send other commands, so pass a duplicate. */
export interface NodeRedisPublisher {
  publish(channel: string, message: string): Promise<unknown>;
}
export interface NodeRedisSubscriber {
  subscribe(channel: string, listener: (message: string, channel: string) => void): Promise<unknown>;
  unsubscribe(channel: string, listener?: (message: string, channel: string) => void): Promise<unknown>;
}

/**
 * Pub/sub over node-redis. `subscriber` must be a separate connection, e.g.
 * `const sub = client.duplicate(); await sub.connect();`.
 */
export function nodeRedisPubSub(publisher: NodeRedisPublisher, subscriber: NodeRedisSubscriber): PubSub {
  return {
    publish: (channel, message) => publisher.publish(channel, message),
    subscribe: async (channel, onMessage) => {
      const listener = (message: string) => onMessage(message);
      await subscriber.subscribe(channel, listener);
      let done = false;
      return async () => {
        if (done) return; // 두 번 불러도 안전
        done = true;
        await subscriber.unsubscribe(channel, listener);
      };
    },
  };
}

/** Minimal ioredis shapes for pub/sub (a subscribed ioredis connection is also dedicated). */
export interface IoredisSubscriber {
  subscribe(channel: string): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  on(event: 'message', listener: (channel: string, message: string) => void): unknown;
  off(event: 'message', listener: (channel: string, message: string) => void): unknown;
}
export interface IoredisPublisher {
  publish(channel: string, message: string): Promise<unknown>;
}

/** Pub/sub over ioredis. `subscriber` must be a separate connection, e.g. `redis.duplicate()`. */
export function ioredisPubSub(publisher: IoredisPublisher, subscriber: IoredisSubscriber): PubSub {
  return {
    publish: (channel, message) => publisher.publish(channel, message),
    subscribe: async (channel, onMessage) => {
      const listener = (ch: string, message: string) => {
        if (ch === channel) onMessage(message);
      };
      subscriber.on('message', listener);
      await subscriber.subscribe(channel);
      let done = false;
      return async () => {
        if (done) return;
        done = true;
        subscriber.off('message', listener);
        await subscriber.unsubscribe(channel);
      };
    },
  };
}
