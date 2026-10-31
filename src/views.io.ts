import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import type { Views } from "./gateway.ts";

export const stream = "VIEWS";

export interface NatsOptions {
  servers: string[];
  user: string;
  pass: string;
  service: string;
}

export const connectNats = (options: NatsOptions): Promise<NatsConnection> =>
  connect({
    servers: options.servers,
    user: options.user,
    pass: options.pass,
    name: options.service,
    inboxPrefix: `_INBOX_${options.service}`,
    maxReconnectAttempts: -1,
    waitOnFirstConnect: true,
  });

export async function natsViews(
  connection: NatsConnection,
): Promise<Views & { ready(): Promise<boolean> }> {
  const js = jetstream(connection);
  const manager = await jetstreamManager(connection);
  const head = async () => {
    const { state } = await manager.streams.info(stream);
    return { first: state.first_seq, last: state.last_seq };
  };
  return {
    head,
    ready: () =>
      head().then(
        () => true,
        () => false,
      ),
    async consume(subjects, startSeq, onMessage, onError) {
      const consumer = await js.consumers.get(stream, {
        filter_subjects: subjects,
        opt_start_seq: startSeq,
      });
      const messages = await consumer.consume();
      const stopped = new AbortController();
      const done = (async () => {
        for await (const message of messages) {
          onMessage({ seq: message.seq, subject: message.subject, data: message.string() });
        }
        if (!stopped.signal.aborted) throw new Error("The VIEWS consumer stopped");
      })().catch(onError);
      return async () => {
        stopped.abort();
        await messages.close();
        await done;
        await consumer.delete().catch(() => false);
      };
    },
    deltas(taskId, onMessage) {
      const subscription = connection.subscribe(`delta.${taskId}.*`, {
        callback: (error, message) => {
          if (!error) onMessage(message.subject, message.string());
        },
      });
      return () => {
        subscription.unsubscribe();
      };
    },
  };
}
