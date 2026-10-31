import { orderedStream, type Nats } from "@famashines/ai-systems-kit/nats";
import type { Views } from "./gateway.ts";

export const stream = "VIEWS";

export async function natsViews(nats: Nats): Promise<Views & { ready(): Promise<boolean> }> {
  const views = await orderedStream(nats, stream);
  return {
    head: () => views.head(),
    ready: () =>
      views.head().then(
        () => true,
        () => false,
      ),
    consume: (subjects, startSeq, onMessage, onError, onGone) =>
      views.consume({ subjects, startSeq, onMessage, onError, onGone }),
    deltas(taskId, onMessage) {
      const subscription = nats.connection.subscribe(`delta.${taskId}.*`, {
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
