import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/transport-node";
import { createStreams, startNats } from "@famashines/ai-systems-kit/testing";
import type { StreamHead } from "../src/protocol.ts";
import { connectNats } from "../src/views.io.ts";

export const socketGatewayGrants = [
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.VIEWS",
  "$JS.API.CONSUMER.CREATE.VIEWS",
  "$JS.API.CONSUMER.CREATE.VIEWS.>",
  "$JS.API.CONSUMER.INFO.VIEWS.*",
  "$JS.API.CONSUMER.DELETE.VIEWS.*",
  "$JS.API.CONSUMER.MSG.NEXT.VIEWS.*",
];

const adminGrants = [
  "$JS.API.>",
  "$JS.ACK.>",
  "orchestration.>",
  "conversation.>",
  "llm.>",
  "budget.>",
  "agents.>",
  "prompts.>",
  "dlq.>",
];

const list = (subjects: string[]): string => subjects.map((s) => `"${s}"`).join(", ");

const user = (name: string, password: string, publish: string[], subscribe: string[]) => `
      {
        user: ${name}, password: ${password}
        permissions {
          publish { allow: [${list(publish)}] }
          subscribe { allow: [${list(subscribe)}] }
        }
      }`;

export const natsConfig = `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [${[
      user("ai-systems-admin", "admin", adminGrants, ["_INBOX_admin.>"]),
      user("ai-systems-socket-gateway", "gateway", socketGatewayGrants, [
        "_INBOX_socket-gateway.>",
        "delta.>",
      ]),
      user(
        "ai-systems-projections",
        "projections",
        ["view.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.VIEWS"],
        ["_INBOX_projections.>"],
      ),
      user("ai-systems-ai-proxy", "ai-proxy", ["delta.>"], ["_INBOX_ai-proxy.>"]),
    ].join("")}
    ]
  }
}
`;

export const viewsStream = {
  name: "VIEWS",
  subjects: ["view.>"],
  max_bytes: 268435456,
  max_age: 604800000000000,
};

export interface ViewsNats {
  url: string;
  publish(subject: string, data: string): Promise<number>;
  publishDelta(subject: string, data: string): Promise<void>;
  head(): Promise<StreamHead>;
  trimBefore(seq: number): Promise<void>;
  consumers(): Promise<number>;
  remake(): Promise<void>;
  close(): Promise<void>;
}

export async function startViewsNats(): Promise<ViewsNats> {
  const container = await startNats({ config: natsConfig });
  const login = (user: string, pass: string, service: string): Promise<NatsConnection> =>
    connectNats({ servers: [container.url], user, pass, service });
  const admin = await login("ai-systems-admin", "admin", "admin");
  const projections = await login("ai-systems-projections", "projections", "projections");
  const aiProxy = await login("ai-systems-ai-proxy", "ai-proxy", "ai-proxy");
  await createStreams(admin, [viewsStream]);
  const manager = await jetstreamManager(admin);
  const js = jetstream(projections);
  return {
    url: container.url,
    publish: async (subject, data) => (await js.publish(subject, data)).seq,
    async publishDelta(subject, data) {
      aiProxy.publish(subject, data);
      await aiProxy.flush();
    },
    async head() {
      const { state } = await manager.streams.info("VIEWS");
      return { first: state.first_seq, last: state.last_seq };
    },
    async trimBefore(seq) {
      await manager.streams.purge("VIEWS", { seq });
    },
    async remake() {
      await manager.streams.delete("VIEWS");
      await createStreams(admin, [viewsStream]);
    },
    consumers: async () => (await manager.streams.info("VIEWS")).state.consumer_count,
    async close() {
      await Promise.all([admin.close(), projections.close(), aiProxy.close()]);
      await container.stop();
    },
  };
}
