import {
  ClientMessageV1,
  ServerMessageV1,
  ViewMessageV1,
  WsLimits,
  type Topic,
} from "@famashines/ai-systems-contracts";

export type Notice = Extract<ServerMessageV1, { type: "notice" }>;
export type Delta = Extract<ServerMessageV1, { type: "delta" }>;

export function parseClientFrame(text: string): ClientMessageV1 | undefined {
  let json: unknown;
  try {
    json = JSON.parse(text);
  } catch {
    return undefined;
  }
  const parsed = ClientMessageV1.safeParse(json);
  return parsed.success ? parsed.data : undefined;
}

export function parseViewMessage(seq: number, subject: string, data: string): Notice | undefined {
  let json: unknown;
  try {
    json = JSON.parse(data);
  } catch {
    return undefined;
  }
  const parsed = ViewMessageV1.safeParse(json);
  if (!parsed.success || parsed.data.type !== subject) return undefined;
  const { topic, docId, rev, data: document, commandId } = parsed.data.payload;
  return {
    type: "notice",
    seq,
    topic,
    docId,
    rev,
    data: document,
    ...(commandId === undefined ? {} : { commandId }),
  };
}

export function parseDelta(subject: string, data: string): Delta | undefined {
  const [prefix, taskId, callId, ...rest] = subject.split(".");
  if (prefix !== "delta" || rest.length > 0) return undefined;
  let json: unknown;
  try {
    json = JSON.parse(data);
  } catch {
    return undefined;
  }
  if (typeof json !== "object" || json === null) return undefined;
  const parsed = ServerMessageV1.safeParse({ ...json, type: "delta", taskId, callId });
  return parsed.success && parsed.data.type === "delta" ? parsed.data : undefined;
}

export const taskOf = (topic: Topic): string | undefined =>
  topic.startsWith("task:") ? topic.slice("task:".length) : undefined;

export interface StreamHead {
  first: number;
  last: number;
}

export const isTooOld = (lastSeq: number, head: StreamHead): boolean =>
  lastSeq + 1 < head.first || lastSeq > head.last;

export function rateWindow(limit: number = WsLimits.clientFramesPerSecond, windowMs = 1000) {
  let start = Number.NEGATIVE_INFINITY;
  let count = 0;
  return (now: number): boolean => {
    if (now - start >= windowMs) {
      start = now;
      count = 0;
    }
    count += 1;
    return count <= limit;
  };
}

export const maxTimerMs = 2 ** 31 - 1;

export const untilExpiry = (exp: number, now: number): number =>
  Math.min(Math.max(exp * 1000 - now, 0), maxTimerMs);

export interface Added {
  added: Topic[];
  fromSeq: number;
}

export function subscriptions(resumeAfter?: number, maxTopics: number = WsLimits.maxTopics) {
  const floors = new Map<Topic, number>();
  let cursor = 0;
  let resume = resumeAfter;
  return {
    topics: (): Topic[] => [...floors.keys()],
    start: (): number => cursor + 1,
    needsHead: (): boolean => resume === undefined,
    add(topics: readonly Topic[], last: number): Added | undefined {
      const added = [...new Set(topics)].filter((topic) => !floors.has(topic));
      if (floors.size + added.length > maxTopics) return undefined;
      const base = resume ?? last;
      if (floors.size === 0) cursor = base;
      for (const topic of added) floors.set(topic, base);
      const fromSeq = Math.min(
        ...topics.map((topic) => (added.includes(topic) ? base : cursor) + 1),
      );
      return { added, fromSeq };
    },
    remove(topics: readonly Topic[]): Topic[] {
      return [...new Set(topics)].filter((topic) => floors.delete(topic));
    },
    accept(seq: number, topic: Topic | undefined): boolean {
      if (seq <= cursor) return false;
      cursor = seq;
      resume = undefined;
      if (topic === undefined) return false;
      const floor = floors.get(topic);
      return floor !== undefined && seq > floor;
    },
  };
}

export type Subscriptions = ReturnType<typeof subscriptions>;
