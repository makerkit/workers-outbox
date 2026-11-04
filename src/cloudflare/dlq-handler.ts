import type { QueueBatch } from './queue-handler';

import { createEmitter, type EventSink } from '../runtime/events';

/** A dead-lettered nudge loses nothing (cron still drives the job): report and ack. */
export function createDlqHandler(options: { events?: EventSink } = {}) {
  const emit = createEmitter(options.events);

  return async (batch: QueueBatch) => {
    for (const message of batch.messages) {
      let body: string;

      try {
        body = JSON.stringify(message.body)?.slice(0, 500) ?? String(message.body);
      } catch {
        body = String(message.body);
      }

      emit({ type: 'dlq_message', queue: batch.queue, attempts: message.attempts, body });
      message.ack();
    }
  };
}
