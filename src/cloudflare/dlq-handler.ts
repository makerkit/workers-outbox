import type { QueueBatch } from './queue-handler';

import { createEmitter, type EventSink } from '../runtime/events';
import { previewBody } from './message-codec';

/** A dead-lettered nudge loses nothing (cron still drives the job): report and ack. */
export function createDlqHandler(options: { events?: EventSink } = {}) {
  const emit = createEmitter(options.events);

  return async (batch: QueueBatch) => {
    for (const message of batch.messages) {
      emit({
        type: 'dlq_message',
        queue: batch.queue,
        attempts: message.attempts,
        body: previewBody(message.body),
      });
      message.ack();
    }
  };
}
