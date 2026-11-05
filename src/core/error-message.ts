/** An error's message, or a non-error's string form. */
export function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
