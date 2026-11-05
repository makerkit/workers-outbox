const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Any-case hex in the 8-4-4-4-12 form the `uuid` column accepts. */
export const isUuid = (value: string) => UUID_PATTERN.test(value);
