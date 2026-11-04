const IDENTIFIER_LIKE = /^[a-z_]+$/;

// Library constants only; the guard makes misuse with outside input fail loudly, not inject.
export function sqlLiteralList(values: readonly string[]) {
  for (const value of values) {
    if (!IDENTIFIER_LIKE.test(value)) {
      throw new Error(`sqlLiteralList: refusing non-constant value ${JSON.stringify(value)}`);
    }
  }

  return values.map((value) => `'${value}'`).join(', ');
}
