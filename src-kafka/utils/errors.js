// Adapted from: https://github.com/MetaMask/utils/blob/main/src/errors.ts
/**
 * @typedef WithCodeType
 * @property {string} [code]
 * 
 * @typedef {Error & WithCodeType} ErrorWithCode
 */

/**
 * A type guard for objects.
 *
 * @param {unknown} value - The value to check.
 * @returns {value is ObjectConstructor} Whether the specified value has a runtime type of `object` and is
 * neither `null` nor an `Array`.
 */
export function isObject(value) {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Type guard for determining whether the given value is an instance of Error.
 * For errors generated via `fs.promises`, `error instanceof Error` won't work,
 * so we have to come up with another way of testing.
 *
 * @param {unknown} error - The object to check.
 * @returns {error is Error} A boolean.
 */
function isError(error) {
    return (
        error instanceof Error ||
        (isObject(error) && error.constructor.name === 'Error')
    );
}

/**
 * Type guard for determining whether the given value is an error object with a
 * `code` property such as the type of error that Node throws for filesystem
 * operations, etc.
 *
 * @param {unknown} error - The object to check.
 * @returns {error is ErrorWithCode} A boolean.
 */
export function isErrorWithCode(error){
    return typeof error === 'object' && error !== null && 'code' in error;
}

/**
 * Builds a new error object, linking it to the original error via the `cause`
 * property if it is an Error.
 *
 * This function is useful to reframe error messages in general, but is
 * _critical_ when interacting with any of Node's filesystem functions as
 * provided via `fs.promises`, because these do not produce stack traces in the
 * case of an I/O error (see <https://github.com/nodejs/node/issues/30944>).
 *
 * @template Throwable
 * @param {Throwable} originalError - The error to be wrapped (something throwable).
 * @param {string} message - The desired message of the new error.
 * @returns {ErrorWithCode} A new error object.
 */
export function wrapError(originalError, message) {
    if (isError(originalError)) {
        /** @type {ErrorWithCode} */
        let error = new Error(message, { cause: originalError });

        if (isErrorWithCode(originalError)) {
            error.code = originalError.code;
        }

        return error;
    }

    if (message.length > 0) {
        return new Error(`${String(originalError)}: ${message}`);
    }

    return new Error(String(originalError));
}