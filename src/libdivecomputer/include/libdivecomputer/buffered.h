/*
 * libdivecomputer
 *
 * Buffered I/O stream for async data sources like BLE.
 *
 * This iostream variant uses native mutex/condition variable synchronization
 * to allow blocking reads while data is pushed from an external async source.
 */

#ifndef DC_BUFFERED_H
#define DC_BUFFERED_H

#include "common.h"
#include "context.h"
#include "iostream.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/**
 * Callback for write operations.
 * Called when libdivecomputer wants to write data (e.g., send via BLE).
 */
typedef dc_status_t (*dc_buffered_write_callback_t)(
    const unsigned char *data,
    size_t size,
    size_t *actual);

/**
 * Callback for close operations.
 * Called when the iostream is being closed.
 */
typedef dc_status_t (*dc_buffered_close_callback_t)();

/**
 * Opaque handle for the buffered iostream.
 */
typedef struct dc_buffered_t dc_buffered_t;

/**
 * Create a buffered I/O stream.
 *
 * The buffered iostream handles reads from an internal buffer that can be
 * filled from an external source (e.g., BLE notifications). Reads will block
 * until data is available or timeout occurs.
 *
 * Writes are passed through to a callback function.
 *
 * @param[out]  iostream        A location to store the buffered I/O stream.
 * @param[in]   context         A valid context object.
 * @param[in]   transport       The transport type.
 * @param[in]   write_callback  Callback for write operations.
 * @param[in]   close_callback  Callback for close operations (can be NULL).
 * @returns #DC_STATUS_SUCCESS on success, or another #dc_status_t code on failure.
 */
dc_status_t
dc_buffered_open(dc_iostream_t **iostream,
                 dc_context_t *context,
                 dc_transport_t transport,
                 dc_buffered_write_callback_t write_callback,
                 dc_buffered_close_callback_t close_callback);

/**
 * Get the buffered iostream handle from the generic iostream.
 *
 * @param[in]   iostream   The iostream created by dc_buffered_open.
 * @returns The buffered iostream handle, or NULL if not a buffered iostream.
 */
dc_buffered_t *
dc_buffered_get_handle(dc_iostream_t *iostream);

/**
 * Push data into the read buffer.
 *
 * Call this function when data is received from the external source
 * (e.g., BLE notification). This will wake up any blocking read operation.
 *
 * Thread-safe: Can be called from any thread.
 *
 * @param[in]   buffered   The buffered iostream handle.
 * @param[in]   data       The data to push into the buffer.
 * @param[in]   size       The size of the data.
 * @returns #DC_STATUS_SUCCESS on success, or another #dc_status_t code on failure.
 */
dc_status_t
dc_buffered_push(dc_buffered_t *buffered,
                 const unsigned char *data,
                 size_t size);

/**
 * Get the number of bytes available in the read buffer.
 *
 * Thread-safe: Can be called from any thread.
 *
 * @param[in]   buffered   The buffered iostream handle.
 * @returns The number of bytes available.
 */
size_t
dc_buffered_get_available(dc_buffered_t *buffered);

/**
 * Clear the read buffer.
 *
 * Thread-safe: Can be called from any thread.
 *
 * @param[in]   buffered   The buffered iostream handle.
 */
void
dc_buffered_clear(dc_buffered_t *buffered);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* DC_BUFFERED_H */
