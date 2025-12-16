/*
 * libdivecomputer
 *
 * Buffered I/O stream implementation for async data sources like BLE.
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>

#include <libdivecomputer/buffered.h>

#include "iostream-private.h"
#include "common-private.h"
#include "context-private.h"

#define BUFFER_INITIAL_SIZE 4096
#define BUFFER_MAX_SIZE (1024 * 1024) /* 1 MB max */

/* Forward declarations */
static dc_status_t dc_buffered_set_timeout(dc_iostream_t *iostream, int timeout);
static dc_status_t dc_buffered_set_break(dc_iostream_t *iostream, unsigned int value);
static dc_status_t dc_buffered_set_dtr(dc_iostream_t *iostream, unsigned int value);
static dc_status_t dc_buffered_set_rts(dc_iostream_t *iostream, unsigned int value);
static dc_status_t dc_buffered_get_lines(dc_iostream_t *iostream, unsigned int *value);
static dc_status_t dc_buffered_available(dc_iostream_t *iostream, size_t *value);
static dc_status_t dc_buffered_configure(dc_iostream_t *iostream, unsigned int baudrate, unsigned int databits, dc_parity_t parity, dc_stopbits_t stopbits, dc_flowcontrol_t flowcontrol);
static dc_status_t dc_buffered_poll(dc_iostream_t *iostream, int timeout);
static dc_status_t dc_buffered_read(dc_iostream_t *iostream, void *data, size_t size, size_t *actual);
static dc_status_t dc_buffered_write(dc_iostream_t *iostream, const void *data, size_t size, size_t *actual);
static dc_status_t dc_buffered_ioctl(dc_iostream_t *iostream, unsigned int request, void *data, size_t size);
static dc_status_t dc_buffered_flush(dc_iostream_t *iostream);
static dc_status_t dc_buffered_purge(dc_iostream_t *iostream, dc_direction_t direction);
static dc_status_t dc_buffered_sleep(dc_iostream_t *iostream, unsigned int milliseconds);
static dc_status_t dc_buffered_close(dc_iostream_t *iostream);

struct dc_buffered_t {
    /* Base class */
    dc_iostream_t base;

    /* Callbacks */
    dc_buffered_write_callback_t write_callback;
    dc_buffered_close_callback_t close_callback;

    /* Read buffer */
    unsigned char *buffer;
    size_t buffer_size;     /* Allocated size */
    size_t buffer_used;     /* Bytes currently in buffer */
    size_t buffer_offset;   /* Read offset */

    /* Synchronization */
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    /* Timeout in milliseconds (-1 = infinite, 0 = non-blocking) */
    int timeout;

    /* Closed flag */
    int closed;
};

static const dc_iostream_vtable_t dc_buffered_vtable = {
    sizeof(dc_buffered_t),
    dc_buffered_set_timeout,
    dc_buffered_set_break,
    dc_buffered_set_dtr,
    dc_buffered_set_rts,
    dc_buffered_get_lines,
    dc_buffered_available,
    dc_buffered_configure,
    dc_buffered_poll,
    dc_buffered_read,
    dc_buffered_write,
    dc_buffered_ioctl,
    dc_buffered_flush,
    dc_buffered_purge,
    dc_buffered_sleep,
    dc_buffered_close,
};

dc_status_t
dc_buffered_open(dc_iostream_t **iostream,
                 dc_context_t *context,
                 dc_transport_t transport,
                 dc_buffered_write_callback_t write_callback,
                 dc_buffered_close_callback_t close_callback)
{
    dc_buffered_t *buffered = NULL;

    if (iostream == NULL || write_callback == NULL)
        return DC_STATUS_INVALIDARGS;

    INFO(context, "Open: transport=%u (buffered)", transport);

    /* Allocate memory */
    buffered = (dc_buffered_t *)dc_iostream_allocate(context, &dc_buffered_vtable, transport);
    if (buffered == NULL) {
        ERROR(context, "Failed to allocate memory.");
        return DC_STATUS_NOMEMORY;
    }

    /* Initialize callbacks */
    buffered->write_callback = write_callback;
    buffered->close_callback = close_callback;

    /* Allocate read buffer */
    buffered->buffer = (unsigned char *)malloc(BUFFER_INITIAL_SIZE);
    if (buffered->buffer == NULL) {
        ERROR(context, "Failed to allocate buffer.");
        dc_iostream_deallocate((dc_iostream_t *)buffered);
        return DC_STATUS_NOMEMORY;
    }
    buffered->buffer_size = BUFFER_INITIAL_SIZE;
    buffered->buffer_used = 0;
    buffered->buffer_offset = 0;

    /* Initialize synchronization */
    if (pthread_mutex_init(&buffered->mutex, NULL) != 0) {
        ERROR(context, "Failed to initialize mutex.");
        free(buffered->buffer);
        dc_iostream_deallocate((dc_iostream_t *)buffered);
        return DC_STATUS_IO;
    }

    if (pthread_cond_init(&buffered->cond, NULL) != 0) {
        ERROR(context, "Failed to initialize condition variable.");
        pthread_mutex_destroy(&buffered->mutex);
        free(buffered->buffer);
        dc_iostream_deallocate((dc_iostream_t *)buffered);
        return DC_STATUS_IO;
    }

    /* Default timeout: blocking */
    buffered->timeout = -1;
    buffered->closed = 0;

    *iostream = (dc_iostream_t *)buffered;

    return DC_STATUS_SUCCESS;
}

dc_buffered_t *
dc_buffered_get_handle(dc_iostream_t *iostream)
{
    if (iostream == NULL)
        return NULL;

    if (!dc_iostream_isinstance(iostream, &dc_buffered_vtable))
        return NULL;

    return (dc_buffered_t *)iostream;
}

dc_status_t
dc_buffered_push(dc_buffered_t *buffered,
                 const unsigned char *data,
                 size_t size)
{
    if (buffered == NULL || data == NULL || size == 0)
        return DC_STATUS_INVALIDARGS;

    pthread_mutex_lock(&buffered->mutex);

    /* Check if closed */
    if (buffered->closed) {
        pthread_mutex_unlock(&buffered->mutex);
        return DC_STATUS_IO;
    }

    /* Compact buffer if needed (move unread data to beginning) */
    if (buffered->buffer_offset > 0) {
        size_t unread = buffered->buffer_used - buffered->buffer_offset;
        if (unread > 0) {
            memmove(buffered->buffer, buffered->buffer + buffered->buffer_offset, unread);
        }
        buffered->buffer_used = unread;
        buffered->buffer_offset = 0;
    }

    /* Grow buffer if needed */
    size_t required = buffered->buffer_used + size;
    if (required > buffered->buffer_size) {
        size_t new_size = buffered->buffer_size * 2;
        while (new_size < required)
            new_size *= 2;

        if (new_size > BUFFER_MAX_SIZE) {
            pthread_mutex_unlock(&buffered->mutex);
            return DC_STATUS_NOMEMORY;
        }

        unsigned char *new_buffer = (unsigned char *)realloc(buffered->buffer, new_size);
        if (new_buffer == NULL) {
            pthread_mutex_unlock(&buffered->mutex);
            return DC_STATUS_NOMEMORY;
        }

        buffered->buffer = new_buffer;
        buffered->buffer_size = new_size;
    }

    /* Copy data to buffer */
    memcpy(buffered->buffer + buffered->buffer_used, data, size);
    buffered->buffer_used += size;

    /* Signal waiting readers */
    pthread_cond_signal(&buffered->cond);

    pthread_mutex_unlock(&buffered->mutex);

    return DC_STATUS_SUCCESS;
}

size_t
dc_buffered_get_available(dc_buffered_t *buffered)
{
    if (buffered == NULL)
        return 0;

    pthread_mutex_lock(&buffered->mutex);
    size_t available = buffered->buffer_used - buffered->buffer_offset;
    pthread_mutex_unlock(&buffered->mutex);

    return available;
}

void
dc_buffered_clear(dc_buffered_t *buffered)
{
    if (buffered == NULL)
        return;

    pthread_mutex_lock(&buffered->mutex);
    buffered->buffer_used = 0;
    buffered->buffer_offset = 0;
    pthread_mutex_unlock(&buffered->mutex);
}

static dc_status_t
dc_buffered_set_timeout(dc_iostream_t *iostream, int timeout)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    pthread_mutex_lock(&buffered->mutex);
    buffered->timeout = timeout;
    pthread_mutex_unlock(&buffered->mutex);

    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_set_break(dc_iostream_t *iostream, unsigned int value)
{
    (void)iostream;
    (void)value;
    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_set_dtr(dc_iostream_t *iostream, unsigned int value)
{
    (void)iostream;
    (void)value;
    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_set_rts(dc_iostream_t *iostream, unsigned int value)
{
    (void)iostream;
    (void)value;
    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_get_lines(dc_iostream_t *iostream, unsigned int *value)
{
    (void)iostream;
    if (value)
        *value = 0;
    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_available(dc_iostream_t *iostream, size_t *value)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    if (value == NULL)
        return DC_STATUS_INVALIDARGS;

    *value = dc_buffered_get_available(buffered);

    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_configure(dc_iostream_t *iostream, unsigned int baudrate, unsigned int databits, dc_parity_t parity, dc_stopbits_t stopbits, dc_flowcontrol_t flowcontrol)
{
    (void)iostream;
    (void)baudrate;
    (void)databits;
    (void)parity;
    (void)stopbits;
    (void)flowcontrol;
    return DC_STATUS_SUCCESS;
}

/* Helper to wait with timeout using pthread_cond_timedwait */
static int
wait_for_data(dc_buffered_t *buffered, int timeout_ms)
{
    /* Check if data already available */
    if (buffered->buffer_used > buffered->buffer_offset)
        return 1;

    if (timeout_ms == 0) {
        /* Non-blocking: return immediately */
        return 0;
    }

    if (timeout_ms < 0) {
        /* Infinite wait */
        while (buffered->buffer_used <= buffered->buffer_offset && !buffered->closed) {
            pthread_cond_wait(&buffered->cond, &buffered->mutex);
        }
        return buffered->buffer_used > buffered->buffer_offset;
    }

    /* Timed wait */
    struct timeval now;
    gettimeofday(&now, NULL);

    struct timespec deadline;
    deadline.tv_sec = now.tv_sec + timeout_ms / 1000;
    deadline.tv_nsec = now.tv_usec * 1000 + (timeout_ms % 1000) * 1000000;
    if (deadline.tv_nsec >= 1000000000) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000;
    }

    while (buffered->buffer_used <= buffered->buffer_offset && !buffered->closed) {
        int rc = pthread_cond_timedwait(&buffered->cond, &buffered->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            return 0;
        }
    }

    return buffered->buffer_used > buffered->buffer_offset;
}

static dc_status_t
dc_buffered_poll(dc_iostream_t *iostream, int timeout)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    pthread_mutex_lock(&buffered->mutex);

    if (buffered->closed) {
        pthread_mutex_unlock(&buffered->mutex);
        return DC_STATUS_IO;
    }

    int has_data = wait_for_data(buffered, timeout);

    pthread_mutex_unlock(&buffered->mutex);

    return has_data ? DC_STATUS_SUCCESS : DC_STATUS_TIMEOUT;
}

static dc_status_t
dc_buffered_read(dc_iostream_t *iostream, void *data, size_t size, size_t *actual)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    if (data == NULL || actual == NULL)
        return DC_STATUS_INVALIDARGS;

    pthread_mutex_lock(&buffered->mutex);

    if (buffered->closed) {
        pthread_mutex_unlock(&buffered->mutex);
        *actual = 0;
        return DC_STATUS_IO;
    }

    /* Wait for data if buffer is empty */
    if (buffered->buffer_used <= buffered->buffer_offset) {
        int has_data = wait_for_data(buffered, buffered->timeout);
        if (!has_data) {
            pthread_mutex_unlock(&buffered->mutex);
            *actual = 0;
            return DC_STATUS_TIMEOUT;
        }
    }

    /* Read available data */
    size_t available = buffered->buffer_used - buffered->buffer_offset;
    size_t to_read = (available < size) ? available : size;

    memcpy(data, buffered->buffer + buffered->buffer_offset, to_read);
    buffered->buffer_offset += to_read;

    /* Compact buffer if we've read past half */
    if (buffered->buffer_offset > buffered->buffer_size / 2) {
        size_t unread = buffered->buffer_used - buffered->buffer_offset;
        if (unread > 0) {
            memmove(buffered->buffer, buffered->buffer + buffered->buffer_offset, unread);
        }
        buffered->buffer_used = unread;
        buffered->buffer_offset = 0;
    }

    pthread_mutex_unlock(&buffered->mutex);

    *actual = to_read;

    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_write(dc_iostream_t *iostream, const void *data, size_t size, size_t *actual)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    if (data == NULL || actual == NULL)
        return DC_STATUS_INVALIDARGS;

    if (buffered->closed) {
        *actual = 0;
        return DC_STATUS_IO;
    }

    /* Pass through to callback */
    return buffered->write_callback(data, size, actual);
}

static dc_status_t
dc_buffered_ioctl(dc_iostream_t *iostream, unsigned int request, void *data, size_t size)
{
    (void)iostream;
    (void)request;
    (void)data;
    (void)size;
    return DC_STATUS_UNSUPPORTED;
}

static dc_status_t
dc_buffered_flush(dc_iostream_t *iostream)
{
    (void)iostream;
    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_purge(dc_iostream_t *iostream, dc_direction_t direction)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    if (direction & DC_DIRECTION_INPUT) {
        dc_buffered_clear(buffered);
    }

    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_sleep(dc_iostream_t *iostream, unsigned int milliseconds)
{
    (void)iostream;

    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (milliseconds % 1000) * 1000000;

    nanosleep(&ts, NULL);

    return DC_STATUS_SUCCESS;
}

static dc_status_t
dc_buffered_close(dc_iostream_t *iostream)
{
    dc_buffered_t *buffered = (dc_buffered_t *)iostream;

    /* Mark as closed and wake up any waiting readers */
    pthread_mutex_lock(&buffered->mutex);
    buffered->closed = 1;
    pthread_cond_broadcast(&buffered->cond);
    pthread_mutex_unlock(&buffered->mutex);

    /* Call close callback */
    if (buffered->close_callback) {
        buffered->close_callback();
    }

    /* Clean up */
    pthread_mutex_destroy(&buffered->mutex);
    pthread_cond_destroy(&buffered->cond);
    free(buffered->buffer);

    return DC_STATUS_SUCCESS;
}
