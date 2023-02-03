/*
 * TODO: cleanup code from "memory assertions" - handle OOM gracefully
 */

#include <string.h>
#include <ctype.h>
#include <erl_nif.h>

#ifdef USE_SYSTEM_SQLITE
#include <sqlite3.h>
#else
#include "./sqlite3.h"
#endif

#if SQLITE_VERSION_NUMBER < 3038000
static int sqlite3_error_offset(sqlite3 *db) {return -1;}
#endif


/* Not imported from erl_nif.h */
#define THE_NON_VALUE	0

/* Assertion: import from ERTS */
#ifdef DEBUG
void erl_assert_error(const char* expr, const char* func, const char* file, int line);
#  define ASSERT(e) ((void) ((e) ? 1 : (erl_assert_error(#e, __func__, __FILE__, __LINE__), 0)))
#else
#  define ASSERT(e) ((void) (e))
#endif


/* Atom definitions (atoms cannot change dynamically) */
static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_true;
static ERL_NIF_TERM am_false;
static ERL_NIF_TERM am_general;
static ERL_NIF_TERM am_reason;
static ERL_NIF_TERM am_badarg;
static ERL_NIF_TERM am_undefined;
static ERL_NIF_TERM am_position;
static ERL_NIF_TERM am_out_of_memory;   /* out of memory */
static ERL_NIF_TERM am_sqlite_error;    /* sqlite-specific error code */
static ERL_NIF_TERM am_blob;
static ERL_NIF_TERM am_done;
static ERL_NIF_TERM am_busy;

/* database open modes */
static ERL_NIF_TERM am_mode;
static ERL_NIF_TERM am_flags;
static ERL_NIF_TERM am_uri;
static ERL_NIF_TERM am_read_only;
static ERL_NIF_TERM am_read_write;
static ERL_NIF_TERM am_read_write_create;
static ERL_NIF_TERM am_memory;
static ERL_NIF_TERM am_shared;
static ERL_NIF_TERM am_busy_timeout;

static ERL_NIF_TERM am_persistent;
static ERL_NIF_TERM am_no_vtab;

/* atoms for notifications */
static ERL_NIF_TERM am_insert;
static ERL_NIF_TERM am_update;
static ERL_NIF_TERM am_delete;

/* connection status atoms */
static ERL_NIF_TERM am_lookaside_memory;
static ERL_NIF_TERM am_pager_cache_memory;
static ERL_NIF_TERM am_schema;
static ERL_NIF_TERM am_statement;
static ERL_NIF_TERM am_deferred_fks;
static ERL_NIF_TERM am_used;
static ERL_NIF_TERM am_max;
static ERL_NIF_TERM am_hit;
static ERL_NIF_TERM am_miss_size;
static ERL_NIF_TERM am_miss_full;
static ERL_NIF_TERM am_shared;
static ERL_NIF_TERM am_miss;
static ERL_NIF_TERM am_write;
static ERL_NIF_TERM am_spill;

/* statement info atoms */
static ERL_NIF_TERM am_fullscan_step;
static ERL_NIF_TERM am_sort;
static ERL_NIF_TERM am_autoindex;
static ERL_NIF_TERM am_vm_step;
static ERL_NIF_TERM am_reprepare;
static ERL_NIF_TERM am_run;
static ERL_NIF_TERM am_filter_miss;
static ERL_NIF_TERM am_filter_hit;
static ERL_NIF_TERM am_memory_used;

/* system info atoms */
static ERL_NIF_TERM am_page_cache;
static ERL_NIF_TERM am_malloc;
static ERL_NIF_TERM am_version;

/* forward definitions (linked list) */
typedef struct statement_t statement_t;
typedef struct delayed_close_t delayed_close_t;

/* prepared statement: stored as a part of connection */
struct statement_t {
    sqlite3_stmt* statement;    /* sqlite3 handle */
    statement_t* next;          /* next statement in the overall list */
    statement_t* released;      /* next released statement that can be reused */
};

typedef struct {
    ErlNifMutex* mutex;         /* used to protect the entire structure from concurrent access */
    sqlite3* connection;        /* sqlite3 handle */
    ErlNifPid tracer;           /* process to send ROWID updates */
    ERL_NIF_TERM ref;           /* sent to the tracer process */
    ErlNifMutex* stmt_mutex;    /* mutex protecting the statements list */
    statement_t* statements;    /* list of all statements for this connection */
    statement_t* released;      /* head of the 'released statements sublist' */
} connection_t;

/* resources: connection and connection-to-deallocate */
static ErlNifResourceType *s_connection_resource;
/* delayed deallocation process */
static ErlNifPid s_delayed_close_pid;

struct delayed_close_t {
    sqlite3* connection;
    statement_t* statements;    /* list of all statements for this connection */
    delayed_close_t* next;
};

/* queue of connections to delete */
static delayed_close_t* s_delayed_close_queue;
/* mutex protexting the queue (could be done lockless, but there is no sense in it) */
static ErlNifMutex* s_delayed_close_mutex;

/* forward declaration, to emergency use in the destructor */
static ERL_NIF_TERM sqlite_dirty_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

/* Connection destructor.
   It may be executed in the context of the normal scheduler, while doing GC,
   so it must not perform any long-running activities.
   Closing sqlite database may take any amount of time, hence it must be
   running by a dirty I/O scheduler.
   To achieve that, the on_load function of a NIF starts a new process that
   keeps running only for the sake of executing a (hidden from the official API)
   dirty I/O NIF that actually does the cleanup. Here, in the destructor,
   no clean-up happens, only the message gets sent to the process. If the process
   does not exist, resource leak happens.
*/
static void sqlite_connection_destroy(ErlNifEnv *env, void *arg) {
    connection_t *conn = (connection_t*)arg;
    
    /* there is no need in locking the connection structure, it's guaranteed 
    to be the very last instance */
    if (conn->connection) {
        /* can get here only if no explicit close/1 call was successful */
        delayed_close_t* node = enif_alloc(sizeof(delayed_close_t));
        node->connection = conn->connection;
        node->statements = conn->statements;
        enif_mutex_lock(s_delayed_close_mutex);
        node->next = s_delayed_close_queue;
        s_delayed_close_queue = node;
        enif_mutex_unlock(s_delayed_close_mutex);
        /* notify garbage collection process */
        if (!enif_send(env, &s_delayed_close_pid, NULL, am_undefined)) {
            enif_fprintf(stderr, "sqlite connection deallocated during GC");
            /* last resort, cleanup resources locally */
            sqlite_dirty_close_nif(env, 0, NULL);
        }
    } else {
        /* No need to schedule a dirty NIF, the connection was already closed.
           At this point there are no outstanding statements, and no referenced
           statements either - so simply free the entire statements list. */
        statement_t* stmt = conn->statements;
        statement_t* freed;
        while (stmt) {
            ASSERT(stmt->statement == NULL);
            freed = stmt;
            stmt = stmt->next;
            enif_free(freed);
        }
    }
        
    enif_mutex_destroy(conn->mutex);
    enif_mutex_destroy(conn->stmt_mutex);
}

/* Prepared statement resource. Has a weak link to the connection it belongs to.
If the connection is closed (either implicitly via GC, or explicitly
via close/1 call), all prepared statements are no longer valid.
*/
static ErlNifResourceType *s_statement_resource;

typedef struct {
    connection_t* connection;
    statement_t* reference;
} statement_resource_t;

/*
Prepared statement management is tricky. Any operation with the statement
(prepare, finalize, or step) requires exclusive connection object ownership.
Therefore all these operations must happen within a dirty NIF, and not
as a part of a normal scheduler. When GC collects a statement, instead of
finalising it in place, just mark it 'released'. GC callback cannot take
the connection mutex! As otherwise it'd be stuck behind dirty NIFs, leading
to a deadlock.

Using SERIALIZED sqlite mode does not help: it simply wraps all operations
(including sqlite3_bind_xxx) with connection mutex. In theory it allows
multiple NIFs to interleave argument binding, but in practice it's not
recommended doing that anyway.
*/

static void sqlite_statement_destroy(ErlNifEnv *env, void *arg) {
    statement_resource_t *stmt = (statement_resource_t*)arg;
    if (stmt->connection) {
        enif_mutex_lock(stmt->connection->stmt_mutex);
        stmt->reference->released = stmt->connection->released;
        stmt->connection->released = stmt->reference;
        enif_mutex_unlock(stmt->connection->stmt_mutex);
        enif_release_resource(stmt->connection);
    }
}

/* -------------------------------------------------------------------------------- */
/* Helper functions (creating binaries and errors) */
static ERL_NIF_TERM make_binary(ErlNifEnv *env, const char* str, unsigned int len) {
    ErlNifBinary bin;
    if (!enif_alloc_binary(len, &bin))
        return THE_NON_VALUE;
    memcpy(bin.data, str, len);
    ERL_NIF_TERM bin_term = enif_make_binary(env, &bin);
    enif_release_binary(&bin);
    return bin_term;
}

/* Creates a tuple to simulate EEP-54 exceptions in the Erlang code).
   - reason: the "old" exception reason (e.g. "badarg")
   - general: the "general" string in the "cause" of the extended error info
   - [reason_text]: optional pre-formatted reason string
   - [arg_pos]: optional argument number (when the exception is caused by badarg)
   - [arg_error]: optional, value of the arg_pos key in the "cause" map
   - [position]: optional, column (for SQL statements) or bound parameter index
*/
static ERL_NIF_TERM make_extended_error_ex(ErlNifEnv *env, ERL_NIF_TERM reason, const char* general,
    int arg_pos, const char* arg_error, const char* reason_text, int position) {
    /* create the new 'cause' map */
    ERL_NIF_TERM error_info_map = enif_make_new_map(env);

    if (general)
        enif_make_map_put(env, error_info_map, am_general, 
            make_binary(env, general, strlen(general)), &error_info_map);

    if (reason_text)
        enif_make_map_put(env, error_info_map, am_reason, 
            make_binary(env, reason_text, strlen(reason_text)), &error_info_map);

    if (arg_pos > 0 && arg_error)
        enif_make_map_put(env, error_info_map, enif_make_int(env, arg_pos), 
            make_binary(env, arg_error, strlen(arg_error)), &error_info_map);

    if (position >= 0)
        enif_make_map_put(env, error_info_map, am_position, enif_make_int(env, position), &error_info_map);

    return enif_make_tuple3(env, am_error, reason, error_info_map);
}

/* Shortcut for badarg */
static ERL_NIF_TERM make_badarg(ErlNifEnv *env, const char* general, int arg_pos, const char* arg_error) {
    return make_extended_error_ex(env, am_badarg, general, arg_pos, arg_error, NULL, -1);
}

/* Special exception for a generic SQLite error when there is no SQL statement known
   - reason is always {sqlite_error, Code}
   - reason_text: error text reported by SQLite
   - general: step that generated the error (e.g. "error opening database")
*/
static ERL_NIF_TERM make_generic_sqlite_error(ErlNifEnv *env, int sqlite_error, const char* operation) {
    const char* sql_err = sqlite3_errstr(sqlite_error);
    ERL_NIF_TERM reason = enif_make_tuple2(env, am_sqlite_error, enif_make_int(env, sqlite_error));
    return make_extended_error_ex(env, reason, operation, -1, NULL, sql_err, -1);
}

/* badarg SQL exception (malformed SQL line)
   - reason is always {sqlite_error, Code}
   - general: step that generated the error (e.g. "error prepared statements")
   - position: if SQLite version has sqlite3_error_offset, the column number
   - arg_pos and arg_error - position, and actual SQLite reported error
*/
static ERL_NIF_TERM make_sql_error(ErlNifEnv *env, sqlite3* conn, int sqlite_error, const char* operation, 
    int arg_pos, const unsigned char* sql, int len) {
    int position = sqlite3_error_offset(conn);
    ERL_NIF_TERM reason = enif_make_tuple2(env, am_sqlite_error, enif_make_int(env, sqlite_error));
    if (position == -1)
        return make_extended_error_ex(env, reason, operation, arg_pos, sqlite3_errstr(sqlite_error), NULL, position);
    else {
        /* insert the "near: " explanation for sql statement with an error */
        char buf[256], word[64]; /* keep up to 64 characters */
        len = len - position;
        /* take the smallest of remaining len and word size */
        len = len > (sizeof(word) - 1) ? (sizeof(word) - 1) : len;
        /* copy until hitting the next non-graph, but always take the first character */
        word[0] = sql[position];
        for (int i=1; i<len; i++)
            if (isgraph(sql[i + position]))
                word[i] = sql[i + position];
            else {
                word[i] = 0;
                break;
            }
        enif_snprintf(buf, sizeof(buf), "%s near column %d: \"%s\"", sqlite3_errstr(sqlite_error), position, word);
        return make_extended_error_ex(env, reason, operation, arg_pos, buf, NULL, position);
    }
}

/* -------------------------------------------------------------------------------- */
/* Connection (opens the database) */
static ERL_NIF_TERM sqlite_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    char *filename;
    ErlNifBinary fn_bin;
    ERL_NIF_TERM mode, flag;
    int v2flags = 0;
    ErlNifSInt64 busy_timeout = -1;

    /* type checks */
    if (!enif_is_map(env, argv[1]))
        return make_badarg(env, NULL, 2, "not a map");

    /* flags specification */
    if (enif_get_map_value(env, argv[1], am_flags, &mode)) {
        while (!enif_is_empty_list(env, mode)) {
            if (!enif_get_list_cell(env, mode, &flag, &mode))
                return make_badarg(env, NULL, 2, "invalid flags");
            if (flag == am_memory)
                v2flags |= SQLITE_OPEN_MEMORY;
            else if (flag == am_uri)
                v2flags |= SQLITE_OPEN_URI;
            else if (flag == am_shared)
                v2flags |= SQLITE_OPEN_SHAREDCACHE;
            else 
                return make_badarg(env, NULL, 2, "invalid flag");
        }
    }

    /* opening mode */
    if (enif_get_map_value(env, argv[1], am_mode, &mode)) {
        if (mode == am_read_only)
            v2flags |= SQLITE_OPEN_READONLY;
        else if (mode == am_read_write)
            v2flags |= SQLITE_OPEN_READWRITE;
        else if (mode == am_read_write_create)
            v2flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        else
            return make_badarg(env, NULL, 2, "unsupported mode");
    } else /* default mode */
        v2flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

    v2flags |= SQLITE_OPEN_EXRESCODE; /* always want extended errors */

    if (enif_get_map_value(env, argv[1], am_busy_timeout, &mode)) {
        if (!enif_get_int64(env, mode, &busy_timeout) || busy_timeout < 0)
            return make_badarg(env, NULL, 2, "invalid busy timeout");
    }

    /* accept filename as iolist */
    if (enif_inspect_iolist_as_binary(env, argv[0], &fn_bin)) {
        /* Erlang binaries are not NULL-terminated, so make a copy */
        filename = enif_alloc(fn_bin.size + 1);
        if (!filename)
            return enif_raise_exception(env, am_out_of_memory);
        strncpy(filename, (const char*)fn_bin.data, fn_bin.size);
        filename[fn_bin.size] = 0;
    } else
        return make_badarg(env, NULL, 1, "invalid file name");

    /* Open the database. */
    sqlite3* sqlite;
    int ret = sqlite3_open_v2(filename, &sqlite, v2flags, NULL);
    enif_free(filename);
    if (ret != SQLITE_OK) {
        sqlite3_close(sqlite); /* non-v2 used because no statement could be there, and no NULL check needed */
        return make_generic_sqlite_error(env, ret, "error opening connection");
    }

    /* set busy timeout */
    if (busy_timeout != -1)
        sqlite3_busy_timeout(sqlite, busy_timeout);

    /* Initialize the resource */
    connection_t* conn = enif_alloc_resource(s_connection_resource, sizeof(connection_t));
    if (!conn) {
        sqlite3_close(sqlite);
        return enif_raise_exception(env, am_out_of_memory);
    }

    enif_set_pid_undefined(&conn->tracer);
    conn->connection = sqlite;
    conn->statements = NULL;
    conn->released = NULL;
    conn->mutex = enif_mutex_create("sqlite3_connection");
    ASSERT(conn->mutex);
    conn->stmt_mutex = enif_mutex_create("sqlite3_statement");
    ASSERT(conn->stmt_mutex);

    ERL_NIF_TERM connection = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return connection;
}

static ERL_NIF_TERM sqlite_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t *conn;
    sqlite3* sqlite_db;
    statement_t* open;

    if (!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    enif_mutex_lock(conn->mutex);
    if (conn->connection) {
        sqlite_db = conn->connection;
        conn->connection = NULL;
    } else {
        enif_mutex_unlock(conn->mutex);
        return make_badarg(env, NULL, 1, "connection already closed");
    }

    /* iterate over all statements, removing sqlite3_stmt */
    open = conn->statements;
    while (open) {
        sqlite3_finalize(open->statement);
        open = open->next;
    }

    /* close the connection, there must not be any statements left */
    int ret = sqlite3_close(sqlite_db);
    ASSERT(ret == SQLITE_OK);

    enif_mutex_unlock(conn->mutex);

    return am_ok;
}

/* This function should only be called from a "cleanup" process that owns orphan connections
 * that weren't closed explicitly. Ideally it should not ever be called. */
static ERL_NIF_TERM sqlite_dirty_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    delayed_close_t* next;

    /* iterate over pending connections */
    enif_mutex_lock(s_delayed_close_mutex);
    ASSERT(s_delayed_close_queue);
    next = s_delayed_close_queue;
    s_delayed_close_queue = next->next;
    enif_mutex_unlock(s_delayed_close_mutex);

    /* it's guaranteed that no statements are used here, so just finalize all of them */
    statement_t* stmt = next->statements;
    statement_t* freed;
    while (stmt) {
        if (stmt->statement)
            sqlite3_finalize(stmt->statement);
        freed = stmt;
        stmt = stmt->next;
        enif_free(freed);
    }
    int ret = sqlite3_close(next->connection);
    ASSERT(ret == SQLITE_OK);
    enif_free(next);
    return am_ok;
}

/* bind & execute helpers */
static ERL_NIF_TERM bind(ErlNifEnv *env, sqlite3_stmt* stmt, ERL_NIF_TERM params, int arg_index) {
    ErlNifSInt64 int_val;
    double double_val;
    ErlNifBinary bin;
    ERL_NIF_TERM param;
    const ERL_NIF_TERM* tuple;
    int ret, column = 1, arity;
    int expected = sqlite3_bind_parameter_count(stmt);

    /* bind parameters passed */
    while (!enif_is_empty_list(env, params)) {
        if (!enif_get_list_cell(env, params, &param, &params))
            return make_extended_error_ex(env, am_badarg, NULL, arg_index, "invalid parameters list", NULL, column);

        /* supported types: undefined (atom), integer, blob, text, float */
        if (param == am_undefined) {
            /* NULL */
            ret = sqlite3_bind_null(stmt, column);
        } else if (enif_inspect_iolist_as_binary(env, param, &bin)) {
            /* TEXT */
            ret = sqlite3_bind_text64(stmt, column, (char*)bin.data, bin.size, SQLITE_STATIC, SQLITE_UTF8);
        } else if (enif_is_number(env, param)) {
            /* INTEGER or FLOAT */
            if (enif_get_int64(env, param, &int_val))
                ret = sqlite3_bind_int64(stmt, column, int_val);
            else if (enif_get_double(env, param, &double_val))
                ret = sqlite3_bind_double(stmt, column, double_val);
            else
                return make_extended_error_ex(env, am_badarg, "failed to bind parameter", arg_index, "bignum not supported",
                    NULL, column);
        } else if (enif_get_tuple(env, param, &arity, &tuple) && (tuple[0] == am_blob) && (enif_inspect_binary(env, tuple[1], &bin))) {
            ret = sqlite3_bind_blob64(stmt, column, (char*)bin.data, bin.size, SQLITE_STATIC);
        } else
            return make_extended_error_ex(env, am_badarg, "failed to bind parameter", arg_index, "unsupported type",
                NULL, column);

        if (ret != SQLITE_OK) {
            const char* sql_error = sqlite3_errstr(ret);
            ERL_NIF_TERM reason = enif_make_tuple2(env, am_sqlite_error, enif_make_int(env, ret));
            return make_extended_error_ex(env, reason, "failed to bind parameter", arg_index, sql_error,
                NULL, column);
        }

        column++;
    }

    if (column <= expected) 
        return make_badarg(env, NULL, 2, "not enough parameters");

    return THE_NON_VALUE;
}

static ERL_NIF_TERM fetch_row(ErlNifEnv *env, sqlite3_stmt* stmt, int column_count, ERL_NIF_TERM values[]) {
    int column_type;
    unsigned int size;
    ErlNifSInt64 int_val;
    double double_val;
    for (int i=0; i<column_count; i++) {
        column_type = sqlite3_column_type(stmt, i);
        switch (column_type) {
            case SQLITE_NULL:
                values[i] = am_undefined;
                break;
            case SQLITE_TEXT:
                size = sqlite3_column_bytes(stmt, i);
                const char* text = (const char*)sqlite3_column_text(stmt, i);
                values[i] = make_binary(env, text, size);
                break;
            case SQLITE_INTEGER:
                int_val = sqlite3_column_int64(stmt, i);
                values[i] = enif_make_int64(env, int_val);
                break;
            case SQLITE_FLOAT:
                double_val = sqlite3_column_double(stmt, i);
                values[i] = enif_make_double(env, double_val);
                break;
            case SQLITE_BLOB:
                size = sqlite3_column_bytes(stmt, i);
                const char* blob = (const char*)sqlite3_column_blob(stmt, i);
                values[i] = make_binary(env, blob, size);
                break;
        }
    }
    return enif_make_tuple_from_array(env, values, column_count);
}

/* fetching a row with over 256 columns is slow anyway, so memory allocation is not an issue there */
#define COLUMNS_ON_STACK    256

/* runs the query with pre-bound arguments */
static ERL_NIF_TERM execute(ErlNifEnv *env, sqlite3_stmt* stmt) {
    ERL_NIF_TERM next_row, result = enif_make_list(env, 0);
    int column_count = sqlite3_column_count(stmt);

    ERL_NIF_TERM values_buffer[COLUMNS_ON_STACK];
    ERL_NIF_TERM* row = &values_buffer[0];
    if (column_count > 512)
        row = (ERL_NIF_TERM*)enif_alloc(column_count * sizeof(ERL_NIF_TERM));
    else 
        row = &values_buffer[0];

    int ret = sqlite3_step(stmt);
    switch (ret) {
        case SQLITE_DONE:
            break;
        case SQLITE_ROW:
            do {
                next_row = fetch_row(env, stmt, column_count, row);
                result = enif_make_list_cell(env, next_row, result);
                ret = sqlite3_step(stmt);
            } while (ret == SQLITE_ROW);
            /* check if it was error that bailed out */
            if (ret != SQLITE_DONE)
                result = make_generic_sqlite_error(env, ret, "error receiving rows");
            break;
        default:
            result = make_generic_sqlite_error(env, ret, "error running query");
            break;
    }

    if (column_count > COLUMNS_ON_STACK)
        enif_free(row);

    return result;
}

/* Query preparation */
static ERL_NIF_TERM sqlite_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t* conn;
    if (!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    ErlNifBinary query_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query_bin))
        return make_badarg(env, NULL, 2, "not an iolist");

    /* type checks */
    if (!enif_is_map(env, argv[2]))
        return make_badarg(env, NULL, 3, "not a map");
    
    ERL_NIF_TERM flag;
    int prep_flags = 0;
    if (enif_get_map_value(env, argv[2], am_persistent, &flag)) {
        if (flag == am_true) 
            prep_flags |= SQLITE_PREPARE_PERSISTENT;
        else if (flag != am_false) 
            return make_badarg(env, NULL, 3, "invalid persistent value");
    }

    if (enif_get_map_value(env, argv[2], am_no_vtab, &flag)) {
        if (flag == am_true) 
            prep_flags |= SQLITE_PREPARE_NO_VTAB;
        else if (flag != am_false) 
            return make_badarg(env, NULL, 3, "invalid no_vtab value");
    }

    /* Initialize the resource */
    statement_resource_t* stmt_res = enif_alloc_resource(s_statement_resource, sizeof(statement_resource_t));
    ASSERT(stmt_res);
    stmt_res->connection = NULL;

    sqlite3_stmt* stmt;

    enif_mutex_lock(conn->mutex);
    if (!conn->connection) {
        enif_mutex_unlock(conn->mutex);
        enif_release_resource(stmt_res);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    int ret = sqlite3_prepare_v3(conn->connection, (char*)query_bin.data, query_bin.size, prep_flags, &stmt, NULL);
    if (ret != SQLITE_OK) {
        enif_mutex_unlock(conn->mutex);
        enif_release_resource(stmt_res);
        return make_sql_error(env, conn->connection, ret, "error preparing statement", 2, query_bin.data, query_bin.size);
    }

    statement_t* statement;
    enif_mutex_lock(conn->stmt_mutex);
    if (conn->released) {
        statement = conn->released;
        sqlite3_finalize(statement->statement);
        conn->released = statement->released;
    } else {
        /* create a new statement reference */
        statement = enif_alloc(sizeof(statement_t));
        ASSERT(statement);
        statement->next = conn->statements;
        conn->statements = statement;
    }

    statement->released = NULL;
    statement->statement = stmt;
    
    enif_mutex_unlock(conn->stmt_mutex);

    enif_mutex_unlock(conn->mutex);

    enif_keep_resource(conn);
    
    stmt_res->reference = statement;
    stmt_res->connection = conn;

    ERL_NIF_TERM stmt_res_term = enif_make_resource(env, stmt_res);
    enif_release_resource(stmt_res);

    return stmt_res_term;
}

/* Binds parameters of the prepared statement */
static ERL_NIF_TERM sqlite_bind_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_resource_t* stmt;

    if (!enif_get_resource(env, argv[0], s_statement_resource, (void **) &stmt))
        return make_badarg(env, NULL, 1, "not a prepared statement");

    enif_mutex_lock(stmt->connection->mutex);
    if (!stmt->connection->connection) {
        enif_mutex_unlock(stmt->connection->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    ERL_NIF_TERM result = bind(env, stmt->reference->statement, argv[1], 2);
    enif_mutex_unlock(stmt->connection->mutex);
    return result == THE_NON_VALUE ? am_ok : result;
}

/* Single query step */
static ERL_NIF_TERM sqlite_step_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_resource_t* stmt;
    ErlNifSInt64 steps;

    if (!enif_get_int64(env, argv[1], &steps) || steps < 1)
        return make_badarg(env, NULL, 2, "invalid step count");

    if (!enif_get_resource(env, argv[0], s_statement_resource, (void **) &stmt))
        return make_badarg(env, NULL, 1, "not a prepared statement");

    enif_mutex_lock(stmt->connection->mutex);
    if (!stmt->connection->connection) {
        enif_mutex_unlock(stmt->connection->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    int ret;
    ERL_NIF_TERM result;
    sqlite3_stmt* sqlite_stmt = stmt->reference->statement;
    int column_count = sqlite3_column_count(sqlite_stmt);
    if (steps == 1) {
        /* single step return */
        ret = sqlite3_step(sqlite_stmt);
        switch (ret) {
            case SQLITE_DONE:
                result = am_done;
                break;
            case SQLITE_BUSY:
                result = am_busy;
                break;
            case SQLITE_ROW: {
                ERL_NIF_TERM values_buffer[COLUMNS_ON_STACK];
                ERL_NIF_TERM* row = &values_buffer[0];
                if (column_count > 512)
                    row = (ERL_NIF_TERM*)enif_alloc(column_count * sizeof(ERL_NIF_TERM));
                else 
                    row = &values_buffer[0];
                result = fetch_row(env, sqlite_stmt, column_count, row);
                if (column_count > COLUMNS_ON_STACK)
                    enif_free(row);
                break;}
            default:
                result = make_generic_sqlite_error(env, ret, "error running query");
                break;
        }
    } else {
        /* multi-step return - either [Row] list, or a tuple of {done|busy, [Row]}*/
        ERL_NIF_TERM values_buffer[COLUMNS_ON_STACK];
        ERL_NIF_TERM* row = &values_buffer[0];
        if (column_count > 512)
            row = (ERL_NIF_TERM*)enif_alloc(column_count * sizeof(ERL_NIF_TERM));
        else 
            row = &values_buffer[0];

        result = enif_make_list(env, 0);
        while (steps > 0) {
            ERL_NIF_TERM next_row;
            steps--;
            ret = sqlite3_step(sqlite_stmt);
            switch (ret) {
                case SQLITE_DONE:
                    result = enif_make_tuple2(env, am_done, result);
                    steps = 0;
                    break;
                case SQLITE_BUSY:
                    result = enif_make_tuple2(env, am_busy, result);
                    steps = 0;
                    break;
                case SQLITE_ROW:
                    next_row = fetch_row(env, sqlite_stmt, column_count, row);
                    result = enif_make_list_cell(env, next_row, result);
                    break;
                default:
                    result = make_generic_sqlite_error(env, ret, "error running query");
                    steps = 0;
                    break;
            }
        }
        if (column_count > COLUMNS_ON_STACK)
            enif_free(row);
    }

    enif_mutex_unlock(stmt->connection->mutex);
    return result;
}

/* Query execution */
static ERL_NIF_TERM sqlite_execute_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_resource_t* stmt;

    if (!enif_get_resource(env, argv[0], s_statement_resource, (void **) &stmt))
        return make_badarg(env, NULL, 1, "not a prepared statement");

    enif_mutex_lock(stmt->connection->mutex);
    if (!stmt->connection->connection) {
        enif_mutex_unlock(stmt->connection->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    int ret = sqlite3_reset(stmt->reference->statement);
    if (ret != SQLITE_OK) {
        enif_mutex_unlock(stmt->connection->mutex);
        return make_generic_sqlite_error(env, ret, "error resetting statement");
    }

    ERL_NIF_TERM result = bind(env, stmt->reference->statement, argv[1], 2);
    if (result != THE_NON_VALUE) {
        enif_mutex_unlock(stmt->connection->mutex);
        return result;
    }
    result = execute(env, stmt->reference->statement);
    enif_mutex_unlock(stmt->connection->mutex);
    return result;
}

static ERL_NIF_TERM sqlite_describe_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_resource_t* stmt_res;
    int count;
    const char* name;
    const char* type;
    ERL_NIF_TERM columns, name_bin, type_bin;

    if (!enif_get_resource(env, argv[0], s_statement_resource, (void **) &stmt_res))
        return make_badarg(env, NULL, 1, "not a prepared statement");

    enif_mutex_lock(stmt_res->connection->mutex);
    if (!stmt_res->connection->connection) {
        enif_mutex_unlock(stmt_res->connection->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    count = sqlite3_column_count(stmt_res->reference->statement);

    columns = enif_make_list(env, 0);

    for(int i=count-1; i>=0; i--) {
        name = sqlite3_column_name(stmt_res->reference->statement, i);
        type = sqlite3_column_decltype(stmt_res->reference->statement, i);
        if (name && !type)
            return enif_raise_exception(env, am_out_of_memory);
        name_bin = make_binary(env, name, strlen(name));
        type_bin = make_binary(env, type, strlen(type));
        columns = enif_make_list_cell(env, enif_make_tuple2(env, name_bin, type_bin), columns);
    }

    enif_mutex_unlock(stmt_res->connection->mutex);

    return columns;
}

/* Combined preparation and execution */
static ERL_NIF_TERM sqlite_query_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t* conn;
    if (!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    ErlNifBinary query_bin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query_bin))
        return make_badarg(env, NULL, 2, "not an iolist");

    enif_mutex_lock(conn->mutex);
    if (!conn->connection) {
        enif_mutex_unlock(conn->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    sqlite3_stmt* stmt;
    int ret = sqlite3_prepare_v2(conn->connection, (char*)query_bin.data, query_bin.size, &stmt, NULL);
    if (ret != SQLITE_OK) {
        enif_mutex_unlock(conn->mutex);
        return make_sql_error(env, conn->connection, ret, "error preparing statement", 2, query_bin.data, query_bin.size);
    }

    ERL_NIF_TERM result = bind(env, stmt, argv[2], 3);
    if (result != THE_NON_VALUE) {
        sqlite3_finalize(stmt);
        enif_mutex_unlock(conn->mutex);
        return result;
    }
    result = execute(env, stmt);

    /* cleanup */
    sqlite3_finalize(stmt);
    enif_mutex_unlock(conn->mutex);

    return result;
}

static void update_callback(void *arg, int op_type, char const *db, char const *table, sqlite3_int64 rowid)
{
    connection_t *conn = (connection_t *)arg;
    if (!conn || enif_is_pid_undefined(&conn->tracer))
        return;

    ERL_NIF_TERM op;

    switch (op_type) {
        case SQLITE_INSERT:
            op = am_insert;
            break;
        case SQLITE_DELETE:
            op = am_delete;
            break;
        case SQLITE_UPDATE:
            op = am_update;
            break;
        default:
            op = am_undefined;
            break;
    }

    /* Need a NIF env to create terms */
    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM database = make_binary(env, db, strlen(db));
    ERL_NIF_TERM tab = make_binary(env, table, strlen(table));
    ERL_NIF_TERM row = enif_make_int64(env, rowid);
    ERL_NIF_TERM ref = enif_make_copy(env, conn->ref);

    ERL_NIF_TERM msg = enif_make_tuple5(env, ref, op, database, tab, row);

    if (!enif_send(NULL, &conn->tracer, env, msg))
        sqlite3_update_hook(conn->connection, NULL, NULL); /* remove failed monitor */

    enif_free_env(env);
}

/* Connection monitoring */
static ERL_NIF_TERM sqlite_monitor_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    connection_t* conn;
    if(!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    if (argv[1] == am_undefined)
        sqlite3_update_hook(conn->connection, NULL, conn);
    else 
        if (!enif_get_local_pid(env, argv[1], &conn->tracer))
            return make_badarg(env, NULL, 2, "not a pid");
        else {
            conn->ref = argv[0];
            sqlite3_update_hook(conn->connection, update_callback, conn);
            return conn->ref;
        }

    return am_ok;
}

static ERL_NIF_TERM sqlite_interrupt_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    connection_t* conn;
    if(!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    if (conn->connection)
        sqlite3_interrupt(conn->connection);
    else
        return make_badarg(env, NULL, 1, "connection closed");
    return am_ok;
}


/* Various informational NIFs */
static ERL_NIF_TERM sqlite_system_info_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM info = enif_make_new_map(env);
    const char* vsn;
    sqlite3_int64 cur, hw, page_cur, page_hw, page, undef;

    if (sqlite3_status64(SQLITE_STATUS_MEMORY_USED, &cur, &hw, 0) == SQLITE_OK) {
        ERL_NIF_TERM tuple = enif_make_tuple2(env, enif_make_int64(env, cur), enif_make_int64(env, hw));
        enif_make_map_put(env, info, am_memory_used, tuple, &info);
    }

    if ((sqlite3_status64(SQLITE_STATUS_PAGECACHE_USED, &cur, &hw, 0) == SQLITE_OK) &&
        (sqlite3_status64(SQLITE_STATUS_PAGECACHE_OVERFLOW, &page_cur, &page_hw, 0) == SQLITE_OK) &&
        (sqlite3_status64(SQLITE_STATUS_PAGECACHE_SIZE, &undef, &page, 0) == SQLITE_OK)) {
            ERL_NIF_TERM tuple = enif_make_tuple5(env, enif_make_int64(env, page), enif_make_int64(env, cur),
                enif_make_int64(env, hw), enif_make_int64(env, page_cur), enif_make_int64(env, page_hw));
            enif_make_map_put(env, info, am_page_cache, tuple, &info);
    }

    if ((sqlite3_status64(SQLITE_STATUS_MALLOC_SIZE, &page, &undef, 0) == SQLITE_OK) &&
        (sqlite3_status64(SQLITE_STATUS_MALLOC_COUNT, &cur, &hw, 0) == SQLITE_OK)) {
        ERL_NIF_TERM tuple = enif_make_tuple3(env, enif_make_int64(env, undef),
            enif_make_int64(env, cur), enif_make_int64(env, hw));
        enif_make_map_put(env, info, am_malloc, tuple, &info);
    }
    
    /* sqlite version */
    vsn = sqlite3_libversion();
    enif_make_map_put(env, info, am_version, make_binary(env, vsn, strlen(vsn)), &info);

    return info;
}

static ERL_NIF_TERM sqlite_get_last_insert_rowid_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t* conn;
    if(!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");
    sqlite3_interrupt(conn->connection);
    return enif_make_int64(env, sqlite3_last_insert_rowid(conn->connection));
}

#define db_cur_stat(stat, atom, place) do {\
    if (sqlite3_db_status(conn->connection, stat, &cur, &hw, 0) == SQLITE_OK) \
        enif_make_map_put(env, place, atom, enif_make_int64(env, cur), &place); \
    }while (0)

#define db_hw_stat(stat, atom, place) do {\
    if (sqlite3_db_status(conn->connection, stat, &cur, &hw, 0) == SQLITE_OK) \
        enif_make_map_put(env, place, atom, enif_make_int64(env, hw), &place); \
    }while (0)

static ERL_NIF_TERM sqlite_status_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t *conn;

    if (!enif_get_resource(env, argv[0], s_connection_resource, (void **) &conn))
        return make_badarg(env, NULL, 1, "not a connection reference");

    enif_mutex_lock(conn->mutex);
    if (conn->connection == NULL) {
        enif_mutex_unlock(conn->mutex);
        return make_badarg(env, NULL, 1, "connection closed");
    }

    ERL_NIF_TERM status = enif_make_new_map(env);
    ERL_NIF_TERM lookaside = enif_make_new_map(env);
    ERL_NIF_TERM cache = enif_make_new_map(env);
    int cur, hw;

    /* lookaside */
    if (sqlite3_db_status(conn->connection, SQLITE_DBSTATUS_LOOKASIDE_USED, &cur, &hw, 0) == SQLITE_OK) {
        enif_make_map_put(env, lookaside, am_used, enif_make_int64(env, cur), &lookaside);
        enif_make_map_put(env, lookaside, am_max, enif_make_int64(env, hw), &lookaside);
    }
    db_hw_stat(SQLITE_DBSTATUS_LOOKASIDE_HIT, am_hit, lookaside);
    db_hw_stat(SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE, am_miss_size, lookaside);
    db_hw_stat(SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL, am_miss_full, lookaside);

    /* cache */
    if (sqlite3_db_status(conn->connection, SQLITE_DBSTATUS_CACHE_SPILL, &cur, &hw, 0) == SQLITE_OK) {
        ERL_NIF_TERM tuple = enif_make_tuple2(env, enif_make_int64(env, cur), enif_make_int64(env, hw));
        enif_make_map_put(env, cache, am_spill, tuple, &cache);
    }
    db_cur_stat(SQLITE_DBSTATUS_CACHE_USED, am_used, cache);
    db_cur_stat(SQLITE_DBSTATUS_CACHE_USED_SHARED, am_shared, cache);
    db_cur_stat(SQLITE_DBSTATUS_CACHE_HIT, am_hit, cache);
    db_cur_stat(SQLITE_DBSTATUS_CACHE_MISS, am_miss, cache);
    db_cur_stat(SQLITE_DBSTATUS_CACHE_WRITE, am_write, cache);

    /* misc: schema, statement, ... */
    db_cur_stat(SQLITE_DBSTATUS_SCHEMA_USED, am_schema, status);
    db_cur_stat(SQLITE_DBSTATUS_STMT_USED, am_statement, status);
    db_cur_stat(SQLITE_DBSTATUS_DEFERRED_FKS, am_deferred_fks, status);

    /* now build the result */
    enif_make_map_put(env, status, am_lookaside_memory, lookaside, &status);
    enif_make_map_put(env, status, am_pager_cache_memory, cache, &status);

    enif_mutex_unlock(conn->mutex);

    return status;
}


#define statement_stat(op, atom) do {\
        enif_make_map_put(env, status, atom, enif_make_int64(env, sqlite3_stmt_status(stmt, op, 0)), &status);\
    }while (0)

/* Statement status */
static ERL_NIF_TERM sqlite_info_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_resource_t* stmt_res;

    if (!enif_get_resource(env, argv[0], s_statement_resource, (void **) &stmt_res))
        return make_badarg(env, NULL, 1, "not a prepared statement");

    /* close/1 may wipe the statement out, so take a lock */
    enif_mutex_lock(stmt_res->connection->mutex);

    sqlite3_stmt* stmt = stmt_res->reference->statement;

    ERL_NIF_TERM status = enif_make_new_map(env);
    statement_stat(SQLITE_STMTSTATUS_FULLSCAN_STEP, am_fullscan_step);
    statement_stat(SQLITE_STMTSTATUS_SORT, am_sort);
    statement_stat(SQLITE_STMTSTATUS_AUTOINDEX, am_autoindex);
    statement_stat(SQLITE_STMTSTATUS_VM_STEP, am_vm_step);
    statement_stat(SQLITE_STMTSTATUS_REPREPARE, am_reprepare);
    statement_stat(SQLITE_STMTSTATUS_RUN, am_run);
    /* statement_stat(SQLITE_STMTSTATUS_FILTER_MISS, am_filter_miss);
    statement_stat(SQLITE_STMTSTATUS_FILTER_HIT, am_filter_hit); */
    statement_stat(SQLITE_STMTSTATUS_MEMUSED, am_memory_used);

    enif_mutex_unlock(stmt_res->connection->mutex);
    return status;
}

/* NIF Exports */
static ErlNifFunc nif_funcs[] = {
    {"sqlite_open_nif", 2, sqlite_open_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_close_nif", 1, sqlite_close_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_status_nif", 1, sqlite_status_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_query_nif", 3, sqlite_query_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_prepare_nif", 3, sqlite_prepare_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_bind_nif", 2, sqlite_bind_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_step_nif", 2, sqlite_step_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_info_nif", 1, sqlite_info_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_describe_nif", 1, sqlite_describe_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_execute_nif", 2, sqlite_execute_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_monitor_nif", 2, sqlite_monitor_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_interrupt_nif", 1, sqlite_interrupt_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"sqlite_system_info_nif", 0, sqlite_system_info_nif},
    {"sqlite_get_last_insert_rowid_nif", 1, sqlite_get_last_insert_rowid_nif},

    {"sqlite_dirty_close_nif", 0, sqlite_dirty_close_nif, ERL_NIF_DIRTY_JOB_IO_BOUND}
};


/* memory management */
static int sqlmem_init(void* unused) {return 0;}
static void sqlmem_shutdown(void* unused) {}

static void* sqlmem_alloc(int size)
{
    ErlNifSInt64* alloc = (ErlNifSInt64*)enif_alloc(size + sizeof(ErlNifSInt64));
    *alloc = size;
    return alloc + 1;
}

static void sqlmem_free(void* ptr)
{
    ErlNifSInt64* alloc = (ErlNifSInt64*)ptr;
    enif_free(alloc - 1);
}

static void* sqlmem_realloc(void* ptr, int size)
{
    ErlNifSInt64* alloc = (ErlNifSInt64*)ptr;
    alloc = enif_realloc(alloc - 1, size + sizeof(ErlNifSInt64));
    *alloc = size;
    return alloc + 1;
}

static int sqlmem_roundup(int orig) 
{
    int bit = sizeof(ErlNifSInt64) - 1;
    return (orig + bit) & (~bit);
}

static int sqlmem_size(void* ptr)
{
    ErlNifSInt64* alloc = (ErlNifSInt64*)ptr;
    return alloc[-1];
}

/* Load/unload/reload */
static int load(ErlNifEnv *env, void** priv_data, ERL_NIF_TERM load_info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_true = enif_make_atom(env, "true");
    am_false = enif_make_atom(env, "false");

    int arity;
    const ERL_NIF_TERM* config;

    if (!enif_get_tuple(env, load_info, &arity, &config) ||
        !enif_get_local_pid(env, config[0], &s_delayed_close_pid) || 
        !enif_is_process_alive(env, &s_delayed_close_pid) ||
        (config[1] != am_true && config[1] != am_false))
        return -6;

    if (config[1] == am_true) {
        /* configure sqlite mutex & memory allocation */
        sqlite3_mem_methods mem;

        mem.xMalloc = sqlmem_alloc;
        mem.xFree = sqlmem_free;
        mem.xRealloc = sqlmem_realloc;
        mem.xRoundup = sqlmem_roundup;
        mem.xSize = sqlmem_size;
        mem.xInit = sqlmem_init;
        mem.xShutdown = sqlmem_shutdown;

        if (sqlite3_config(SQLITE_CONFIG_MALLOC, &mem) != SQLITE_OK)
            return -1;
    }

    /* use multi-threaded SQLite mode to enable explicit NIF mutex name */
    if (sqlite3_config(SQLITE_CONFIG_MULTITHREAD, NULL) != SQLITE_OK)
        return -2;

    if (sqlite3_initialize() != SQLITE_OK)
        return -3;

    am_general = enif_make_atom(env, "general");
    am_reason = enif_make_atom(env, "reason");
    am_badarg = enif_make_atom(env, "badarg");
    am_undefined = enif_make_atom(env, "undefined");
    am_position = enif_make_atom(env, "position");
    am_out_of_memory = enif_make_atom(env, "out_of_memory");
    am_sqlite_error = enif_make_atom(env, "sqlite_error");
    am_blob = enif_make_atom(env, "blob");
    am_done = enif_make_atom(env, "done");
    am_busy = enif_make_atom(env, "busy");

    am_mode = enif_make_atom(env, "mode");
    am_flags = enif_make_atom(env, "flags");
    am_uri = enif_make_atom(env, "uri");
    am_read_only = enif_make_atom(env, "read_only");
    am_read_write = enif_make_atom(env, "read_write");
    am_read_write_create = enif_make_atom(env, "read_write_create");
    am_shared = enif_make_atom(env, "shared");
    am_memory = enif_make_atom(env, "memory");
    am_busy_timeout = enif_make_atom(env, "busy_timeout");

    am_persistent = enif_make_atom(env, "persistent");
    am_no_vtab = enif_make_atom(env, "no_vtab");

    am_insert = enif_make_atom(env, "insert");
    am_update = enif_make_atom(env, "update");
    am_delete = enif_make_atom(env, "delete");

    am_lookaside_memory = enif_make_atom(env, "lookaside_memory");
    am_pager_cache_memory = enif_make_atom(env, "pager_cache_memory");
    am_schema = enif_make_atom(env, "schema");
    am_statement = enif_make_atom(env, "statement");
    am_deferred_fks = enif_make_atom(env, "deferred_fks");

    am_used = enif_make_atom(env, "used");
    am_max = enif_make_atom(env, "max");
    am_hit = enif_make_atom(env, "hit");
    am_miss_size = enif_make_atom(env, "miss_size");
    am_miss_full = enif_make_atom(env, "miss_full");
    am_shared = enif_make_atom(env, "shared");
    am_miss = enif_make_atom(env, "miss");
    am_write = enif_make_atom(env, "write");
    am_spill = enif_make_atom(env, "spill");

    am_fullscan_step = enif_make_atom(env, "fullscan_step");
    am_sort = enif_make_atom(env, "sort");
    am_autoindex = enif_make_atom(env, "autoindex");
    am_vm_step = enif_make_atom(env, "vm_step");
    am_reprepare = enif_make_atom(env, "reprepare");
    am_run = enif_make_atom(env, "run");
    am_filter_miss = enif_make_atom(env, "filter_miss");
    am_filter_hit = enif_make_atom(env, "filter_hit");
    am_memory_used = enif_make_atom(env, "memory_used");

    am_page_cache = enif_make_atom(env, "page_cache");
    am_malloc = enif_make_atom(env, "malloc");
    am_version = enif_make_atom(env, "version");

    ErlNifResourceType *rt;
    rt = enif_open_resource_type(env, NULL, "sqlite_connection", sqlite_connection_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
        sqlite3_shutdown();
        return -4;
    }
    s_connection_resource = rt;

    rt = enif_open_resource_type(env, NULL, "sqlite_statement", sqlite_statement_destroy, ERL_NIF_RT_CREATE, NULL);
    if (!rt) {
        sqlite3_shutdown();
        return -4;
    }
    s_statement_resource = rt;

    s_delayed_close_mutex = enif_mutex_create("sqlite_delayed_close");
    ASSERT(!s_delayed_close_mutex);
    s_delayed_close_queue = NULL;

    return 0;
}

static void unload(ErlNifEnv *env, void* priv_data)
{
    sqlite3_shutdown();
    enif_mutex_destroy(s_delayed_close_mutex);
}

static int upgrade(ErlNifEnv *env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

ERL_NIF_INIT(sqlite, nif_funcs, load, NULL, upgrade, unload);