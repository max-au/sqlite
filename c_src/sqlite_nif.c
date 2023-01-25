#include <string.h>

#include <erl_nif.h>

#include <sqlite3.h>

/* Not imported from erl_nif.h */
#define THE_NON_VALUE	0

/* Atom definitions (atoms cannot change dynamically) */
static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_general;
static ERL_NIF_TERM am_extra;
static ERL_NIF_TERM am_badarg;
static ERL_NIF_TERM am_undefined;

static ERL_NIF_TERM am_out_of_memory;   /* out of memory */
static ERL_NIF_TERM am_sqlite_error;    /* sqlite-specific error code */

/* database open modes */
static ERL_NIF_TERM am_mode;
static ERL_NIF_TERM am_uri;
static ERL_NIF_TERM am_read_only;
static ERL_NIF_TERM am_read_write;
static ERL_NIF_TERM am_read_write_create;
static ERL_NIF_TERM am_in_memory;

/* atoms for notifications */
static ERL_NIF_TERM am_insert;
static ERL_NIF_TERM am_update;
static ERL_NIF_TERM am_delete;

/* connection status atoms */
static ERL_NIF_TERM am_lookaside;
static ERL_NIF_TERM am_cache;
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

static ERL_NIF_TERM am_fullscan_step;
static ERL_NIF_TERM am_sort;
static ERL_NIF_TERM am_autoindex;
static ERL_NIF_TERM am_vm_step;
static ERL_NIF_TERM am_reprepare;
static ERL_NIF_TERM am_run;
static ERL_NIF_TERM am_filter_miss;
static ERL_NIF_TERM am_filter_hit;
static ERL_NIF_TERM am_memory_used;

static ERL_NIF_TERM am_page_cache;
static ERL_NIF_TERM am_malloc;

/* Resources: connection */
static ErlNifResourceType *sqlite_connection_resource;

/* connection is not using any mutex or other way to
 * synchronise access, relying on SQLite to do that
 */
typedef struct {
    sqlite3* connection;
    ErlNifPid tracer;
    ERL_NIF_TERM ref;
} connection_t;

static void sqlite_connection_destroy(ErlNifEnv *env, void *arg)
{
    connection_t *conn = (connection_t*)arg;
    /* ignore SQLITE_MISUSE or any other error code */
    sqlite3_close_v2(conn->connection);
}

/* Resources: statement */
static ErlNifResourceType *sqlite_statement_resource;

typedef struct {
    sqlite3_stmt* statement;
    connection_t* connection;
} statement_t;

static void sqlite_statement_destroy(ErlNifEnv *env, void *arg)
{
    statement_t *stmt = (statement_t*)arg;
    sqlite3_finalize(stmt->statement);
    enif_release_resource(stmt->connection);
}

static ERL_NIF_TERM make_binary(ErlNifEnv *env, const char* str, unsigned int len) {
    ErlNifBinary bin;
    if (!enif_alloc_binary(len, &bin))
        return THE_NON_VALUE;
    memcpy(bin.data, str, len);
    ERL_NIF_TERM bin_term = enif_make_binary(env, &bin);
    enif_release_binary(&bin);
    return bin_term;
}

/* Creates a tuple to simulate EEP-54 exceptions in the Erlang code:
 * {error, Reason, ErrorInfoMap}
 * Erlang code simply calls erlang:error(Reason, [...args...], [{error_info, ErrorInfoMap}]).
 */
static ERL_NIF_TERM make_extended_error_ex(ErlNifEnv *env, ERL_NIF_TERM reason,
    const char* general, int arg_pos, const char* arg_error, ERL_NIF_TERM extra) {
    ERL_NIF_TERM error_info_map = enif_make_new_map(env);

    if (general) {
        ERL_NIF_TERM general_bin = make_binary(env, general, strlen(general));
        if (general_bin == THE_NON_VALUE)
            return enif_raise_exception(env, am_out_of_memory);
        enif_make_map_put(env, error_info_map, am_general, general_bin, &error_info_map);
    }

    if (arg_pos > 0 && arg_error) {
        ERL_NIF_TERM arg_bin = make_binary(env, arg_error, strlen(arg_error));
        if (arg_bin == THE_NON_VALUE)
            return enif_raise_exception(env, am_out_of_memory);
        enif_make_map_put(env, error_info_map, enif_make_int(env, arg_pos), arg_bin, &error_info_map);
    }

    if (extra != THE_NON_VALUE)
        enif_make_map_put(env, error_info_map, am_extra, extra, &error_info_map);

    return enif_make_tuple3(env, am_error, reason, error_info_map);
}

static ERL_NIF_TERM make_extended_error(ErlNifEnv *env, ERL_NIF_TERM reason,
    const char* general, int arg_pos, const char* arg_error) {
    return make_extended_error_ex(env, reason, general, arg_pos, arg_error, THE_NON_VALUE);
}

static ERL_NIF_TERM make_sqlite_error(ErlNifEnv *env, int sqlite_error, const char* operation) {
    /* int error_offset = sqlite3_error_offset(db); */
    ERL_NIF_TERM operation_bin = operation ? make_binary(env, operation, strlen(operation)) : THE_NON_VALUE;
    ERL_NIF_TERM reason = enif_make_tuple2(env, am_sqlite_error, enif_make_int(env, sqlite_error));
    return make_extended_error_ex(env, reason, sqlite3_errstr(sqlite_error), -1, NULL, operation_bin);
}

/* Connection (opens the database) */
static ERL_NIF_TERM sqlite_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t *conn;
    char *filename;
    ErlNifBinary fn_bin;
    ERL_NIF_TERM mode;
    int v2flags = 0;

    /* type checks */
    if (!enif_is_map(env, argv[1]))
        return make_extended_error(env, am_badarg, NULL, 2, "not a map");

    if (enif_get_map_value(env, argv[1], am_mode, &mode)) {
        if (mode == am_read_only)
            v2flags = SQLITE_OPEN_READONLY;
        else if (mode == am_read_write)
            v2flags = SQLITE_OPEN_READWRITE;
        else if (mode == am_read_write_create)
            v2flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        else if (mode == am_in_memory)
            v2flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY;
        else
            return make_extended_error(env, am_badarg, NULL, 2, "unsupported mode");
    } else
        v2flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE; /* default mode */

    mode |= SQLITE_OPEN_EXRESCODE;

    /* try filename as binary */
    if (enif_inspect_iolist_as_binary(env, argv[0], &fn_bin)) {
        /* Erlang binaries are not NULL-terminated, so make a copy */
        filename = enif_alloc(fn_bin.size + 1);
        if (!filename)
            return enif_raise_exception(env, am_out_of_memory);
        strncpy(filename, (const char*)fn_bin.data, fn_bin.size);
        filename[fn_bin.size] = 0;
    } else
        return make_extended_error(env, am_badarg, NULL, 1, "invalid file name");

    /* Initialize the resource */
    conn = enif_alloc_resource(sqlite_connection_resource, sizeof(connection_t));
    if (!conn) {
        enif_free(filename);
        return enif_raise_exception(env, am_out_of_memory);
    }

     /* Open the database. */
    int ret = sqlite3_open_v2(filename, &conn->connection, v2flags, NULL);
    enif_free(filename);
    if (ret != SQLITE_OK) {
        if (conn->connection)
            sqlite3_close_v2(conn->connection);
        enif_release_resource(conn);
        return make_sqlite_error(env, ret, "error opening connection");
    }

    ERL_NIF_TERM connection = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return connection;
}

static ERL_NIF_TERM sqlite_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t *conn;
    int ret;

    if (!enif_get_resource(env, argv[0], sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");

    /* sqlite3_close_v2 fails if transaction is in progress */
    if (!sqlite3_get_autocommit(conn->connection)) {
        ret = sqlite3_exec(conn->connection, "ROLLBACK;", NULL, NULL, NULL);
        if (ret != SQLITE_OK)
            return make_sqlite_error(env, ret, "unable to rollback current transaction");
    }

    ret = sqlite3_close_v2(conn->connection);
    if (ret == SQLITE_MISUSE)
        return make_extended_error(env, am_badarg, NULL, 1, "connection already closed");
    if (ret != SQLITE_OK)
        return make_sqlite_error(env, ret, "error closing connection");

    return am_ok;
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

    if (!enif_get_resource(env, argv[0], sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");

    if (conn->connection == NULL)
        return make_extended_error(env, am_badarg, NULL, 1, "connection closed");

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
    enif_make_map_put(env, status, am_lookaside, lookaside, &status);
    enif_make_map_put(env, status, am_cache, cache, &status);

    return status;
}

/* internal function to prepare a new statement */
static ERL_NIF_TERM prepare(ErlNifEnv *env, ERL_NIF_TERM connection, ERL_NIF_TERM query, statement_t* stmt) {
    connection_t *conn;

    if (!enif_get_resource(env, connection, sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");

    ErlNifBinary query_bin;
    if (!enif_inspect_iolist_as_binary(env, query, &query_bin))
        return make_extended_error(env, am_badarg, NULL, 2, "not an iolist");

    int ret = sqlite3_prepare_v2(conn->connection, (char*)query_bin.data, query_bin.size, &stmt->statement, NULL);
    if (ret != SQLITE_OK)
        return make_sqlite_error(env, ret, "error preparing statement");

    enif_keep_resource(conn);
    stmt->connection = conn;

    return THE_NON_VALUE;
}

/* bind & execute */
static ERL_NIF_TERM bind_and_execute(ErlNifEnv *env, sqlite3_stmt* stmt, ERL_NIF_TERM params) {
    ErlNifSInt64 int_val;
    double double_val;
    ErlNifBinary bin;
    ERL_NIF_TERM param, result;
    int ret, column = 1;

    /* bind arguments passed */
    while (!enif_is_empty_list(env, params)) {
        if (!enif_get_list_cell(env, params, &param, &params))
            return make_extended_error_ex(env, am_badarg, NULL, 3, "invalid bindings list", enif_make_int(env, column));

        /* supported types: undefined (atom), integer, blob, text, float */
        if (param == am_undefined) {
            /* NULL */
            ret = sqlite3_bind_null(stmt, column);
        } else if (enif_inspect_iolist_as_binary(env, param, &bin)) {
            /* binary blob */
            ret = sqlite3_bind_text64(stmt, column, (char*)bin.data, bin.size, SQLITE_TRANSIENT, SQLITE_UTF8);
        } else if (enif_is_number(env, param)) {
            /* integer or float */
            if (enif_get_int64(env, param, &int_val)) {
                ret = sqlite3_bind_int64(stmt, column, int_val);
            } else if (enif_get_double(env, param, &double_val))
                ret = sqlite3_bind_double(stmt, column, double_val);
            else {
                ret = -1; /* TODO: implement better error reporting */
            }
        } else {
            ret = -1;
        }

        if (ret != SQLITE_OK) {
            ERL_NIF_TERM reason = enif_make_tuple2(env, am_sqlite_error, enif_make_int(env, ret));
            ERL_NIF_TERM extra = enif_make_tuple2(env, make_binary(env, "failed to bind argument", 23),
                enif_make_int(env, column));
            return make_extended_error_ex(env, reason, sqlite3_errstr(ret), -1, NULL, extra);
        }

        column++;
    }

    /* run the query */
    ret = sqlite3_step(stmt);
    switch (ret) {
        case SQLITE_DONE:
            result = am_ok;
            break;
        case SQLITE_ROW:
            result = enif_make_list(env, 0);
            do {
                int column_count = sqlite3_column_count(stmt);
                int column_type, int_val;
                double double_val;
                unsigned int size;
                ERL_NIF_TERM row;
                ERL_NIF_TERM values[column_count]; /* C99 supports this, but it's discouraged, maybe use enif_alloc? */
                for (int i=0; i<column_count; i++) {
                    column_type = sqlite3_column_type(stmt, i);
                    switch (column_type) {
                        case SQLITE_NULL:
                            result = values[i] = am_undefined;
                            break;
                        case SQLITE_BLOB:
                            size = sqlite3_column_bytes(stmt, i);
                            const char* blob = (const char*)sqlite3_column_blob(stmt, i);
                            values[i] = make_binary(env, blob, size);
                            break;
                        case SQLITE_TEXT:
                            size = sqlite3_column_bytes(stmt, i);
                            const char* text = (const char*)sqlite3_column_text(stmt, i);
                            /* TODO: decide between binary and the Erlang string */
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
                    }
                }
                row = enif_make_tuple_from_array(env, values, column_count);
                result = enif_make_list_cell(env, row, result);
                ret = sqlite3_step(stmt);
            } while (ret == SQLITE_ROW);
            /* check if it was error that bailed out */
            if (ret != SQLITE_DONE)
                result = make_sqlite_error(env, ret, "error receiving rows");
            break;
        default:
            result = make_sqlite_error(env, ret, "error running query");
            break;
    }

    return result;
}

/* Query preparation */
static ERL_NIF_TERM sqlite_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    /* Initialize the resource */
    statement_t* statement = enif_alloc_resource(sqlite_statement_resource, sizeof(statement_t));
    if (!statement)
        return enif_raise_exception(env, am_out_of_memory);

    ERL_NIF_TERM reason = prepare(env, argv[0], argv[1], statement);
    if (reason != THE_NON_VALUE) {
        enif_release_resource(statement);
        return reason;
    }

    ERL_NIF_TERM prepared_statement = enif_make_resource(env, statement);
    enif_release_resource(statement);

    return prepared_statement;
}

#define statement_stat(op, atom) enif_make_map_put(env, status, atom, enif_make_int64(env, sqlite3_stmt_status(stmt->statement, op, 0)), &status);

/* Statement status */
static ERL_NIF_TERM sqlite_info_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_t* stmt;

    if (!enif_get_resource(env, argv[0], sqlite_statement_resource, (void **) &stmt))
        return make_extended_error(env, am_badarg, NULL, 1, "not a prepared statement");

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

    return status;
}

/* Query execution */
static ERL_NIF_TERM sqlite_execute_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_t* stmt;

    if (!enif_get_resource(env, argv[0], sqlite_statement_resource, (void **) &stmt))
        return make_extended_error(env, am_badarg, NULL, 1, "not a prepared statement");

    int ret = sqlite3_reset(stmt->statement);
    if (ret != SQLITE_OK)
        return make_sqlite_error(env, ret, "error resetting statement");

    return bind_and_execute(env, stmt->statement, argv[1]);
}

static ERL_NIF_TERM sqlite_describe_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_t* stmt;
    int count;
    const char* name;
    const char* type;
    ERL_NIF_TERM columns, name_bin, type_bin;

    if (!enif_get_resource(env, argv[0], sqlite_statement_resource, (void **) &stmt))
        return make_extended_error(env, am_badarg, NULL, 1, "not a prepared statement");

    count = sqlite3_column_count(stmt->statement);

    columns = enif_make_list(env, 0);

    for(int i=count-1; i>=0; i--) {
        name = sqlite3_column_name(stmt->statement, i);
        type = sqlite3_column_decltype(stmt->statement, i);
        if (name && !type)
            return enif_raise_exception(env, am_out_of_memory);
        name_bin = make_binary(env, name, strlen(name));
        type_bin = make_binary(env, type, strlen(type));
        columns = enif_make_list_cell(env, enif_make_tuple2(env, name_bin, type_bin), columns);
    }

    return columns;
}

/* Combined preparation and execution */
static ERL_NIF_TERM sqlite_query_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    statement_t stmt;
    /* create a new throw-away statement that's not accounted as a resource */
    ERL_NIF_TERM reason = prepare(env, argv[0], argv[1], &stmt);

    /* after this, ensure no extra returns can happen without freeing the stmt */
    if (reason != THE_NON_VALUE)
        return reason;

    ERL_NIF_TERM result = bind_and_execute(env, stmt.statement, argv[2]);

    /* cleanup */
    sqlite3_finalize(stmt.statement);
    enif_release_resource(stmt.connection);

    return result;
}

static void update_callback(void *arg, int op_type, char const *db, char const *table, sqlite3_int64 rowid)
{
    connection_t *conn = (connection_t *)arg;
    if (!conn)
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
            op = am_delete;
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
    if(!enif_get_resource(env, argv[0], sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");

    if (argv[1] == am_undefined)
        sqlite3_update_hook(conn->connection, NULL, conn);
    else 
        if (!enif_get_local_pid(env, argv[1], &conn->tracer))
            return make_extended_error(env, am_badarg, NULL, 2, "not a pid");
        else {
            conn->ref = argv[0];
            sqlite3_update_hook(conn->connection, update_callback, conn);
            return conn->ref;
        }

    return am_ok;
}

static ERL_NIF_TERM sqlite_interrupt_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    connection_t* conn;
    if(!enif_get_resource(env, argv[0], sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");

    sqlite3_interrupt(conn->connection);
    return am_ok;
}

static ERL_NIF_TERM sqlite_system_info_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM info = enif_make_new_map(env);
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
    return info;
}

static ERL_NIF_TERM sqlite_get_last_insert_rowid_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    connection_t* conn;
    if(!enif_get_resource(env, argv[0], sqlite_connection_resource, (void **) &conn))
        return make_extended_error(env, am_badarg, NULL, 1, "not a connection reference");
    sqlite3_interrupt(conn->connection);
    return enif_make_int64(env, sqlite3_last_insert_rowid(conn->connection));
}


/* NIF Exports */
static ErlNifFunc nif_funcs[] = {
    {"sqlite_open_nif", 2, sqlite_open_nif},
    {"sqlite_close_nif", 1, sqlite_close_nif},
    {"sqlite_status_nif", 1, sqlite_status_nif},
    {"sqlite_query_nif", 3, sqlite_query_nif},
    {"sqlite_prepare_nif", 2, sqlite_prepare_nif},
    {"sqlite_info_nif", 1, sqlite_info_nif},
    {"sqlite_describe_nif", 1, sqlite_describe_nif},
    {"sqlite_execute_nif", 2, sqlite_execute_nif},
    {"sqlite_monitor_nif", 2, sqlite_monitor_nif},
    {"sqlite_interrupt_nif", 1, sqlite_interrupt_nif},
    {"sqlite_system_info_nif", 0, sqlite_system_info_nif},
    {"sqlite_get_last_insert_rowid_nif", 1, sqlite_get_last_insert_rowid_nif}
};


/* memory management */
static int sqlmem_init(void*) {return 0;}
static void sqlmem_shutdown(void*) {}
static void* sqlmem_alloc(int size)
{
    int* alloc = (int*)enif_alloc(size + sizeof(int));
    *alloc = size;
    return alloc + 1;
}

static void sqlmem_free(void* ptr)
{
    int* alloc = (int*)ptr;
    enif_free(alloc - 1);
}

static void* sqlmem_realloc(void* ptr, int size)
{
    int* alloc = (int*)ptr;
    alloc = enif_realloc(alloc - 1, size + sizeof(int));
    *alloc = size;
    return alloc + 1;
}

static int sqlmem_roundup(int orig) {return (orig + 7) & (~7);}

static int sqlmem_size(void* ptr)
{
    int* alloc = (int*)ptr;
    return alloc[-1];
}

/* Load/unload/reload */
static int load(ErlNifEnv *env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceType *rt;

    if (!sqlite3_threadsafe())
        return -1;

    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_general = enif_make_atom(env, "general");
    am_extra = enif_make_atom(env, "extra");
    am_badarg = enif_make_atom(env, "badarg");
    am_undefined = enif_make_atom(env, "undefined");

    am_out_of_memory = enif_make_atom(env, "out_of_memory");
    am_sqlite_error = enif_make_atom(env, "sqlite_error");

    am_mode = enif_make_atom(env, "mode");
    am_uri = enif_make_atom(env, "uri");
    am_read_only = enif_make_atom(env, "read_only");
    am_read_write = enif_make_atom(env, "read_write");
    am_read_write_create = enif_make_atom(env, "read_write_create");
    am_in_memory = enif_make_atom(env, "in_memory");

    am_insert = enif_make_atom(env, "insert");
    am_update = enif_make_atom(env, "update");
    am_delete = enif_make_atom(env, "delete");

    am_lookaside = enif_make_atom(env, "lookaside");
    am_cache = enif_make_atom(env, "cache");
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


    rt = enif_open_resource_type(env, NULL, "sqlite_connection", sqlite_connection_destroy,
        ERL_NIF_RT_CREATE, NULL);
    if (!rt)
        return -2;
    sqlite_connection_resource = rt;

    rt = enif_open_resource_type(env, NULL, "sqlite_statement", sqlite_statement_destroy,
        ERL_NIF_RT_CREATE, NULL);
    if (!rt)
        return -3;
    sqlite_statement_resource = rt;

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
        return -4;

    if (sqlite3_initialize() != SQLITE_OK)
        return -5;

    return 0;
}

static void unload(ErlNifEnv *env, void* priv_data)
{
    sqlite3_shutdown();
}

static int upgrade(ErlNifEnv *env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

ERL_NIF_INIT(sqlite, nif_funcs, load, NULL, upgrade, unload);