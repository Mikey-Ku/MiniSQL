#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include "miniqlite.h"

typedef void (*CommandHandler)(Database*, char*);

typedef struct {
    const char* keyword;
    CommandHandler fn;
} Command;

static void handle_create(Database* db, char* input);
static void handle_insert(Database* db, char* input);
static void handle_select(Database* db, char* input);
static void handle_update(Database* db, char* input);
static void handle_delete(Database* db, char* input);
static void handle_drop(Database* db, char* input);

static const Command command_table[] = {
    { "CREATE TABLE", handle_create },
    { "INSERT INTO",  handle_insert },
    { "SELECT",       handle_select },
    { "UPDATE",       handle_update },
    { "DELETE FROM",  handle_delete },
    { "DROP TABLE",   handle_drop },
    { NULL, NULL }
};

static int handle_meta(Database* db, char* line);
static void parse_create_table(Database* db, char* line);
static void parse_insert(Database* db, char* line);
static void parse_select(Database* db, char* line);
static void parse_delete(Database* db, char* line);
static void parse_update(Database* db, char* line);

static char* trim(char* s) {
    while (isspace((unsigned char)*s)) s++;
    if (*s == 0) return s;
    char* end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return s;
}

/* Parse comma-separated values supporting quoted strings. */
static char** parse_values_list(char* s, int* out_count) {
    int cap = 8;
    int n = 0;
    char** vals = malloc(sizeof(char*) * cap);
    if (!vals) return NULL;

    char* p = s;
    while (*p) {
        while (isspace((unsigned char)*p)) p++;
        if (*p == '\0') break;

        char* val_start = NULL;
        char* val_end = NULL;

        if (*p == '"') {
            p++;
            val_start = p;
            while (*p && *p != '"') p++;
            val_end = p;
            if (*p == '"') p++;
        } else {
            val_start = p;
            while (*p && *p != ',') p++;
            val_end = p;
        }

        while (val_end > val_start && isspace((unsigned char)*(val_end - 1))) val_end--;

        int len = (int)(val_end - val_start);
        char* val = malloc(len + 1);
        if (!val) {
            for (int i = 0; i < n; i++) free(vals[i]);
            free(vals);
            return NULL;
        }
        memcpy(val, val_start, len);
        val[len] = '\0';

        if (n >= cap) {
            cap *= 2;
            char** tmp = realloc(vals, sizeof(char*) * cap);
            if (!tmp) {
                for (int i = 0; i < n; i++) free(vals[i]);
                free(vals);
                free(val);
                return NULL;
            }
            vals = tmp;
        }
        vals[n++] = val;

        while (*p && (*p == ',')) p++;
    }

    *out_count = n;
    return vals;
}

/* Parse simple condition: col = value */
static int parse_condition_eq(char* s, char* col_buf, size_t col_buf_sz,
                              char* val_buf, size_t val_buf_sz) {
    char* eq = strchr(s, '=');
    if (!eq) return 0;
    *eq = '\0';

    char* left = trim(s);
    char* right = trim(eq + 1);

    if (strlen(left) == 0) return 0;
    strncpy(col_buf, left, col_buf_sz - 1);
    col_buf[col_buf_sz - 1] = '\0';

    if (*right == '"') {
        right++;
        char* endq = strchr(right, '"');
        if (endq) *endq = '\0';
    }
    strncpy(val_buf, right, val_buf_sz - 1);
    val_buf[val_buf_sz - 1] = '\0';

    return 1;
}

//Comand execution dispatcher
int execute_command(Database* db, char* input) {
    char* line = trim(input);
    if (*line == '\0') return 0;

    if (line[0] == '.') {
        return handle_meta(db, line);
    }

    // Function-pointer dispatch loop
    for (int i = 0; command_table[i].keyword != NULL; i++) {
        const char* keyword = command_table[i].keyword;
        size_t len = strlen(keyword);
        if (strncmp(line, keyword, len) == 0) {
            command_table[i].fn(db, line);
            return 0;
        }
    }

    printf("Unrecognized command: %s\n", line);
    return 0;
}

// Meta-command handler
static int handle_meta(Database* db, char* line) {
    if (strcmp(line, ".tables") == 0) {
        list_tables(db);
        return 0;
    }
    if (strncmp(line, ".save", 5) == 0) {
        char fname[256];
        if (sscanf(line + 5, "%255s", fname) == 1) {
            if (save_database(db, fname))
                printf("Saved to '%s'.\n", fname);
            else
                printf("Error saving to '%s'.\n", fname);
        } else {
            printf("Usage: .save <filename>\n");
        }
        return 0;
    }
    if (strcmp(line, ".meminfo") == 0) {
    static int global_var = 42;   // global/static region
    int local_var = 123;          // stack
    int *heap_var = malloc(sizeof(int)); // heap
    *heap_var = 999;

    printf("Memory layout demonstration:\n");
    printf("  Address of code (function handle_meta): %p\n", (void*)&handle_meta);
    printf("  Address of global/static variable:      %p\n", (void*)&global_var);
    printf("  Address of heap allocation:             %p\n", (void*)heap_var);
    printf("  Address of local variable:              %p\n", (void*)&local_var);

    printf("\nInterpretation:\n");
    printf("  - Code (functions) lives in the lowest address region.\n");
    printf("  - Globals/static vars are in a fixed data region.\n");
    printf("  - Heap allocations come from the dynamic memory area.\n");
    printf("  - Stack variables are near the top and change each call.\n");

    free(heap_var);
    return 0;
    }
    if (strncmp(line, ".bench", 6) == 0) {
        char op[32];
        int n = 1000; // default count
        if (sscanf(line + 6, "%31s %d", op, &n) < 1) {
            printf("Usage: .bench insert <count>\n");
            return 0;
        }

        if (strcmp(op, "insert") == 0) {
            clock_t start = clock();
            Table* t = find_table(db, "bench");
            if (!t) {
                ColumnDef cols[2] = { {"id", COL_INT}, {"value", COL_TEXT} };
                create_table(db, "bench", cols, 2);
                t = find_table(db, "bench");
            }

            char idbuf[32], valbuf[32];
            for (int i = 0; i < n; i++) {
                snprintf(idbuf, sizeof(idbuf), "%d", i);
                snprintf(valbuf, sizeof(valbuf), "%d", rand() % 1000);
                char* vals[2] = { idbuf, valbuf };
                insert_row(db, "bench", vals, 2);
            }
            clock_t end = clock();
            double ms = 1000.0 * (end - start) / CLOCKS_PER_SEC;
            printf("Inserted %d rows in %.2f ms (%s mode)\n",
                n, ms, db->binary_mode ? "binary" : "text");
            return 0;
        }

        printf("Unknown .bench operation: %s\n", op);
        return 0;
    }

    if (strncmp(line, ".load", 5) == 0) {
        char fname[256];
        if (sscanf(line + 5, "%255s", fname) == 1) {
            if (load_database(db, fname))
                printf("Loaded from '%s'.\n", fname);
            else
                printf("Error loading from '%s'.\n", fname);
        } else {
            printf("Usage: .load <filename>\n");
        }
        return 0;
    }
    if (strncmp(line, ".columnstore", 12) == 0) {
    char mode[16];
    if (sscanf(line + 12, "%15s", mode) == 1) {
        if (strcmp(mode, "on") == 0) {
            db->column_store = 1;
            printf("Column-major storage mode ON.\n");
        } else if (strcmp(mode, "off") == 0) {
            db->column_store = 0;
            printf("Row-major storage mode ON.\n");
        } else {
            printf("Usage: .columnstore [on|off]\n");
        }
    } else {
        printf("Current storage mode: %s\n", db->column_store ? "column-major" : "row-major");
    }
    return 0;
    }
    if (strcmp(line, ".meminfo") == 0) {
    static int global_var = 42;   // global/static region
    int local_var = 123;          // stack
    int *heap_var = malloc(sizeof(int)); // heap
    *heap_var = 999;

    printf("Memory layout demonstration:\n");
    printf("  Address of code (function handle_meta): %p\n", (void*)&handle_meta);
    printf("  Address of global/static variable:      %p\n", (void*)&global_var);
    printf("  Address of heap allocation:             %p\n", (void*)heap_var);
    printf("  Address of local variable:              %p\n", (void*)&local_var);

    printf("\nInterpretation:\n");
    printf("  - Code (functions) lives in the lowest address region.\n");
    printf("  - Globals/static vars are in a fixed data region.\n");
    printf("  - Heap allocations come from the dynamic memory area.\n");
    printf("  - Stack variables are near the top and change each call.\n");

    free(heap_var);
    return 0;
    }
    if (strncmp(line, ".binary", 7) == 0) {
    char mode[16];
    if (sscanf(line + 7, "%15s", mode) == 1) {
        if (strcmp(mode, "on") == 0) {
            db->binary_mode = 1;
            printf("Binary storage mode ON.\n");
        } else if (strcmp(mode, "off") == 0) {
            db->binary_mode = 0;
            printf("Binary storage mode OFF.\n");
        } else {
            printf("Usage: .binary [on|off]\n");
        }
    } else {
        printf("Current binary mode: %s\n", db->binary_mode ? "on" : "off");
    }
    return 0;
    }
    if (strcmp(line, ".exit") == 0 || strcmp(line, ".quit") == 0) {
        return 1; // signal exit
    }

    printf("Unrecognized meta-command: %s\n", line);
    return 0;
}

/* ============================================================
   SQL PARSER FUNCTIONS
   ============================================================ */

static void parse_create_table(Database* db, char* line) {
    // CREATE TABLE name (col TYPE, col TYPE, ...);
    char* p = line + strlen("CREATE TABLE");
    while (isspace((unsigned char)*p)) p++;

    char tname[MAX_NAME_LEN];
    int ni = 0;
    while (*p && !isspace((unsigned char)*p) && *p != '(' && ni < MAX_NAME_LEN - 1) {
        tname[ni++] = *p++;
    }
    tname[ni] = '\0';

    if (tname[0] == '\0') {
        printf("Syntax error: missing table name.\n");
        return;
    }

    while (isspace((unsigned char)*p)) p++;
    if (*p != '(') {
        printf("Syntax error: expected '('.\n");
        return;
    }
    p++;

    char* end_paren = strrchr(p, ')');
    if (!end_paren) {
        printf("Syntax error: missing ')'.\n");
        return;
    }
    *end_paren = '\0';

    int cap = 8;
    int num_cols = 0;
    ColumnDef* cols = malloc(sizeof(ColumnDef) * cap);
    if (!cols) return;

    char* tok = strtok(p, ",");
    while (tok) {
        char* def = trim(tok);
        if (*def) {
            char cname[MAX_NAME_LEN];
            char ctype[32];
            if (sscanf(def, "%63s %31s", cname, ctype) != 2) {
                printf("Syntax error in column definition: '%s'\n", def);
                free(cols);
                return;
            }

            if (num_cols >= cap) {
                cap *= 2;
                ColumnDef* tmp = realloc(cols, sizeof(ColumnDef) * cap);
                if (!tmp) {
                    free(cols);
                    return;
                }
                cols = tmp;
            }

            strncpy(cols[num_cols].name, cname, MAX_NAME_LEN - 1);
            cols[num_cols].name[MAX_NAME_LEN - 1] = '\0';
            cols[num_cols].type = parse_column_type(ctype);
            num_cols++;
        }
        tok = strtok(NULL, ",");
    }

    if (num_cols == 0) {
        printf("Syntax error: no columns.\n");
        free(cols);
        return;
    }

    create_table(db, tname, cols, num_cols);
    free(cols);
}

static void parse_insert(Database* db, char* line) {
    // INSERT INTO name VALUES (v1, v2, ...);
    char* p = line + strlen("INSERT INTO");
    while (isspace((unsigned char)*p)) p++;

    char tname[MAX_NAME_LEN];
    int ni = 0;
    while (*p && !isspace((unsigned char)*p) && *p != '(' && ni < MAX_NAME_LEN - 1) {
        tname[ni++] = *p++;
    }
    tname[ni] = '\0';

    if (tname[0] == '\0') {
        printf("Syntax error: missing table name in INSERT.\n");
        return;
    }

    char* values_kw = strstr(p, "VALUES");
    if (!values_kw) {
        printf("Syntax error: expected VALUES.\n");
        return;
    }

    char* lp = strchr(values_kw, '(');
    char* rp = strrchr(values_kw, ')');
    if (!lp || !rp || rp <= lp + 1) {
        printf("Syntax error: invalid VALUES list.\n");
        return;
    }
    *rp = '\0';
    char* vals_str = lp + 1;

    int count = 0;
    char** vals = parse_values_list(vals_str, &count);
    if (!vals) {
        printf("Error parsing values.\n");
        return;
    }

    insert_row(db, tname, vals, count);
    for (int i = 0; i < count; i++) free(vals[i]);
    free(vals);
}

static void parse_select(Database* db, char* line) {
    char* from_kw = strstr(line, "FROM");
    if (!from_kw) {
        printf("Syntax error: missing FROM.\n");
        return;
    }

    char select_part[512] = {0};
    char from_part[512] = {0};

    size_t sp_len = from_kw - (line + strlen("SELECT"));
    strncpy(select_part, line + strlen("SELECT"), sp_len);
    select_part[sp_len] = '\0';
    strcpy(from_part, from_kw + strlen("FROM"));

    char* cols_str = trim(select_part);
    char* rest = trim(from_part);

    if (*cols_str == '\0') {
        printf("Syntax error: missing columns in SELECT.\n");
        return;
    }

    char tname[MAX_NAME_LEN];
    int i = 0;
    while (*rest && !isspace((unsigned char)*rest) && *rest != ';' && i < MAX_NAME_LEN - 1) {
        tname[i++] = *rest++;
    }
    tname[i] = '\0';
    rest = trim(rest);

    if (tname[0] == '\0') {
        printf("Syntax error: missing table name in SELECT.\n");
        return;
    }

    char* where_kw = strstr(rest, "WHERE");
    if (!where_kw) {
        if (strcmp(cols_str, "*") == 0) {
            select_all(db, tname);
        } else {
            int cap = 8;
            int n = 0;
            char** cols = malloc(sizeof(char*) * cap);
            if (!cols) return;

            char* tok = strtok(cols_str, ",");
            while (tok) {
                char* c = trim(tok);
                if (*c) {
                    if (n >= cap) {
                        cap *= 2;
                        char** tmp = realloc(cols, sizeof(char*) * cap);
                        if (!tmp) {
                            for (int k = 0; k < n; k++) free(cols[k]);
                            free(cols);
                            return;
                        }
                        cols = tmp;
                    }
                    cols[n++] = str_duplicate(c);
                }
                tok = strtok(NULL, ",");
            }

            select_columns(db, tname, cols, n);
            for (int k = 0; k < n; k++) free(cols[k]);
            free(cols);
        }
    } else {
        *where_kw = '\0';
        char* after_where = where_kw + strlen("WHERE");
        after_where = trim(after_where);

        char col[64];
        char val[256];
        if (!parse_condition_eq(after_where, col, sizeof(col), val, sizeof(val))) {
            printf("Syntax error in WHERE clause.\n");
            return;
        }

        int cap = 8;
        int n = 0;
        char** cols = malloc(sizeof(char*) * cap);
        if (!cols) return;

        if (strcmp(cols_str, "*") == 0) {
            Table* t = find_table(db, tname);
            if (!t) { free(cols); return; }
            cap = t->num_columns;
            cols = realloc(cols, sizeof(char*) * cap);
            for (int c = 0; c < t->num_columns; c++) {
                cols[n++] = str_duplicate(t->columns[c].name);
            }
        } else {
            char* tok = strtok(cols_str, ",");
            while (tok) {
                char* c = trim(tok);
                if (*c) {
                    if (n >= cap) {
                        cap *= 2;
                        char** tmp = realloc(cols, sizeof(char*) * cap);
                        if (!tmp) {
                            for (int k = 0; k < n; k++) free(cols[k]);
                            free(cols);
                            return;
                        }
                        cols = tmp;
                    }
                    cols[n++] = str_duplicate(c);
                }
                tok = strtok(NULL, ",");
            }
        }

        select_where_eq(db, tname, cols, n, col, val);
        for (int k = 0; k < n; k++) free(cols[k]);
        free(cols);
    }
}

static void parse_delete(Database* db, char* line) {
    char* p = line + strlen("DELETE FROM");
    p = trim(p);

    char tname[MAX_NAME_LEN];
    int i = 0;
    while (*p && !isspace((unsigned char)*p) && *p != ';' && i < MAX_NAME_LEN - 1) {
        tname[i++] = *p++;
    }
    tname[i] = '\0';
    p = trim(p);

    if (tname[0] == '\0') {
        printf("Syntax error: missing table name in DELETE.\n");
        return;
    }

    char* where_kw = strstr(p, "WHERE");
    if (!where_kw) {
        printf("Syntax error: DELETE without WHERE not supported.\n");
        return;
    }

    char* cond = trim(where_kw + strlen("WHERE"));
    char col[64];
    char val[256];

    if (!parse_condition_eq(cond, col, sizeof(col), val, sizeof(val))) {
        printf("Syntax error in WHERE clause.\n");
        return;
    }

    delete_where_eq(db, tname, col, val);
}

static void parse_update(Database* db, char* line) {
    char* p = line + strlen("UPDATE");
    p = trim(p);

    char tname[MAX_NAME_LEN];
    int i = 0;
    while (*p && !isspace((unsigned char)*p) && *p != ';' && i < MAX_NAME_LEN - 1) {
        tname[i++] = *p++;
    }
    tname[i] = '\0';
    p = trim(p);

    if (tname[0] == '\0') {
        printf("Syntax error: missing table name in UPDATE.\n");
        return;
    }

    char* set_kw = strstr(p, "SET");
    char* where_kw = strstr(p, "WHERE");
    if (!set_kw || !where_kw) {
        printf("Syntax error: invalid UPDATE.\n");
        return;
    }

    char set_part[256] = {0};
    size_t len = where_kw - (set_kw + strlen("SET"));
    strncpy(set_part, set_kw + strlen("SET"), len);
    set_part[len] = '\0';

    char set_col[64];
    char set_val[256];
    if (!parse_condition_eq(set_part, set_col, sizeof(set_col),
                            set_val, sizeof(set_val))) {
        printf("Syntax error in SET clause.\n");
        return;
    }

    char* where_cond = trim(where_kw + strlen("WHERE"));
    char where_col[64];
    char where_val[256];
    if (!parse_condition_eq(where_cond, where_col, sizeof(where_col),
                            where_val, sizeof(where_val))) {
        printf("Syntax error in WHERE clause.\n");
        return;
    }

    update_where_eq(db, tname, set_col, set_val, where_col, where_val);
}

/* ============================================================
   HANDLER WRAPPERS (bridge dispatcher → parser)
   ============================================================ */

static void handle_create(Database* db, char* input)  {
    db_log("[CREATE] %s", input);
    parse_create_table(db, input); 
}
static void handle_insert(Database* db, char* input)  { 
    db_log("[INSERT] %s", input);
    parse_insert(db, input); 
}
static void handle_select(Database* db, char* input)  { 
    db_log("[SELECT] %s", input);
    parse_select(db, input); 
}
static void handle_update(Database* db, char* input)  { 
    parse_update(db, input); 
}
static void handle_delete(Database* db, char* input)  { 
    parse_delete(db, input); 
}

static void handle_drop(Database* db, char* input) {
    char tname[MAX_NAME_LEN];
    if (sscanf(input, "DROP TABLE %63s", tname) == 1) {
        size_t len = strlen(tname);
        if (len > 0 && tname[len - 1] == ';') tname[len - 1] = '\0';
        drop_table(db, tname);
    } else {
        printf("Syntax error in DROP TABLE.\n");
    }
}
