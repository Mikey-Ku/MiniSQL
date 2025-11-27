#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "miniqlite.h"

static void free_table(Table* t);
static int column_index(Table* t, const char* name);

/* ===== Utility ===== */

char* str_duplicate(const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char* out = (char*)malloc(len + 1);
    if (!out) {
        fprintf(stderr, "Out of memory in str_duplicate\n");
        exit(1);
    }
    memcpy(out, s, len + 1);
    return out;
}

ColumnType parse_column_type(const char* s) {
    if (!s) return COL_TEXT;
    if (strcasecmp(s, "INT") == 0 || strcasecmp(s, "INTEGER") == 0) return COL_INT;
    if (strcasecmp(s, "TEXT") == 0) return COL_TEXT;
    if (strcasecmp(s, "FLOAT") == 0 || strcasecmp(s, "REAL") == 0) return COL_FLOAT;
    return COL_TEXT; // default
}

const char* column_type_to_string(ColumnType t) {
    switch (t) {
        case COL_INT:   return "INT";
        case COL_TEXT:  return "TEXT";
        case COL_FLOAT: return "FLOAT";
        default:        return "TEXT";
    }
}

/* ===== Database lifecycle ===== */

void init_database(Database* db) {
    db->num_tables = 0;
    db->tables = NULL;
}

void free_database(Database* db) {
    if (!db) return;
    for (int i = 0; i < db->num_tables; i++) {
        free_table(&db->tables[i]);
    }
    free(db->tables);
    db->tables = NULL;
    db->num_tables = 0;
}

static void free_table(Table* t) {
    if (!t) return;
    free(t->columns);
    for (int r = 0; r < t->num_rows; r++) {
        if (t->rows[r].values) {
            for (int c = 0; c < t->num_columns; c++) {
                free(t->rows[r].values[c]);
            }
            free(t->rows[r].values);
        }
    }
    free(t->rows);
    t->rows = NULL;
    t->columns = NULL;
    t->num_columns = 0;
    t->num_rows = 0;
}

/* ===== Helpers ===== */

Table* find_table(Database* db, const char* name) {
    for (int i = 0; i < db->num_tables; i++) {
        if (strcmp(db->tables[i].name, name) == 0) {
            return &db->tables[i];
        }
    }
    return NULL;
}

static int column_index(Table* t, const char* name) {
    for (int i = 0; i < t->num_columns; i++) {
        if (strcmp(t->columns[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

/* ===== Table operations ===== */
int create_table(Database* db, const char* name, ColumnDef* cols, int num_cols) {
    if (find_table(db, name)) {
        printf("Error: table '%s' already exists.\n", name);
        return 0;
    }

    Table* new_tables = realloc(db->tables, sizeof(Table) * (db->num_tables + 1));
    if (!new_tables) {
        fprintf(stderr, "Out of memory creating table\n");
        return 0;
    }
    db->tables = new_tables;

    Table* t = &db->tables[db->num_tables];
    memset(t, 0, sizeof(Table));

    // --- Table metadata ---
    strncpy(t->name, name, MAX_NAME_LEN - 1);
    t->num_columns = num_cols;

    // --- Copy column definitions ---
    t->columns = malloc(sizeof(ColumnDef) * num_cols);
    if (!t->columns) {
        fprintf(stderr, "Out of memory for columns\n");
        return 0;
    }
    for (int i = 0; i < num_cols; i++) {
        t->columns[i] = cols[i];
    }

    // --- Initialize row-major fields ---
    t->num_rows = 0;
    t->rows = NULL;

    // --- Initialize column-major fields ---
    t->column_data = calloc(num_cols, sizeof(ColumnStorage));
    if (!t->column_data) {
        fprintf(stderr, "Out of memory for column storage\n");
        free(t->columns);
        return 0;
    }

    for (int i = 0; i < num_cols; i++) {
        strncpy(t->column_data[i].name, cols[i].name, MAX_NAME_LEN - 1);
        t->column_data[i].type = cols[i].type;
        t->column_data[i].values = NULL;  // will grow as rows are inserted
    }

    db->num_tables++;
    printf("Table '%s' created with %d columns.\n", name, num_cols);
    return 1;
}


int drop_table(Database* db, const char* name) {
    for (int i = 0; i < db->num_tables; i++) {
        if (strcmp(db->tables[i].name, name) == 0) {
            free_table(&db->tables[i]);
            for (int j = i + 1; j < db->num_tables; j++) {
                db->tables[j - 1] = db->tables[j];
            }
            db->num_tables--;

            if (db->num_tables == 0) {
                free(db->tables);
                db->tables = NULL;
            } else {
                Table* tmp = realloc(db->tables, sizeof(Table) * db->num_tables);
                if (tmp) db->tables = tmp;
            }

            printf("Table '%s' dropped.\n", name);
            return 1;
        }
    }
    printf("Error: table '%s' not found.\n", name);
    return 0;
}

int insert_row(Database* db, const char* table_name, char** values, int num_values) {
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }
    if (num_values != t->num_columns) {
        printf("Error: expected %d values, got %d.\n", t->num_columns, num_values);
        return 0;
    }

    /* ======================================================
       ROW-MAJOR MODE (original behavior)
       ====================================================== */
    if (db->column_store == 0) {
        Row* new_rows = realloc(t->rows, sizeof(Row) * (t->num_rows + 1));
        if (!new_rows) {
            fprintf(stderr, "Out of memory inserting row\n");
            return 0;
        }
        t->rows = new_rows;

        Row* r = &t->rows[t->num_rows];
        r->values = malloc(sizeof(char*) * t->num_columns);
        if (!r->values) {
            fprintf(stderr, "Out of memory inserting row values\n");
            return 0;
        }

        for (int i = 0; i < t->num_columns; i++) {
            r->values[i] = str_duplicate(values[i]);
        }

        t->num_rows++;
        printf("1 row inserted into '%s' (row-major mode).\n", table_name);
        return 1;
    }

    /* ======================================================
       COLUMN-MAJOR MODE (new cache-friendly layout)
       ====================================================== */
    else {
        if (!t->column_data) {
            fprintf(stderr, "Error: column storage not initialized for table '%s'.\n", t->name);
            return 0;
        }

        for (int i = 0; i < t->num_columns; i++) {
            ColumnStorage* col = &t->column_data[i];

            // Grow the column array by one entry
            char** new_vals = realloc(col->values, sizeof(char*) * (t->num_rows + 1));
            if (!new_vals) {
                fprintf(stderr, "Out of memory inserting into column '%s'.\n", col->name);
                return 0;
            }
            col->values = new_vals;

            // Copy the new value into the column
            col->values[t->num_rows] = str_duplicate(values[i]);
        }

        t->num_rows++;
        printf("1 row inserted into '%s' (column-major mode).\n", table_name);
        return 1;
    }
}


/* ===== SELECT ===== */

static void print_header(Table* t, int* cols, int num_cols) {
    for (int i = 0; i < num_cols; i++) {
        int idx = cols[i];
        printf("%s", t->columns[idx].name);
        if (i < num_cols - 1) printf(" | ");
    }
    printf("\n");
}

static void print_row(Table* t, Row* r, int* cols, int num_cols) {
    (void)t;
    for (int i = 0; i < num_cols; i++) {
        int idx = cols[i];
        printf("%s", r->values[idx] ? r->values[idx] : "NULL");
        if (i < num_cols - 1) printf(" | ");
    }
    printf("\n");
}

int select_all(Database* db, const char* table_name) {
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }
    if (db->column_store == 1) {
    // --- COLUMN-MAJOR FAST PATH ---
    Table* t = find_table(db, table_name);
    if (!t) return;

    for (int r = 0; r < t->num_rows; r++) {
        for (int c = 0; c < t->num_columns; c++) {
            printf("%s\t", t->column_data[c].values[r]);
        }
        printf("\n");
    }
    return;
    }
    int* cols = malloc(sizeof(int) * t->num_columns);
    if (!cols) return 0;
    for (int i = 0; i < t->num_columns; i++) cols[i] = i;

    print_header(t, cols, t->num_columns);
    for (int r = 0; r < t->num_rows; r++) {
        print_row(t, &t->rows[r], cols, t->num_columns);
    }

    free(cols);
    return 1;
}

int select_columns(Database* db, const char* table_name, char** cols, int num_cols) {
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }
    if (db->column_store == 1) {
    Table* t = find_table(db, table_name);
    if (!t) return;

    // 1. Print header
    for (int i = 0; i < num_cols; i++) {
        printf("%s\t", cols[i]);
    }
    printf("\n");

    // 2. Locate each requested column
    int col_idx[num_cols];
    for (int i = 0; i < num_cols; i++) {
        col_idx[i] = find_column_index(t, cols[i]);  // you must already have this helper
        if (col_idx[i] < 0) {
            printf("Unknown column: %s\n", cols[i]);
            return;
        }
    }

    // 3. Print rows
    for (int r = 0; r < t->num_rows; r++) {
        for (int i = 0; i < num_cols; i++) {
            printf("%s\t", t->column_data[col_idx[i]].values[r]);
        }
        printf("\n");
    }

    return;
    }
    int* idxs = malloc(sizeof(int) * num_cols);
    if (!idxs) return 0;

    for (int i = 0; i < num_cols; i++) {
        int idx = column_index(t, cols[i]);
        if (idx < 0) {
            printf("Error: unknown column '%s'.\n", cols[i]);
            free(idxs);
            return 0;
        }
        idxs[i] = idx;
    }

    print_header(t, idxs, num_cols);
    for (int r = 0; r < t->num_rows; r++) {
        print_row(t, &t->rows[r], idxs, num_cols);
    }

    free(idxs);
    return 1;
}

int select_where_eq(Database* db, const char* table_name,
                    char** cols, int num_cols,
                    const char* where_col, const char* where_val) {
    if (db->column_store == 1) {
    Table* t = find_table(db, table_name);
    if (!t) return;

    int where_col = find_column_index(t, where_col);
    if (where_col < 0) {
        printf("Unknown WHERE column: %s\n", where_col);
        return;
    }

    // Map selected columns
    int col_idx[num_cols];
    for (int i = 0; i < num_cols; i++) {
        col_idx[i] = find_column_index(t, cols[i]);
        if (col_idx[i] < 0) {
            printf("Unknown column: %s\n", cols[i]);
            return;
        }
    }

    // Print matching rows
    for (int r = 0; r < t->num_rows; r++) {
        if (strcmp(t->column_data[where_col].values[r], where_val) == 0) {
            for (int i = 0; i < num_cols; i++) {
                printf("%s\t", t->column_data[col_idx[i]].values[r]);
            }
            printf("\n");
        }
    }

    return;
}  
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }
    int where_idx = column_index(t, where_col);
    if (where_idx < 0) {
        printf("Error: unknown column '%s' in WHERE.\n", where_col);
        return 0;
    }

    int* idxs = malloc(sizeof(int) * num_cols);
    if (!idxs) return 0;
    for (int i = 0; i < num_cols; i++) {
        int idx = column_index(t, cols[i]);
        if (idx < 0) {
            printf("Error: unknown column '%s'.\n", cols[i]);
            free(idxs);
            return 0;
        }
        idxs[i] = idx;
    }

    print_header(t, idxs, num_cols);
    for (int r = 0; r < t->num_rows; r++) {
        char* v = t->rows[r].values[where_idx];
        if (v && strcmp(v, where_val) == 0) {
            print_row(t, &t->rows[r], idxs, num_cols);
        }
    }

    free(idxs);
    return 1;
}

/* ===== DELETE / UPDATE ===== */

int delete_where_eq(Database* db, const char* table_name,
                    const char* where_col, const char* where_val) {
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }

    int where_idx = column_index(t, where_col);
    if (where_idx < 0) {
        printf("Error: unknown column '%s' in WHERE.\n", where_col);
        return 0;
    }

    int removed = 0;
    for (int r = 0; r < t->num_rows; ) {
        char* v = t->rows[r].values[where_idx];
        if (v && strcmp(v, where_val) == 0) {
            for (int c = 0; c < t->num_columns; c++) {
                free(t->rows[r].values[c]);
            }
            free(t->rows[r].values);

            for (int j = r + 1; j < t->num_rows; j++) {
                t->rows[j - 1] = t->rows[j];
            }
            t->num_rows--;
            removed++;
        } else {
            r++;
        }
    }

    if (removed > 0) {
        Row* tmp = realloc(t->rows, sizeof(Row) * t->num_rows);
        if (tmp || t->num_rows == 0) t->rows = tmp;
    }

    printf("%d row(s) deleted from '%s'.\n", removed, table_name);
    return 1;
}

int update_where_eq(Database* db, const char* table_name,
                    const char* set_col, const char* set_val,
                    const char* where_col, const char* where_val) {
    Table* t = find_table(db, table_name);
    if (!t) {
        printf("Error: table '%s' not found.\n", table_name);
        return 0;
    }

    int where_idx = column_index(t, where_col);
    int set_idx   = column_index(t, set_col);
    if (where_idx < 0 || set_idx < 0) {
        printf("Error: unknown column in UPDATE.\n");
        return 0;
    }

    int updated = 0;
    for (int r = 0; r < t->num_rows; r++) {
        char* v = t->rows[r].values[where_idx];
        if (v && strcmp(v, where_val) == 0) {
            free(t->rows[r].values[set_idx]);
            t->rows[r].values[set_idx] = str_duplicate(set_val);
            updated++;
        }
    }

    printf("%d row(s) updated in '%s'.\n", updated, table_name);
    return 1;
}

/* ===== Misc ===== */

void list_tables(Database* db) {
    printf("Tables:\n");
    for (int i = 0; i < db->num_tables; i++) {
        printf("  %s (%d columns, %d rows)\n",
               db->tables[i].name,
               db->tables[i].num_columns,
               db->tables[i].num_rows);
    }
}
