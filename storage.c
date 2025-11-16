#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "miniqlite.h"

/* ============================================================
   VARIADIC LOGGER — db_log()
   ============================================================ */

void db_log(const char* fmt, ...) {
    FILE* f = fopen("miniqlite.log", "a");
    if (!f) {
        fprintf(stderr, "[ERROR] Could not open log file.\n");
        return;
    }

    va_list args;
    va_start(args, fmt);

    vfprintf(f, fmt, args);
    fprintf(f, "\n");

    va_end(args);
    fclose(f);
}

int save_database(Database* db, const char* filename) {
    FILE* f = fopen(filename, db -> binary_mode? "wb":"w");
    if (!f) {
        perror("fopen");
        return 0;
    }

    fprintf(f, "MINIQLITE 1\n");
    fprintf(f, "TABLE_COUNT %d\n", db->num_tables);

    for (int i = 0; i < db->num_tables; i++) {
        Table* t = &db->tables[i];
        if (db->binary_mode){
            // Write a binary table blob. Do not emit text TABLE/COLUMN/ROW lines.
            if (!save_table_binary(t, f)) {
                fclose(f);
                return 0;
            }
        }
        else {
            fprintf(f, "TABLE %s %d %d\n", t->name, t->num_columns, t->num_rows);

            for (int c = 0; c < t->num_columns; c++) {
                fprintf(f, "COLUMN %s %s\n",
                        t->columns[c].name,
                        column_type_to_string(t->columns[c].type));
            }

            for (int r = 0; r < t->num_rows; r++) {
                fprintf(f, "ROW");
                for (int c = 0; c < t->num_columns; c++) {
                    fprintf(f, "\t%s", t->rows[r].values[c] ? t->rows[r].values[c] : "");
                }
                fprintf(f, "\n");
            }
        }
    }

    fclose(f);
    return 1;
}

int save_table_binary(Table* t, FILE* f) {
    if (!f || !t) return 0;

    // Write fixed-size table name block
    char namebuf[MAX_NAME_LEN] = {0};
    strncpy(namebuf, t->name ? t->name : "", MAX_NAME_LEN-1);
    if (fwrite(namebuf, 1, MAX_NAME_LEN, f) != MAX_NAME_LEN) return 0;

    // number of columns
    if (fwrite(&t->num_columns, sizeof(int), 1, f) != 1) return 0;

    // columns: name (fixed) + type (int)
    for (int i = 0; i < t->num_columns; i++) {
        char colname[MAX_NAME_LEN] = {0};
        strncpy(colname, t->columns[i].name ? t->columns[i].name : "", MAX_NAME_LEN-1);
        if (fwrite(colname, 1, MAX_NAME_LEN, f) != MAX_NAME_LEN) return 0;
        if (fwrite(&t->columns[i].type, sizeof(int), 1, f) != 1) return 0;
    }

    // number of rows
    if (fwrite(&t->num_rows, sizeof(int), 1, f) != 1) return 0;

    // rows: for each cell write length (int) then raw bytes
    for (int r = 0; r < t->num_rows; r++) {
        for (int c = 0; c < t->num_columns; c++) {
            const char* val = (t->rows[r].values[c] ? t->rows[r].values[c] : "");
            int len = (int)strlen(val);
            if (fwrite(&len, sizeof(int), 1, f) != 1) return 0;
            if (len > 0) {
                if (fwrite(val, 1, len, f) != (size_t)len) return 0;
            }
        }
    }

    return 1;
}
Table* load_table_binary(FILE* f) {
    if (!f) return NULL;

    // read table name
    char tname[MAX_NAME_LEN] = {0};
    if (fread(tname, 1, MAX_NAME_LEN, f) != MAX_NAME_LEN) return NULL;

    int num_cols = 0;
    if (fread(&num_cols, sizeof(int), 1, f) != 1) return NULL;

    ColumnDef* cols = malloc(sizeof(ColumnDef) * num_cols);
    if (!cols) return NULL;

    for (int i = 0; i < num_cols; i++) {
        if (fread(cols[i].name, 1, MAX_NAME_LEN, f) != MAX_NAME_LEN) {
            free(cols);
            return NULL;
        }
        if (fread(&cols[i].type, sizeof(int), 1, f) != 1) {
            free(cols);
            return NULL;
        }
        cols[i].name[MAX_NAME_LEN-1] = '\0';
    }

    int num_rows = 0;
    if (fread(&num_rows, sizeof(int), 1, f) != 1) {
        free(cols);
        return NULL;
    }

    // create table structure
    Table* t = create_table_struct(cols, num_cols);
    if (!t) {
        free(cols);
        return NULL;
    }

    // ensure name copied
    strncpy(t->name, tname, MAX_NAME_LEN-1);
    t->name[MAX_NAME_LEN-1] = '\0';

    // Read rows: for each cell read len + bytes, then assign via str_duplicate
    for (int r = 0; r < num_rows; r++) {
        // prepare temporary vals array
        char** vals = malloc(sizeof(char*) * num_cols);
        if (!vals) {
            free(cols);
            // Note: not freeing table fully here; caller should manage
            return t;
        }
        for (int c = 0; c < num_cols; c++) {
            int len = 0;
            if (fread(&len, sizeof(int), 1, f) != 1) {
                // cleanup
                for (int k = 0; k < c; k++) free(vals[k]);
                free(vals);
                free(cols);
                return t;
            }
            char* buf = malloc(len + 1);
            if (!buf) {
                for (int k = 0; k < c; k++) free(vals[k]);
                free(vals);
                free(cols);
                return t;
            }
            if (len > 0) {
                if (fread(buf, 1, len, f) != (size_t)len) {
                    free(buf);
                    for (int k = 0; k < c; k++) free(vals[k]);
                    free(vals);
                    free(cols);
                    return t;
                }
            }
            buf[len] = '\0';
            vals[c] = str_duplicate(buf);
            free(buf);
        }

        // Try to insert into table's row storage. Prefer using existing helper if available.
        // If insert_row(db, tname, vals, num_cols) is not usable here (no db), attempt to
        // place values directly if t->rows is allocated.
        if (t->rows && r < t->num_rows) {
            for (int c = 0; c < num_cols; c++) {
                // free any existing value to avoid leak (best-effort)
                if (t->rows[r].values[c]) free(t->rows[r].values[c]);
                t->rows[r].values[c] = vals[c];
            }
        } else {
            // best-effort: append by using create/insert helper if available (not used here).
            // If direct placement was not possible, free the temporary values to avoid leak.
            for (int c = 0; c < num_cols; c++) free(vals[c]);
        }
        free(vals);
    }

    free(cols);
    t->num_rows = num_rows;
    return t;
}

int load_database(Database* db, const char* filename) {
    FILE* f = fopen(filename, "r");
    if (!f) {
        // Not an error if file doesn't exist yet.
        return 0;
    }

    free_database(db);
    init_database(db);

    char line[1024];

    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return 0;
    }
    if (strncmp(line, "MINIQLITE", 9) != 0) {
        fclose(f);
        return 0;
    }

    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return 0;
    }

    int table_count = 0;
    if (sscanf(line, "TABLE_COUNT %d", &table_count) != 1) {
        fclose(f);
        return 0;
    }

    for (int ti = 0; ti < table_count; ti++) {
        if (!fgets(line, sizeof(line), f)) break;

        char tname[MAX_NAME_LEN];
        int num_cols = 0;
        int num_rows = 0;

        if (sscanf(line, "TABLE %63s %d %d", tname, &num_cols, &num_rows) != 3) {
            break;
        }

        ColumnDef* cols = malloc(sizeof(ColumnDef) * num_cols);
        if (!cols) {
            fclose(f);
            return 0;
        }

        for (int c = 0; c < num_cols; c++) {
            if (!fgets(line, sizeof(line), f)) {
                free(cols);
                fclose(f);
                return 0;
            }
            char colname[MAX_NAME_LEN];
            char typestr[32];
            if (sscanf(line, "COLUMN %63s %31s", colname, typestr) != 2) {
                free(cols);
                fclose(f);
                return 0;
            }
            strncpy(cols[c].name, colname, MAX_NAME_LEN - 1);
            cols[c].name[MAX_NAME_LEN - 1] = '\0';
            cols[c].type = parse_column_type(typestr);
        }

        create_table(db, tname, cols, num_cols);
        Table* t = find_table(db, tname);
        if (!t) {
            free(cols);
            fclose(f);
            return 0;
        }

        for (int r = 0; r < num_rows; r++) {
            if (!fgets(line, sizeof(line), f)) {
                free(cols);
                fclose(f);
                return 0;
            }
            if (strncmp(line, "ROW", 3) != 0) {
                free(cols);
                fclose(f);
                return 0;
            }

            char** vals = malloc(sizeof(char*) * num_cols);
            if (!vals) {
                free(cols);
                fclose(f);
                return 0;
            }

            int vc = 0;
            char* p = strchr(line, '\t');
            if (p) {
                p++;
                while (vc < num_cols) {
                    char* end = strchr(p, '\t');
                    if (!end) {
                        char* nl = strchr(p, '\n');
                        if (nl) *nl = '\0';
                        vals[vc++] = str_duplicate(p);
                        break;
                    } else {
                        *end = '\0';
                        vals[vc++] = str_duplicate(p);
                        p = end + 1;
                    }
                }
            }

            while (vc < num_cols) {
                vals[vc++] = str_duplicate("");
            }

            insert_row(db, tname, vals, num_cols);

            for (int i = 0; i < num_cols; i++) free(vals[i]);
            free(vals);
        }

        free(cols);
    }

    fclose(f);
    return 1;
}
