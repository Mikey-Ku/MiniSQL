#include <stdio.h>
#include <string.h>
#include "miniqlite.h"

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    Database db;
    init_database(&db);

    const char* default_file = "miniqlite.db";
    load_database(&db, default_file);

    char input[1024];

    while (1) {
        printf("miniqlite> ");
        if (!fgets(input, sizeof(input), stdin)) {
            printf("\n");
            break;
        }

        size_t len = strlen(input);
        if (len > 0 && input[len - 1] == '\n') {
            input[len - 1] = '\0';
        }

        int exit_flag = execute_command(&db, input);
        if (exit_flag) {
            break;
        }
    }

    save_database(&db, default_file);
    free_database(&db);

    return 0;
}
