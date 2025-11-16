#ifndef MINIQLITE_H
#define MINIQLITE_H

#include <stdio.h>
#include <stddef.h>

#define MAX_NAME_LEN 64
#define MAX_VALUE_LEN 256 

//enum of all column types
typedef enum {
    COL_INT,
    COL_TEXT,
    COL_FLOAT
} ColumnType;

typedef struct {
    char name[MAX_NAME_LEN];
    ColumnType type;
    char** values;  // array of strings for this column (column-major)
} ColumnStorage;

//Defines a column type in a table
typedef struct {
    char name[MAX_NAME_LEN];
    ColumnType type;
} ColumnDef;

//Defines a row in a table
typedef struct {
    char** values; //Array of strings representing the values for each column
} Row;

//Defines a table in the database
typedef struct {
    char name[MAX_NAME_LEN];
    int num_columns;
    ColumnDef* columns;
    int num_rows;
    Row* rows;                // for row-major mode
    ColumnStorage* column_data;  // for column-major mode
} Table;

//Defines the database structure
typedef struct {
    int num_tables; //Number of tables
    Table* tables; //Pointer to array of tables [num_tables]
    int binary_mode; //Whether to save/load in binary mode 0 = text, 1 = binary
    int column_store; //0 = row-major, 1 = column-major
} Database;


//Funcitons for starting the database
void init_database(Database* db); //initalizes an empty database
void free_database(Database* db); //frees all memory associated with the database

//Funtions for manipulating tables and data
Table* find_table(Database* db, const char* name); //finds a table by name, returns NULL if not found
int create_table(Database* db, const char* name, ColumnDef* cols, int num_cols); //Creates a new table with given name and columns
int drop_table(Database* db, const char* name); //Deletes a table by name
int insert_row(Database* db, const char* table_name, char** values, int num_values); //Inserts a new row into a table
int select_all(Database* db, const char* table_name); //Selects and prints all rows from a table
int select_columns(Database* db, const char* table_name, char** cols, int num_cols); //Selects and prints specific columns from a table
int select_where_eq(Database* db, const char* table_name, char** cols, int num_cols, const char* where_col, const char* where_val); //Prints rows where a column equals a value
int delete_where_eq(Database* db, const char* table_name, const char* where_col, const char* where_val); //Deletes rows where a column equals a value
int update_where_eq(Database* db, const char* table_name, const char* set_col, const char* set_val, const char* where_col, const char* where_val); //Updates rows where a column equals a value
void list_tables(Database* db); //Lists all tables in the database
int load_database(Database* db, const char* filename);
int save_database(Database* db, const char* filename);
void db_log(const char* fmt, ...);


int execute_command(Database* db, char* input); /* Return 1 to request exit, 0 to continue. */


/* ===== Utility ===== */

ColumnType parse_column_type(const char* s);
const char* column_type_to_string(ColumnType t);
char* str_duplicate(const char* s);

#endif
