#include <stdint.h>
#include <string.h>
#include "uthash.h"

struct dense_db_table;

typedef struct dense_db {
  char * storage_path;

  int max_fds;

  struct dense_db_table * lookup;
} dense_db_t;

typedef struct dense_db_field {
  char * name;

  size_t size;
} dense_db_field_t;

typedef struct dense_db_accessor {
  int offset;
  int size;
} dense_db_accessor_t;

typedef struct dense_db_table {
  dense_db_t * db;

  char * name;

  int fd;
  char * data;

  size_t size;

  size_t rows;

  dense_db_field_t * fields;
  size_t n_fields;

  size_t header_size;
  size_t row_size;

  int refcount;

  UT_hash_handle hh;
} dense_db_table_t;

dense_db_t * dense_db_new(char * storage_path, int max_fds);
void dense_table_sync(dense_db_table_t * table);

dense_db_accessor_t dense_db_table_get_accessor(dense_db_table_t * table, char * field);
void dense_db_table_get(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * out);
uint64_t dense_db_table_get_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc);

void dense_db_table_set(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, void * in);
void dense_db_table_set_int(dense_db_table_t * table, uint64_t row, dense_db_accessor_t acc, uint64_t in);

dense_db_table_t * dense_db_table_create(dense_db_t * db, char * name, dense_db_field_t * fields, size_t n_fields, size_t rows);
dense_db_table_t * dense_db_table_open(dense_db_t * db, char * name);
void dense_db_table_close(dense_db_table_t * table);

void dense_db_destroy(dense_db_t * db);
