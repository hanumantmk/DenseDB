#include <stdio.h>
#include <stdlib.h>
#include "dense_db.h"

void pp(dense_db_table_t * table)
{
  dense_db_field_t * fields = table->metadata->fields;
  size_t n_fields = table->metadata->n_fields;
  size_t rows = table->rows;

  dense_db_accessor_t accs[n_fields];

  int i;
  for (i = 0; i < n_fields; i++) {
    printf("%s%c", fields[i].name, i == n_fields - 1 ? '\n' : '\t');
    accs[i] = dense_db_table_get_accessor(table, fields[i].name);
  }

  for (i = 0; i < rows; i++) {
    char foo[20] = { 0 };
    int bar;
    int baz;

    dense_db_table_get(table, i, accs[1], foo);
    bar = dense_db_table_get_int(table, i, accs[0]);
    baz = dense_db_table_get_int(table, i, accs[2]);

    printf("%d\t%s\t%d\n", bar, foo, baz);
  }
}


int main (int argc, char ** argv)
{
  if (argc != 2) {
    printf("Usage - %s AMOUNT\n", argv[0]);

    return 1;
  }

  int amount = atoi(argv[1]);

  dense_db_t * db = dense_db_new(".");

  dense_db_field_t fields[] = {
    { "bar", 4 },
    { "foo", 8 * 20 },
    { "baz", 4 },
  };

  dense_db_table_t * table = dense_db_create_table(db, "foo", fields, 3, amount);

  dense_db_accessor_t accs[3];

  int i;
  for (i = 0; i < 3; i++) {
    accs[i] = dense_db_table_get_accessor(table, fields[i].name);
  }

  for (i = 0; i < amount; i++) {
    //char foo[9] = { (i % 26) + 65, (i % 26) + 97 };
    char foo[] = "12345678901234567";

    uint64_t bar = i % 4;
    uint64_t baz = i % 8;
    dense_db_table_set(table, i, accs[1], foo);
    dense_db_table_set_int(table, i, accs[0], bar);
    dense_db_table_set_int(table, i, accs[2], baz);
  }

  dense_table_sync(table);

  pp(table);

  dense_db_table_destroy(table);

  dense_db_destroy(db);

  return 0;
}
