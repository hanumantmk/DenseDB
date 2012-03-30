/*
Copyright (c) 2012, Jason Carey  https://github.com/hanumantmk/DenseDB
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <stdio.h>
#include <stdlib.h>
#include "dense_db.h"

void pp_stats(dense_db_table_t * table)
{
  dense_db_field_t * fields = table->fields;
  size_t n_fields = table->n_fields;

  printf(
    "Table Name: %s\n"
    "      Rows: %d\n"
    "  Row Size: %d\n"
    "    Fields:\n"
    , table->name, table->rows, table->row_size);

  int i;
  for (i = 0; i < n_fields; i++) {
    printf("  %s:\t%d\n", fields[i].name, fields[i].size);
  }
}

void pp(dense_db_table_t * table)
{
  dense_db_field_t * fields = table->fields;
  size_t n_fields = table->n_fields;
  size_t rows = table->rows;

  dense_db_accessor_t accs[n_fields];

  int i;
  for (i = 0; i < n_fields; i++) {
    printf("%s%c", fields[i].name, i == n_fields - 1 ? '\n' : '\t');
    accs[i] = dense_db_table_get_accessor(table, fields[i].name);
  }

  for (i = 0; i < rows; i++) {
    char foo[20] = { 0 };
    int bar, baz, bip, bop, bip2;

    dense_db_table_get(table, i, accs[1], foo);
    bar = dense_db_table_get_int(table, i, accs[0]);
    baz = dense_db_table_get_int(table, i, accs[2]);
    bop = dense_db_table_get_int(table, i, accs[3]);
    bip = dense_db_table_get_int(table, i, accs[4]);
    bip2= dense_db_table_get_int(table, i, accs[5]);

    printf("%d\t%s\t%d\t%d\t%d\t%d\n", bar, foo, baz, bop, bip, bip2);
  }
}


int main (int argc, char ** argv)
{
  if (argc != 2) {
    printf("Usage - %s AMOUNT\n", argv[0]);

    return 1;
  }

  int amount = atoi(argv[1]);

  dense_db_t * db = dense_db_new(".", 1);

  char foo[] = "There's no place like home";

  dense_db_field_t fields[] = {
    { "bar", 4 },
    { "foo", 8 * sizeof(foo)},
    { "baz", 4 },
    { "bop", 3 },
    { "bip", 2 },
    { "bip2", 2 },
  };

  dense_db_table_t * table = dense_db_table_create(db, "foo", fields, 6, amount);

  dense_db_table_close(table);

  table = dense_db_table_open(db, "foo");

  dense_db_accessor_t accs[6];

  int i;
  for (i = 0; i < 6; i++) {
    accs[i] = dense_db_table_get_accessor(table, fields[i].name);
  }

  for (i = 0; i < amount; i++) {
    uint64_t bar = i % 16;
    uint64_t baz = i % 12;
    uint64_t bop = i % 4;
    uint64_t bip = i % 2;
    uint64_t bip2 = i % 2;

    dense_db_table_set(table, i, accs[1], foo);
    dense_db_table_set_int(table, i, accs[0], bar);
    dense_db_table_set_int(table, i, accs[2], baz);
    dense_db_table_set_int(table, i, accs[3], bop);
    dense_db_table_set_int(table, i, accs[4], bip);
    dense_db_table_set_int(table, i, accs[5], bip2);
  }

  dense_table_sync(table);

  pp_stats(table);

  pp(table);

  dense_db_table_close(table);

  table = dense_db_table_create(db, "foo2", fields, 6, amount);

  dense_db_table_close(table);

  dense_db_destroy(db);

  return 0;
}
