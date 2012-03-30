/*
Copyright (c) 2012, Jason Carey  https://github.com/hanumantmk/DenseDB
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
  The names of its contributors may not be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
