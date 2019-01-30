#include "rydb.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main(void) {
  rydb_config_t cf = {
    .row_len = 50,
    .id_len = 10,
    .index_count = 0
  };
  rydb_error_t err;
  rydb_t *db = rydb_open("./" ,"foo", &cf, NULL, &err);
  if(db) {
    printf("opened ok\n");
    rydb_close(db);
    return 1;
  }
  else {
    rydb_error_print(&err);
    return 0;
  }
}
