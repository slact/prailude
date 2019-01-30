#include "cbdb.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main(void) {
  cbdb_config_t cf = {
    .row_len = 50,
    .id_len = 10,
    .index_count = 0
  };
  cbdb_error_t err;
  cbdb_t *db = cbdb_open("./" ,"foo", &cf, NULL, &err);
  if(db) {
    printf("opened ok\n");
    cbdb_close(db);
    return 1;
  }
  else {
    cbdb_error_print(&err);
    return 0;
  }
}
