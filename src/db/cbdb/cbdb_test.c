#include "cbdb.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main(void) {
  cbdb_config_t cf = {
    .id_len = 10,
    .data_len = 50
  };
  cbdb_error_t err;
  cbdb_t *db = cbdb_create("./" ,"foo", &cf, &err);
  if(db) {
    printf("opened ok");
  }
  else {
    printf("ERROR: %d: %s, errno %d %s", err.code, err.str, err.errno_val, strerror(err.errno_val));
  }
  
  return 0; 
}
