#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

void ensure_40_characters(char *str);

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // create pipes
  int req_pipe,resp_pipe;
  mkfifo(notif_pipe_path, 0640);
  mkfifo(req_pipe_path, 0640);
  mkfifo(resp_pipe_path, 0640);
  int res=kvs_connect(req_pipe_path,resp_pipe_path,argv[2],notif_pipe_path);
  if(res==1){
    printf("Connection failed try again later\n");
    unlink(notif_pipe_path);
    unlink(resp_pipe_path);
    unlink(req_pipe_path);
    return 1;
  }

  
  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      req_pipe=open(req_pipe_path,O_RDWR);
      resp_pipe=open(resp_pipe_path, O_RDWR);
      if(req_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if(resp_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if (kvs_disconnect(req_pipe,resp_pipe) != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }
      
      unlink(notif_pipe_path);
      unlink(resp_pipe_path);
      unlink(req_pipe_path);
      close(req_pipe);
      close(resp_pipe);
      printf("Disconnected from server\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      req_pipe=open(req_pipe_path,O_RDWR);
      resp_pipe=open(resp_pipe_path, O_RDWR);
      if(req_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if(resp_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0],req_pipe,resp_pipe)!=0) {
        fprintf(stderr, "Command subscribe failed\n");
        close(req_pipe);
        close(resp_pipe);
        return 0;
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      req_pipe=open(req_pipe_path,O_RDWR);
      resp_pipe=open(resp_pipe_path, O_RDWR);
      if(req_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if(resp_pipe<0){
        printf("Server unresponsive... Disconnecting...\n");
        close(req_pipe);
        close(resp_pipe);
        return 1;
      }
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0],req_pipe,resp_pipe)!=0) {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      break;
    }
  }
}


