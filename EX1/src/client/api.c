#include "api.h"

#include "../common/constants.h"
#include "../common/protocol.h"
#include "../common/io.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path/*,
                int *notif_pipe*/) {
  // create pipes and connect
  char message[121];
  message[0]='1';
  pad_string(message + 1, req_pipe_path);
  pad_string(message + 1 + 40, resp_pipe_path);
  pad_string(message + 1 + 2 * 40, notif_pipe_path);
  
  int server_fd=open(server_pipe_path, O_WRONLY);
  
  //printf("%s\n",message);
  ssize_t bytes_written = write(server_fd, message, 121);
  if (bytes_written != 121) {
    perror("write");
    return 1;
  }
  
  printf("success\n");

  return 0;
}

int kvs_disconnect(int req_pipe,int resp_pipe) {
  // close pipes and unlink pipe files
  char message[1]="2";
  write(req_pipe,message,1);

  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      printf("%c\n",buf[0]);
      if(buf[0]=='2'){
        printf("Disconecting...1\n");
        if(buf[1]=='0'){
          printf("Disconecting...2\n");
        }
      }
      break;
    }
  }
  return 0;
}

int kvs_subscribe(const char *key,int req_pipe,int resp_pipe) {
  // send subscribe message to request pipe and wait for response in response
  char message[42];
  message[0]='3';
  size_t len = strlen(key);
  if (len >= 40) {
    memcpy(message+1, key, 41); // Copy up to 40 characters
  } else {
    // Copy the string and pad the rest with '\0'
    memcpy(message+1, key, len);
    memset(message+1 + len, '\0', 41 - len);
  }
  ssize_t bytes_written = write(req_pipe, message, 42);
  if (bytes_written != 42) {
    perror("write");
    return 1;
  }
  
  printf("success\n");
  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      printf("%c\n",buf[0]);
      if(buf[0]=='3'){
        printf("subscribe done\n");
        if(buf[1]=='0')
          printf("Key unexistent\n");
      }
      break;
    }
  }
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key,int req_pipe,int resp_pipe) {
  // send unsubscribe message to request pipe and wait for response in response
  char message[42];
  message[0]='4';
  size_t len = strlen(key);
  if (len >= 40) {
    memcpy(message+1, key, 41); // Copy up to 40 characters
  } else {
    // Copy the string and pad the rest with '\0'
    memcpy(message+1, key, len);
    memset(message+1 + len, '\0', 41 - len);
  }
  ssize_t bytes_written = write(req_pipe, message, 42);
  if (bytes_written != 42) {
    perror("write");
    return 1;
  }
  
  printf("success\n");
  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      printf("%c\n",buf[0]);
      if(buf[0]=='4'){
        printf("unsubscribe done\n");
        if(buf[1]=='0')
          printf("Key unexistent\n");
      }
      break;
    }
  }
  // pipe
  return 0;
}
