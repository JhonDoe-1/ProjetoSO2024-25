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
                char const *server_pipe_path, char const *notif_pipe_path) {
  // create pipes and connect
  char message[121];
  message[0]='1';
  pad_string(message + 1, req_pipe_path);
  pad_string(message + 1 + 40, resp_pipe_path);
  pad_string(message + 1 + 2 * 40, notif_pipe_path);
  
  int server_fd=open(server_pipe_path, O_WRONLY);
  
  ssize_t bytes_written = write(server_fd, message, 121);
  if(bytes_written<0){
    close(server_fd);
    pthread_exit(NULL);
    return 1;
  }
  if (bytes_written != 121) {
    perror("write");
    return 1;
  }
  int resp_pipe=open(resp_pipe_path,O_RDWR);

  char buf[2];
  ssize_t num_read = read(resp_pipe, buf, 2);
  if (num_read > 0){
    //printf("%c\n",buf[0]);
    if(buf[0]=='1'){
      if(buf[1]=='1'){
        printf("Server returned 1 for operation: connect\n");
        return 1;
      }
      else{
        printf("Server returned 0 for operation: connect\n");
      }
    }
  }

  int noti_pipe=open(notif_pipe_path,O_RDWR);

  pthread_t thread;
  //GERIR O PIPE DE NOTIFICAÇÕES
  if (pthread_create(&thread, NULL, notiThread, (void *)(intptr_t)noti_pipe) ) {
    perror("pthread_create failed");
  }
  return 0;
}

int kvs_disconnect(int req_pipe,int resp_pipe) {
  // close pipes and unlink pipe files
  char message[1]="2";
  ssize_t bytes_written =write(req_pipe,message,1);
  if(bytes_written<0){
    close(req_pipe);
    close(resp_pipe);
    pthread_exit(NULL);
    return 1;
  }

  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      //printf("%c\n",buf[0]);
      if(buf[0]=='2'){
        if(buf[1]=='0')
          printf("Server returned 0 for operation: disconnect\n");
        else
          printf("Server returned 1 for operation: disconnect\n");
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
    memcpy(message+1, key, 41); 
  } else {
    
    memcpy(message+1, key, len);
    memset(message+1 + len, '\0', 41 - len);
  }
  ssize_t bytes_written = write(req_pipe, message, 42);
  if(bytes_written<0){
    close(req_pipe);
    close(resp_pipe);
    pthread_exit(NULL);
    return 1;
  }
  if (bytes_written != 42) {
    perror("write");
    return 1;
  }
  
  
  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      if(buf[0]=='3'){
        if(buf[1]=='0')
          printf("Server returned 0 for operation: subscribe\n");
        else
          printf("Server returned 1 for operation: subscribe\n");
      }
      break;
    }
  }
  return 0;
}

int kvs_unsubscribe(const char *key,int req_pipe,int resp_pipe) {
  char message[42];
  message[0]='4';
  size_t len = strlen(key);
  if (len >= 40) {
    memcpy(message+1, key, 41); 
  } else {
    memcpy(message+1, key, len);
    memset(message+1 + len, '\0', 41 - len);
  }
  ssize_t bytes_written = write(req_pipe, message, 42);
  if(bytes_written<0){
    close(req_pipe);
    close(resp_pipe);
    pthread_exit(NULL);
    return 1;
  }
  if (bytes_written != 42) {
    perror("write");
    return 1;
  }

  while(1){
    char buf[2];
    ssize_t num_read = read(resp_pipe, buf, 2);
    
    if (num_read > 0){
      if(buf[0]=='4'){
        if(buf[1]=='0')
          printf("Server returned 0 for operation: unsubscribe\n");
        else
          printf("Server returned 1 for operation: unsubscribe\n");
      }
      break;
    }
  }
  return 0;
}

void *notiThread(void *arg){
  int noti_pipe= (int)(intptr_t)arg;
  char buf[256];
  while(1){
    ssize_t num_read = read(noti_pipe,buf,256);
    if(num_read<0){
      close(noti_pipe);
      pthread_exit(NULL);
      return (void *)1;
    }
    if(num_read > 0){
      printf("%s\n",buf);
    }
  }
}
