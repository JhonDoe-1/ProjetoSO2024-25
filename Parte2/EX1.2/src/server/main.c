#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdbool.h>
#include <errno.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "queue.h"
#include "pthread.h"
#include "../common/io.h"
#include "../common/constants.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

session_t *sessions[MAX_SESSION_COUNT];
int connected_ids=0;
int active_threads=1;
size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;
int sigusr1_triggered = false;

int filter_job_files(const struct dirent *entry);
static int entry_files(const char *dir, struct dirent *entry, char *in_path,char *out_path);
static int run_job(int in_fd, int out_fd, char *filename);
static void *get_file(void *arguments);
static void dispatch_threads(DIR *dir,int server_fd);
void create_session(session_t *session, char req_pipe_path[],char resp_pipe_path[],char noti_pipe_path[]);
void *manager_thread(void *arg);
int addKey(char array[][MAX_STRING_SIZE],char key[]);
int removeKey(char array[][MAX_STRING_SIZE],char key[]);
void updateKey(size_t num_pairs,char keys[][MAX_STRING_SIZE],char values[][MAX_STRING_SIZE], int mode);
int existentKey(char array[][MAX_STRING_SIZE],char key[]);
int remove_session(session_t *session);


static void sig_handler(int sig) {
  sigusr1_triggered = true;
  if (signal(sig, sig_handler) == SIG_ERR) fprintf(stderr, "[ERR]: Failed to set signal handler\n");
}

int main(int argc, char **argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }
  char server_pipe_path[256] = "/tmp/";
  strncat(server_pipe_path, argv[4], strlen(argv[4]) * sizeof(char));
  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }
  // Initialize pipe
  int register_fifo;
  if (initialize_pipe(server_pipe_path)) {
    return 1;
  }
  openPipe(&register_fifo, server_pipe_path);

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }
  //initialize producer-consumer buffer
  if (queue_init()) 
    return 1;
  
  dispatch_threads(dir,register_fifo);


  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }
  unlink(argv[4]);
  kvs_terminate();
  queue_destroy();
  return 0;
}

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      updateKey(num_pairs,keys,values,0);
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      updateKey(num_pairs,keys,values,1);
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void dispatch_threads(DIR *dir ,int server_fd) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }
  // Set signal handler for sigusr1
  if (signal(SIGUSR1, sig_handler) == SIG_ERR) {
    fprintf(stderr, "[ERR]: Failed to set signal handler\n");
    return;
  }
  // Create worker threads
  
  pthread_t thread[MAX_SESSION_COUNT];
  for (size_t i = 0; i < MAX_SESSION_COUNT; i++) {
    pthread_create(thread + i, NULL, manager_thread, (void *)i);
  }
  // ler do FIFO de registo
  while(1){
    if(sigusr1_triggered==true){
      printf("Shutting down sessions...\n");
      for(int i=0;i<MAX_SESSION_COUNT;i++){
        if(sessions[i]!=NULL){
          printf("Removing session %d\n",sessions[i]->id);
          unlink(sessions[i]->request_pipe);
          unlink(sessions[i]->response_pipe);
          unlink(sessions[i]->noti_pipe);
          free(sessions[i]);
          sessions[i]=NULL;
        }
        
      }
      connected_ids=0;
      active_threads=1;
      sigusr1_triggered=false;
    }
    char buf[1];
    ssize_t num_read = read(server_fd, buf, 1);
    if (num_read > 0){
      if(buf[0]=='1'){
        session_t session;
        char req_pipe_path[41];
        char resp_pipe_path[41];
        char noti_pipe_path[41];
        read(server_fd,req_pipe_path,40);req_pipe_path[40]='\0';
        read(server_fd,resp_pipe_path,40);resp_pipe_path[40]='\0';
        read(server_fd,noti_pipe_path,40);noti_pipe_path[40]='\0';

        create_session(&session, req_pipe_path,resp_pipe_path,noti_pipe_path);
        //printf("Placing session %s in queue\n",session.request_pipe);
        queue_produce(&session);
        //printf("Session %s is now in queue\n",session.request_pipe);

      }
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}
void *manager_thread(void *arg) {
  unsigned int session_id = (unsigned int)(intptr_t)arg;
  session_t *session=malloc(sizeof(session_t));
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL)) {
    fprintf(stderr, "Failed to block signal\n");
    return (void *)1;
  }
  
  while (1) {
    // Take session from producer-consumer buffer
    int resp_pipe, req_pipe;
    queue_consume(session, session_id);
    for (int i=0;i<MAX_SESSION_COUNT;i++){
      if(sessions[i]==NULL){
        sessions[i]=session;
        break;
      }
      
    }
    //printf("Consumed session for %s\n",session->request_pipe);
    resp_pipe=open(session->response_pipe, O_WRONLY);
    write(resp_pipe,"10",2);
    close(resp_pipe);

    while(1){
     req_pipe=open(session->request_pipe, O_RDONLY);
     resp_pipe=open(session->response_pipe, O_WRONLY);
      if (req_pipe<0||resp_pipe<0){
        close(req_pipe);
        close(resp_pipe);
        break;
      }
      char buf[1];
      ssize_t num_read = read(req_pipe, buf, 1);

      if(num_read<0){
        close(req_pipe);
        close(resp_pipe);
        break;
      }
      if (num_read > 0){
        if(buf[0]=='2'){
          printf("Disconect on session %d\n",session->id);
          if(remove_session(session)==0){
            char message[2]="20";
            write(resp_pipe,message,2);
          }
          else{
            char message[2]="21";
            write(resp_pipe,message,2);
          }
          connected_ids--;
          close(req_pipe);
          close(resp_pipe);
          break;
        }


        if(buf[0]=='3'){
          printf("Subscribe on session %d\n",session->id);
          char key[41];
          read(req_pipe,key,41);
          if(checkKey(key)==0&&addKey(session->keys,key)==0){
            char message[2]="31";
            write(resp_pipe,message,2);
            session->num_keys++;
          }
          else{
            char message[2]="30";
            write(resp_pipe,message,2);
          }
        }


        if(buf[0]=='4'){
          printf("Unsubscribe on session %d\n",session->id);
          char key[41];
          read(req_pipe,key,41);
          if(removeKey(session->keys,key)==0){
            char message[2]="41";
            write(resp_pipe,message,2);
            session->num_keys++;
          }
          else{
            char message[2]="40";
            write(resp_pipe,message,2);
          }
        }
      }
      close(resp_pipe);
      close(req_pipe);
    }
  }
  return (void *)0;
}


void create_session(session_t *session, char req_pipe_path[],char resp_pipe_path[],char noti_pipe_path[]){  
  strncpy(session->request_pipe, req_pipe_path,MAX_PIPE_PATH_LENGTH);
  strncpy(session->response_pipe, resp_pipe_path,MAX_PIPE_PATH_LENGTH);
  strncpy(session->noti_pipe, noti_pipe_path,MAX_PIPE_PATH_LENGTH);
  session->num_keys=0;
  for(int j=0;j<MAX_NUMBER_SUB;j++){
    session->keys[j][0]='\0';
  }

}

int addKey(char array[][MAX_STRING_SIZE],char key[]){
  if(existentKey(array,key)>=0){

    return 1;
  }
  for(int i=0; i<MAX_NUMBER_SUB;i++){
    if(array[i]==NULL||array[i][0]=='\0'){
      strncpy(array[i],key,MAX_STRING_SIZE);
      return 0;
    }
  }
  return 1;
}
int removeKey(char array[][MAX_STRING_SIZE],char key[]){
  int i=existentKey(array,key);
  if(i<0){
    return 1;
  }
  
  size_t len=strlen(array[i]);
  for(size_t j=0; j<len;j++){
    array[i][j]='\0';
  }
  return 0;

}

void updateKey(size_t num_pairs,char keys[][MAX_STRING_SIZE],char values[][MAX_STRING_SIZE], int mode){

  for(size_t i=0;i<num_pairs;i++){
    //Lookig for a session following keys[i]
    for(size_t j=0;j<MAX_SESSION_COUNT;j++){
      if(sessions[j]!=NULL){
        //Trying in sessions[j]->id
        for(size_t k=0;k<MAX_NUMBER_SUB;k++){
          if(strcmp(sessions[j]->keys[k],keys[i])==0){
            //Found in session sessions[j]->id
            char message[256];
            if(mode==0){
              sprintf(message,"(%s,%s)",keys[i],values[i]);
            }
            if(mode==1){
              sprintf(message,"(%s,DELETED)",keys[i]);
              removeKey(sessions[i]->keys, keys[i]);
            }
            int noti_pipe=open(sessions[j]->noti_pipe,O_WRONLY);
            //Writing to pipe
            write(noti_pipe,message,strlen(message));
          }
          break;
           
        }

      }
    }
  }

}


int remove_session(session_t *session){
  for(int i=0;i<MAX_SESSION_COUNT;i++){
    if(sessions[i]!=NULL){
      if(sessions[i]->id==session->id){
        free(sessions[i]);
        sessions[i]=NULL;
        session=NULL;
        active_threads--;
        return 0;
      }
    }
    
  }
  return 1;
}

int existentKey(char array[][MAX_STRING_SIZE],char key[]){
  for(int i=0;i<MAX_NUMBER_SUB;i++){
    if(strcmp(array[i],key)==0){
      return i;
    }
  }
  return -1;
}