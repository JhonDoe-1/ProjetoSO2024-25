#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <pthread.h> // Required for read-write locks

#include "constants.h"
#include "parser.h"
#include "operations.h"

pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER; // Read-write lock

void *process_job_file(void *arg);
void execute_command(enum Command cmd, int fd ,int output_fd);


int main(int argc, char *argv[]) {
  char *directory_path; 
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  if (argc != 4) {
    fprintf(stderr, "Usage: %s <directory_path> <max_backups>\n", argv[0]);
    return 1;
  }

  directory_path = argv[1];
  int max_backups = atoi(argv[2]);
  int max_threads = atoi(argv[3]);
  
  if (max_backups < 1) {
    fprintf(stderr, "Invalid max_backups value.\n");
    return 1;
  }
  DIR *dir;
  //Abre a diretoria especificada
  dir = opendir(directory_path);
  if (!dir) {
    perror("Failed to open directory");
    exit(EXIT_FAILURE);
  }

  chdir(directory_path);
  pthread_t threads[max_threads];
  int thread_count = 0;

  //Lê cada ficheiro na diretoria 
  struct dirent *dp;
  while ((dp = readdir(dir)) != NULL) {
    //Se o nome do ficheiro contiver ".job" chama a função process_job_file
    if (strstr(dp->d_name, ".job")) {
      // Cria uma thread para processar o ficheiro
      if (pthread_create(&threads[thread_count], NULL, process_job_file, dp) != 0) {
        perror("Failed to create thread");
        closedir(dir);
        return 1;
      }

      thread_count++;
      // Aguarda threads se o limite for atingido
      if (thread_count == max_threads) {
        for (int i = 0; i < thread_count; i++) {
          pthread_join(threads[i], NULL);
        }
        thread_count = 0;
      }
    }

  }
  // Espera pelas threads restantes
  for (int i = 0; i < thread_count; i++) {
    pthread_join(threads[i], NULL);
  }
  // Destroy the read-write lock
  pthread_rwlock_destroy(&rwlock);
  closedir(dir);
}




void *process_job_file(void *arg ) {
  struct dirent *dp = (struct dirent*) arg;
  char outputFileName[MAX_JOB_FILE_NAME_SIZE];
  strcpy(outputFileName, dp->d_name);
  //Abrir o ficheiro de input
  int input_fd = open(dp->d_name, O_RDONLY);
  if (input_fd < 0) {
    perror("Failed to open input file");
    close(input_fd);
    pthread_exit(NULL);
  }
  //Nome do ficheiro de output ex:. input1.job-> input1.out
  size_t len = strlen(outputFileName);
  if (len >= 4) {
    outputFileName[len-4] = 0;
  }
  strcat(outputFileName,".out");
  
  //Criar ficheiro de output
  int output_fd = open(outputFileName, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (output_fd < 0) {
    perror("Failed to open output file");
    close(input_fd);
    close(output_fd);
    pthread_exit(NULL);
  }
  
  while (1) {
    enum Command cmd = get_next(input_fd);
    if (cmd == EOC) break;
    // Executar cada comando
    execute_command(cmd, input_fd,output_fd);
  }
  close(input_fd);
  close(output_fd);
  pthread_exit(NULL);
}

void execute_command(enum Command cmd, int fd ,int output_fd) {
  char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  unsigned int delay;
  size_t num_pairs;

  switch (cmd) {
    case CMD_WRITE:
        num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);  
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          return;
        }
        // Acquire the write lock
        pthread_rwlock_wrlock(&rwlock);
        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }
        // Release the write lock
        pthread_rwlock_unlock(&rwlock);
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          return;
        }
        // Acquire the read lock
        pthread_rwlock_rdlock(&rwlock);
        if (kvs_read(num_pairs, keys,output_fd)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        // Release the read lock
        pthread_rwlock_unlock(&rwlock);
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          return;
        }

        if (kvs_delete(num_pairs, keys,output_fd)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show(output_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          return;
        }

        if (delay > 0) {
          printf("Waiting...\n");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" 
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        kvs_terminate();
        return;
  }
  
}

