#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"

#define PIPE_NAME_SIZE 40
#define MAX_KEY_SIZE 40
#define OP_CONNECT 1
#define OP_DISCONNECT 2
#define OP_SUBSCRIBE 3
#define OP_UNSUBSCRIBE 4
#define OP_NOTIFICATION 5
#define MAX_SUBSCRIPTIONS 100

// Estrutura para armazenar subscrições de chaves
typedef struct {
  char key[MAX_KEY_SIZE];
  int notification_fd;
} Subscription;

// Estrutura para representar um cliente conectado
typedef struct {
  int active;
  int request_fd;
  int response_fd;
  int notification_fd;
  Subscription subscriptions[MAX_SUBSCRIPTIONS];
  int subscription_count;
} ClientSession;

ClientSession client_session;

void process_request(int server_fd, const char *jobs_dir);
void handle_connect(char *buffer);
void handle_disconnect();
void handle_subscribe(char *buffer);
void handle_unsubscribe(char *buffer);
void notify_clients(const char *key, const char *value);

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;


void process_request(int server_fd, const char *jobs_dir) {
  char buffer[128] = {0};
  if (read(server_fd, buffer, sizeof(buffer)) > 0) {
      char op_code = buffer[0];
      switch (op_code) {
          case OP_CONNECT:
              handle_connect(buffer);
              break;
          case OP_DISCONNECT:
              handle_disconnect();
              break;
          case OP_SUBSCRIBE:
              handle_subscribe(buffer);
              break;
          case OP_UNSUBSCRIBE:
              handle_unsubscribe(buffer);
              break;
          default:
              fprintf(stderr, "Unknown operation code: %d\n", op_code);
      }
  }
}

void handle_connect(char *buffer) {
  if (client_session.active) {
      fprintf(stderr, "Server already has an active session.\n");
      return;
  }

  char *request_pipe = buffer + 1;
  char *response_pipe = buffer + 1 + PIPE_NAME_SIZE;
  char *notification_pipe = buffer + 1 + 2 * PIPE_NAME_SIZE;

  client_session.request_fd = open(request_pipe, O_RDONLY);
  client_session.response_fd = open(response_pipe, O_WRONLY);
  client_session.notification_fd = open(notification_pipe, O_WRONLY);

  if (client_session.request_fd == -1 || client_session.response_fd == -1 || client_session.notification_fd == -1) {
      perror("Error opening client FIFOs");
      return;
  }

  client_session.active = 1;
  client_session.subscription_count = 0;

  char response[2] = {OP_CONNECT, 0};
  write(client_session.response_fd, response, sizeof(response));
  printf("Client connected.\n");
}

void handle_disconnect() {
  if (!client_session.active) {
      fprintf(stderr, "No active client session to disconnect.\n");
      return;
  }

  close(client_session.request_fd);
  close(client_session.response_fd);
  close(client_session.notification_fd);

  memset(&client_session, 0, sizeof(ClientSession));
  printf("Client disconnected.\n");
}

void handle_subscribe(char *buffer) {
  if (!client_session.active) {
      fprintf(stderr, "No active client session for subscription.\n");
      return;
  }

  char *key = buffer + 1;

  if (client_session.subscription_count >= MAX_SUBSCRIPTIONS) {
      fprintf(stderr, "Maximum subscriptions reached.\n");
      return;
  }

  for (int i = 0; i < client_session.subscription_count; ++i) {
      if (strcmp(client_session.subscriptions[i].key, key) == 0) {
          fprintf(stderr, "Key already subscribed.\n");
          return;
      }
  }

  strcpy(client_session.subscriptions[client_session.subscription_count].key, key);
  client_session.subscriptions[client_session.subscription_count].notification_fd = client_session.notification_fd;
  client_session.subscription_count++;

  char response[2] = {OP_SUBSCRIBE, 0};
  write(client_session.response_fd, response, sizeof(response));
  printf("Client subscribed to key: %s\n", key);
}

void handle_unsubscribe(char *buffer) {
  if (!client_session.active) {
      fprintf(stderr, "No active client session for unsubscription.\n");
      return;
  }

  char *key = buffer + 1;

  for (int i = 0; i < client_session.subscription_count; ++i) {
      if (strcmp(client_session.subscriptions[i].key, key) == 0) {
          // Remove subscription
          for (int j = i; j < client_session.subscription_count - 1; ++j) {
              client_session.subscriptions[j] = client_session.subscriptions[j + 1];
          }
          client_session.subscription_count--;

          char response[2] = {OP_UNSUBSCRIBE, 0};
          write(client_session.response_fd, response, sizeof(response));
          printf("Client unsubscribed from key: %s\n", key);
          return;
      }
  }

  char response[2] = {OP_UNSUBSCRIBE, 1};
  write(client_session.response_fd, response, sizeof(response));
  fprintf(stderr, "Key not found for unsubscription: %s\n", key);
}

void notify_clients(const char *key, const char *value) {
  if (!client_session.active) {
      return;
  }

  for (int i = 0; i < client_session.subscription_count; ++i) {
      if (strcmp(client_session.subscriptions[i].key, key) == 0) {
          char notification[82] = {0};
          strncpy(notification, key, MAX_KEY_SIZE);
          strncpy(notification + MAX_KEY_SIZE, value, MAX_KEY_SIZE);
          write(client_session.notification_fd, notification, sizeof(notification));
          printf("Notified client about key: %s, value: %s\n", key, value);
      }
  }
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

static void dispatch_threads(DIR *dir) {
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

  // ler do FIFO de registo

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

// Function to set up the named pipe and start the server
int setup_named_pipe(const char *pipe_path) {
  // Attempt to create the named pipe
  unlink(pipe_path);
  if (mkfifo(pipe_path, 0666) < 0) {
    perror("mkfifo");
    return -1;
  }

  return 0;
}

int main(int argc, char **argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <nome_do_FIFO_de_registo> \n");
    return 1;
  }

  jobs_directory = argv[1];

  const char *register_fifo = argv[4];

    // Criar o named pipe de registro
    if (mkfifo(register_fifo, 0666) == -1) {
        perror("Error creating register FIFO");
        return 1;
    }

    int server_fd = open(register_fifo, O_RDONLY);
    if (server_fd == -1) {
        perror("Error opening register FIFO");
        return 1;
    }

    memset(&client_session, 0, sizeof(ClientSession));

    printf("Server is running and waiting for connections...\n");
    while (1) {
        process_request(server_fd, jobs_dir);
    }//todo!!!!!!!!!!!!!!!!!!!!!!!



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

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();
  
  close(server_fd);
  unlink(register_fifo);
  return 0;
}
