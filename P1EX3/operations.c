#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <pthread.h> // Required for read-write locks

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;
pthread_mutex_t kvs_mutex = PTHREAD_MUTEX_INITIALIZER;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_lock(&kvs_mutex);
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
    pthread_mutex_unlock(&kvs_mutex);
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  dprintf(output_fd,"[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      dprintf(output_fd,"(%s,KVSERROR)", keys[i]);
    } else {
      dprintf(output_fd,"(%s,%s)", keys[i], result);
    }
    free(result);
  }
  dprintf(output_fd,"]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        dprintf(output_fd,"[");
        aux = 1;
      }
      dprintf(output_fd,"(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    dprintf(output_fd,"]\n");
  }
  pthread_mutex_unlock(&kvs_mutex);
  return 0;
}

void kvs_show(int output_fd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      pthread_mutex_lock(&kvs_mutex);
      dprintf(output_fd,"(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
      pthread_mutex_unlock(&kvs_mutex);
    }
  }
}

int kvs_backup(char backupFileName[]) {
  
  if (kvs_table == NULL) {
    //fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  int backup_fd = open(backupFileName, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if(backup_fd<0){
    perror("Failed to create backup file");
    close(backup_fd);
    return 1;
  }
  
  // Execute command
  kvs_show(backup_fd);
  // Restore stdout
  close(backup_fd);
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}