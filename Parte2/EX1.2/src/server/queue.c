#include "queue.h"

#include <stdio.h>
#include <stdlib.h>

queue_t *queue;

int queue_init() {
  if (queue) return 1;
  queue = malloc(sizeof(queue_t));
  if (queue == NULL) 
    return 1;
  if (pthread_mutex_init(&queue->mutex, NULL)) 
    return 1;
  if (pthread_cond_init(&queue->full, NULL)) 
    return 1;
  if (pthread_cond_init(&queue->empty, NULL)) 
    return 1;
  queue->session_count = queue->host = queue->worker = 0;
  return 0;
}

void queue_destroy() {
  pthread_mutex_destroy(&queue->mutex);
  pthread_cond_destroy(&queue->full);
  pthread_cond_destroy(&queue->empty);
  free(queue);
  queue = NULL;
}

void queue_produce(session_t *session) {
  pthread_mutex_lock(&queue->mutex);
  while (queue->session_count == QUEUE_BUFFER_SIZE) 
    pthread_cond_wait(&queue->full, &queue->mutex);
  queue->buffer[queue->host] = *session;
  queue->host++;
  if (queue->host == QUEUE_BUFFER_SIZE) 
    queue->host = 0;
  queue->session_count++;
  pthread_cond_signal(&queue->empty);
  pthread_mutex_unlock(&queue->mutex);
}

void queue_consume(session_t *session, unsigned int session_id) {
  pthread_mutex_lock(&queue->mutex);
  while (queue->session_count == 0) {
    pthread_cond_wait(&queue->empty, &queue->mutex);
  }
  *session = queue->buffer[queue->worker];
  queue->worker++;
  if (queue->worker == QUEUE_BUFFER_SIZE) 
    queue->worker = 0;
  queue->session_count--;
  pthread_cond_signal(&queue->full);
  pthread_mutex_unlock(&queue->mutex);
  session->id = (int)session_id;
}
