#ifndef SERVER_QUEUE_H
#define SERVER_QUEUE_H
#include <pthread.h>

#include "../common/constants.h"
#include "parser.h"

typedef struct {
  session_t buffer[QUEUE_BUFFER_SIZE];
  pthread_mutex_t mutex;
  pthread_cond_t full;
  pthread_cond_t empty;
  int session_count;
  int host;
  int worker;
} queue_t;

/// @brief Initializes global producer-consumer buffer
/// @return 0 if no errors, 1 otherwise
int queue_init();

/// @brief Destroy global producer-consumer buffer
void queue_destroy();

/// @brief adds a session to the producer-consumer buffer.
/// @param session
void queue_produce(session_t *session);

/// @brief removes a session from the producer-consumer buffer.
/// @param session
/// @param session_id
void queue_consume(session_t *session, unsigned int session_id);

#endif  // SERVER_QUEUE_H
