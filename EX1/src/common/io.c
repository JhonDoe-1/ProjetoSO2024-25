#include "io.h"

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

#include "constants.h"

int read_all(int fd, void *buffer, size_t size, int *intr) {
  if (intr != NULL && *intr) {
    return -1;
  }
  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t result = read(fd, buffer + bytes_read, size - bytes_read);
    if (result == -1) {
      if (errno == EINTR) {
        if (intr != NULL) {
          *intr = 1;
          if (bytes_read == 0) {
            return -1;
          }
        }
        continue;
      }
      perror("Failed to read from pipe");
      return -1;
    } else if (result == 0) {
      return 0;
    }
    bytes_read += (size_t)result;
  }
  return 1;
}

int read_string(int fd, char *str) {
  ssize_t bytes_read = 0;
  char ch;
  while (bytes_read < MAX_STRING_SIZE - 1) {
    if (read(fd, &ch, 1) != 1) {
      return -1;
    }
    if (ch == '\0' || ch == '\n') {
      break;
    }
    str[bytes_read++] = ch;
  }
  str[bytes_read] = '\0';
  return (int)bytes_read;
}

int write_all(int fd, const void *buffer, size_t size) {
  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t result = write(fd, buffer + bytes_written, size - bytes_written);
    if (result == -1) {
      if (errno == EINTR) {
        // error for broken PIPE (error associated with writting to the closed
        // PIPE)
        continue;
      }
      perror("Failed to write to pipe");
      return -1;
    }
    bytes_written += (size_t)result;
  }
  return 1;
}

static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void delay(unsigned int time_ms) {
  struct timespec delay = delay_to_timespec(time_ms);
  nanosleep(&delay, NULL);
}

int initialize_pipe(int* register_fifo, const char* register_pipe_path) {
  // Remove pipe if it exists already
  if (unlink(register_pipe_path) && errno != ENOENT) {
    fprintf(stderr, "[ERR]: Failed to unlink %s: %s\n", register_pipe_path, strerror(errno));
    return 1;
  }

  // Create pipe
  if (mkfifo(register_pipe_path, 0640)) {
    fprintf(stderr, "[ERR]: Failed to create register fifo %s: %s\n", register_pipe_path, strerror(errno));
    return 1;
  }

  /*if (DEBUG_IO) {
    printf("[DEBUG]: Opening pipe \"%s\" (O_RDWR)\n", register_pipe_path);
  }*/
  // Opening with Read Write causes thread to block when nothing is sent (instead of EOF)
  *register_fifo = open(register_pipe_path, O_RDWR);
  if (*register_fifo < 0) {
    fprintf(stderr, "[ERR]: Failed to open register fifo \"%s\": %s\n", register_pipe_path, strerror(errno));
    return 1;
  }

  return 0;
}

// Function to pad a string with '\0' to ensure it has exactly 40 characters
void pad_string(char *dest, const char *src) {
    size_t len = strlen(src);
    if (len >= 40) {
        memcpy(dest, src, 40); // Copy up to 40 characters
    } else {
        // Copy the string and pad the rest with '\0'
        memcpy(dest, src, len);
        memset(dest + len, '\0', 40 - len);
    }
}
