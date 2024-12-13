#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

void process_job_file(struct dirent *dp, int max_backups );
void execute_command(enum Command cmd, int fd, int max_backups ,int *backupCounter,char inputFileName[],int output_fd);

int activeBackups=0; //vaiacel golbal para saber o nmr de processos de backup ativos no momento

int main(int argc, char *argv[]) {

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  if (argc != 3) {
    fprintf(stderr, "Usage: %s <directory_path> <max_backups>\n", argv[0]);
    return 1;
  }

  char *directory_path = argv[1];
  int max_backups = atoi(argv[2]);
  if (max_backups < 1) {
    fprintf(stderr, "Invalid max_backups value.\n");
    return 1;
  }
  //Abre a diretoria especificada
  DIR *dir = opendir(directory_path);
  if (!dir) {
    perror("Failed to open directory");
    exit(EXIT_FAILURE);
  }
  chdir(directory_path);
  //Lê cada ficheiro na diretoria 
  struct dirent *dp;
  while ((dp = readdir(dir)) != NULL) {
    //Se o nome do ficheiro contiver ".job" chama a função process_job_file
    if (strstr(dp->d_name, ".job")) {
      process_job_file(dp,max_backups);
    }

  }

  closedir(dir);
}

void process_job_file(struct dirent *dp, int max_backups ) {
    int value = 0;
    int *backupCounter=&value;
    char outputFileName[MAX_JOB_FILE_NAME_SIZE];
    strcpy(outputFileName, dp->d_name);
    //Abrir o ficheiro de input
    int input_fd = open(dp->d_name, O_RDONLY);
    if (input_fd < 0) {
      perror("Failed to open input file");
      return;
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
        return;
    }
    
    // Processar cada comando do ficheiro
    
    while (1) {
      enum Command cmd = get_next(input_fd);
      if (cmd == EOC) break;
      // Executar cada comando
      execute_command(cmd, input_fd,max_backups,backupCounter,dp->d_name,output_fd);
    }
    
    close(input_fd);
    close(output_fd);
}

void execute_command(enum Command cmd, int fd, int max_backups ,int *backupCounter,char inputFileName[],int output_fd) {
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

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          return;
        }

        if (kvs_read(num_pairs, keys,output_fd)) {
          fprintf(stderr, "Failed to read pair\n");
        }
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
        (*backupCounter)++;

        //nomear o ficheiro de backup
        char tempFileName[MAX_JOB_FILE_NAME_SIZE];
        char backupFileName[MAX_JOB_FILE_NAME_SIZE+8];
        strcpy(tempFileName, inputFileName);
        size_t len = strlen(tempFileName);
        if (len >= 4) {
          tempFileName[len-4] = 0;
        }
        
        
        sprintf(backupFileName,"%s-%d.bck",tempFileName,*backupCounter);
        // Verificar limite de backups simultâneos
        while (activeBackups >=max_backups) {
            wait(NULL);  // Esperar por um backup concluir
            activeBackups--;
        }
        pid_t pid = fork();
        if (pid == 0) {
            // Processo filho realiza o backup
            if (kvs_backup(backupFileName)) {
              fprintf(stderr, "Failed to perform backup.\n");
              backupCounter--;
            }
            exit(0);
        } else if (pid > 0) {
            // Processo pai incrementa o contador de backups ativos
            fprintf(stderr,"Processo filho com PID %d iniciou o processamento do backup %d do fihceiro '%s'\n", pid,*backupCounter,inputFileName);
            activeBackups++;
        } else {
            perror("Erro ao criar processo filho");
            exit(1);
        }
        
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
            "  BACKUP\n" // Not implemented
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


