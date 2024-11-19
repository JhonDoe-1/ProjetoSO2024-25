#include <stdio.h>
#include <string.h>

int main(void){
    char string[40]="hello.jobs";
    size_t len = strlen(string);
    if (len >= 5) {
        string[len-5] = 0;
    }
    strcat(string,".out");
    printf("%s\n",string);
    return 1;
}