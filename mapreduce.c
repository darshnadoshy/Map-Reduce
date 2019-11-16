#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

// Declaring data structure
typedef struct MR {
  char *key;
  char *value;
  int partition_num;
}*table;

void MR_Emit(char *key, char *value) {
    // Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 
}

// Got it from the specs
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    // ensures  that keys are in a sorted order across the partitions 
    // (i.e., keys are not hashed into random partitions as in the default partition function)
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    // Use Mapper to map filenames to Map() ??
    int i,j;
    // struct MR params[num_mappers];
    pthread_t p[num_mappers];
    pthread_t p[num_reducers];

    //struct MR *table = malloc(sizeof(MR) * num_partitions);
    table = malloc(sizeof(MR) * num_partitions);
    if (table == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }

    // Need to do some sort of scheduling to map the files to the mappers
    // and maybe pass those as parameters to mappers_exe

    for(i = 0; i < num_mappers; i++) {
        pthread_create(&p[i], NULL, mappers_exe, (void *)&params[i]);
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_join(p[i], NULL);
    }
}

void *mappers_exe(void *arg) { 
    // struct MR *params = (struct MR *)arg;
    // for(int i = 0;i < argc - 1; i++) {
    //     map(argv[i]);
    // }
    return NULL;
}

