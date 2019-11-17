#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

// TODO: Change implementation to linked lists!!

// Declaring data structure
typedef struct MR{
  char *key;
  char *value;
}MR;

typedef struct {
    char *argv[];
    int argc;
}fileName;

typedef struct {
    int index;
}Counter;

Map maps;
Reducer reduces;
Partitioner partitions;

MR **table;
fileName *fname1;
Counter *pnum;

void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 

    // TODO: What should be the length of the array? Need to figure out realloc()
    // Use locks
    unsigned long pno;
    pno = (*partitions)(key, num_partitions);
    table[pno] = malloc(sizeof(MR) * length?);
    table[pno][pnum[pno]->index]->key = key;
    table[pno][pnum[pno]->index]->value = value;
    pnum[pno]->index++;
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
    // TODO: ensures  that keys are in a sorted order across the partitions 
    // (i.e., keys are not hashed into random partitions as in the default partition function)

    // TODO: single call to qsort for each partition
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    int i;

    maps = map;
    reduces = reduce;
    partitions = partition;

    pnum = calloc(num_partitions, sizeof(Counter));

    pthread_t p[num_mappers];
    pthread_t q[num_reducers];

    //struct MR *table = malloc(sizeof(MR) * num_partitions);
    table = (MR **)malloc(sizeof(MR *) * num_partitions);
    if (table == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }

    // Note: Need to put this in MR_Emit()? Also need to figure out realloc for expansion
    //unsigned long pno;
    // for(i = 0; i < num_partitions; i++) {
    //     //pno = (*partitions)(key, num_partitions);
    //     table[i] = (MR *)malloc(sizeof(MR) * 10000);
    //     if (table[i] == NULL)
    //     {
    //         printf("Memory Allocation Failed!\n");
    //         exit(1);
    //     }
    // }

    // TODO: Need to do some sort of scheduling to map the files to the mappers
    // and maybe pass those as parameters to mappers_exe

    // #ofFiles = #ofThreads
    for(i = 0; i < argc - 1; i++) {
        fname1[i]->argc = argc;
        fname1[i]->argv = argv[i+1];
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_create(&p[i], NULL, mapper_exe, (void *)&fname1[i]);
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_join(p[i], NULL);
    }
    
    sort();

    for(i = 0; i < num_reducers; i++) {
        pthread_create(&q[i], NULL, reducer_exe, (void *)??);
    }

    for(i = 0; i < num_reducers; i++) {
        pthread_join(q[i], NULL);
    }
}

void *mapper_exe(void *arg) { 
    // Case 1: Calling map once for each file
    struct fileName *fname2 = (struct MR *)arg;
    for(int i = 0;i < fname2->argc - 1; i++) {
        map(fname2->argv[i]);
    }
    return NULL;
}
void *reducer_exe(void *arg) {

}

char *get_next(char *key, int num_partitions) {
    unsigned long pno;
    pno = (*partitions)(key, num_partitions);

    return table[pno][pnum[pno]->index]->value;
}

