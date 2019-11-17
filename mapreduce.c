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
    
}fileName;

typedef struct {
    int index;
    int MAX_SIZE;
}Counter;

Map maps;
Reducer reduces;
Partitioner partitions;

MR **table;
fileName *fname1;
Counter *pnum;

static int cmpstringp(const void *p1, const void *p2)
{
    const MR table1 =  * (MR *)p1;
    const MR table2 = * (MR *)p2;
    return strcmp(table1.key, table2.key);
}


void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 

    // TODO: What should be the length of the array? Need to figure out realloc()
    // Use locks
    unsigned long pno;
    pno = 1;
    // pno = (*partitions)(key, num_partitions);
    if(pnum[pno].index == pnum[pno].MAX_SIZE)
    {
        pnum[pno].MAX_SIZE = pnum[pno].MAX_SIZE * 2;
        table[pno] = (MR *)realloc(table[pno], (sizeof(MR) * pnum[pno].MAX_SIZE));
        if (table[pno] == NULL)
        {
            printf("Memory Allocation Failed!\n");
            exit(1);
        }
    }
    if(pnum[pno].index == 0)
    {
        table[pno] = (MR *)malloc(sizeof(MR) * pnum[pno].MAX_SIZE);
        if (table[pno] == NULL)
        {
            printf("Memory Allocation Failed!\n");
            exit(1);
        }
        
    }
    // if(pnum[pno]->index < MAX_SIZE) {
        
    // }

    // table[pno] = malloc(sizeof(MR) * length?);
    table[pno][pnum[pno].index].key = key;
    table[pno][pnum[pno].index].value = value;
    // printf("Inserted: %s\n", table[pno][pnum[pno].index].key);
    pnum[pno].index++;
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

    // global_np = num_partitions;
    // This next line is sketchy. Change it later
    // pnum = calloc(num_partitions, sizeof(Counter));

    // Set the initial values of the partition counters
    pnum = (Counter *)malloc(sizeof(Counter) * num_partitions);
    for(int i = 0; i < num_partitions; i++)
    {
        pnum[i]->index = 0;
        pnum[i]->MAX_SIZE = 5;
    }
    
    pthread_t p[num_mappers];
    pthread_t q[num_reducers];

    //struct MR *table = malloc(sizeof(MR) * num_partitions);
    table = (MR **)malloc(sizeof(MR *) * num_partitions);
    if (table == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }

    // TODO: Need to do some sort of scheduling to map the files to the mappers
    // and maybe pass those as parameters to mappers_exe

    

    for(i = 0; i < num_mappers; i++) {
        pthread_create(&p[i], NULL, mapper_exe, (void *)&fname[i]);
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_join(p[i], NULL);
    }
    
    // Code for qsort should go in here.
    // qsort();
    for(int i = 0; i < num_partitions; i++)
        qsort(table[1], pnum[1].index, sizeof(MR), cmpstringp);


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

