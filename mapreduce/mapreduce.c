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
    int index;
    char *name[];
}fileName;

typedef struct {
    int index;
    unsigned long partition_no[];
}Part;

typedef struct {
    int index;
    int MAX_SIZE;
    int get_index;
}Counter;

Mapper maps;
Reducer reducers;
Partitioner partitions;

MR **table;
fileName *fname;
Part *part;
Counter *pnum;

int part_no;

static int cmpstringp(const void *p1, const void *p2)
{
    const MR table1 =  * (MR *)p1;
    const MR table2 = * (MR *)p2;
    return strcmp(table1.key, table2.key);
}

char *get_next(char *key, int num_partitions) {
    unsigned long pno;
    pno = (*partitions)(key, num_partitions);
    for(int i = pnum[pno].get_index; i <= pnum[pno].index; i++)
    {
        if(strcmp(table[pno][i].key, key) == 0)
        {
            pnum[pno].get_index++;
            return table[pno][i].value;
        }
    }
}

void *mapper_exe(void *arg) { 
    fileName *fname = (fileName *)arg;
    for(int i = 0; i < fname->index; i++){
        maps(fname->name[i]);
    }
    return NULL;
}
void *reducer_exe(void *arg) {
    Part *part = (Part *)arg;
    unsigned long partition_num; 
    char *distinctKey, *prevKey;
    for(int i = 0; i < part->index; i++) {
        partition_num = part->partition_no[i];
        if(pnum[partition_num].index != 0){
            strncpy(prevKey, table[partition_num][0].key, strlen(table[partition_num][0].key));
            for(int j = 0; j < pnum[partition_num].index; j++) {
                strncpy(distinctKey, table[partition_num][j].key, strlen(table[partition_num][0].key));
                if(strcmp(distinctKey, prevKey) != 0) {
                    reducers(distinctKey, get_next, partition_num);
                }
                strncpy(prevKey, distinctKey, strlen(distinctKey));
            }
        }
        else {
            return NULL;
        }
    }
    return NULL;
}

void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 

    // TODO: What should be the length of the array? Need to figure out realloc()
    // Use locks
    unsigned long pno;
    pno = (*partitions)(key, part_no);
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
    table[pno][pnum[pno].index].key = key;
    table[pno][pnum[pno].index].value = value;
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

}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    int i, j;

    maps = (Mapper)map;
    reducers = reduce;
    partitions = partition;
    part_no = num_partitions;

    // global_np = num_partitions;
    // This next line is sketchy. Change it later
    // pnum = calloc(num_partitions, sizeof(Counter));

    // Set the initial values of the partition counters
    pnum = (Counter *)malloc(sizeof(Counter) * num_partitions);
    for(int i = 0; i < num_partitions; i++)
    {
        pnum[i].index = 0;
        pnum[i].MAX_SIZE = 5;
        pnum[i].get_index = 0;
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

    int pos;
    fname = (fileName *)malloc(sizeof(fileName) * num_mappers);
    if (fname == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for(int i = 0; i < num_mappers; i++)
    {
        fname[i].index = 0;
    }

    // Mapping files to their threads
    // TODO: Sort files according to their size and then map them
    for (i = 1; i < argc; i++) {
        pos = i % num_mappers;
        if(pos == 0) {
            fname[num_mappers - 1].name[fname[num_mappers - 1].index] = argv[i];
            fname[num_mappers - 1].index++;
        }
        else {
            fname[pos - 1].name[fname[pos - 1].index] = argv[i];
            fname[pos - 1].index++;
        }   
    }
    for(i = 0; i < num_mappers; i++) {
        if(fname[i].index == 0) {
            fname[i].name[0] = NULL;
        }
    }

    // Creating mapper threads
    for(i = 0; i < num_mappers; i++) {
        pthread_create(&p[i], NULL, mapper_exe, (void *)&fname[i]);
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_join(p[i], NULL);
    }
    
    for(int i = 0; i < num_partitions; i++) {
        qsort(table[i], pnum[i].index, sizeof(MR), cmpstringp);
    }
    
    // TODO: map partitions to reducers and pass that as an arg to the reducer
    part = (Part *)malloc(sizeof(Part) * num_reducers);
    if (part == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for(int i = 0; i < num_reducers; i++)
    {
        part[i].index = 0;
    }

    // Mapping partitions to their threads
    for (i = 0; i < num_partitions; i++) {
        pos = i % num_reducers;
        if(pos == 0) {
            part[num_reducers - 1].partition_no[part[num_reducers - 1].index] = i;
            part[num_reducers - 1].index++;
        }
        else {
            part[pos - 1].partition_no[part[pos - 1].index] = i;
            part[pos - 1].index++;
        }   
    }
    for(i = 0; i < num_reducers; i++) {
        if(part[i].index == 0) {
            part[i].partition_no[0] = 0;
        }
    }

    // Creating reducer threads
    for(i = 0; i < num_reducers; i++) {
        pthread_create(&q[i], NULL, reducer_exe, (void *)&part[i]);
    }

    for(i = 0; i < num_reducers; i++) {
        pthread_join(q[i], NULL);
    }
}


