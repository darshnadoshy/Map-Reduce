#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>

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

void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 

    printf("In MR_Emit()\n");
    unsigned long pno;
    pno = (*partitions)(key, part_no);
    printf("pno = %lu\n", pno);
    if(pnum[pno].index == 0)
    {
        table[pno] = (MR *)malloc(sizeof(MR) * pnum[pno].MAX_SIZE);
        if (table[pno] == NULL)
        {
            printf("Memory Allocation Failed!\n");
            exit(1);
        }
        
    }
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

    table[pno][pnum[pno].index].key = (char *)malloc(sizeof(char)*strlen(key));
    if (table[pno][pnum[pno].index].key == NULL)
    {
        printf("Memory Allocation Failed!\n");
        exit(1);
    }
    table[pno][pnum[pno].index].value = (char *)malloc(sizeof(char)*strlen(value));
    if (table[pno][pnum[pno].index].value == NULL)
    {
        printf("Memory Allocation Failed!\n");
        exit(1);
    }

    strncpy(table[pno][pnum[pno].index].key, key, strlen(key));
    strncpy(table[pno][pnum[pno].index].value, value, strlen(value));
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
    return 0;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    int i;

    maps = map;
    reducers = reduce;
    partitions = partition;
    part_no = num_partitions;

    printf("Starting MR_Run()\n");

    // Set the initial values of the partition counters
    pnum = (Counter *)malloc(sizeof(Counter) * num_partitions);
    if (pnum == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for(int i = 0; i < num_partitions; i++)
    {
        pnum[i].index = 0;
        pnum[i].MAX_SIZE = 5;
        pnum[i].get_index = 0;
    }

    table = (MR **)malloc(sizeof(MR *) * num_partitions);
    if (table == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    printf("Created table\n");

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
    printf("Done mapping 1\n");

    for (i = 0; i < num_mappers; i++) {
        for(int j = 0; j < fname[i].index; j++) {
            map(fname[i].name[j]);
        }
    }

    for(i = 0; i < num_partitions; i++) {
        for( int j = 0; j < pnum[i].index; j++) {
            printf("key = %s value = %s index = %d\n", table[i][j].key, table[i][j].value, pnum[i].index);
        }
        printf("\n");
    }
    
    // // TODO: map partitions to reducers and pass that as an arg to the reducer
    // part = (Part *)malloc(sizeof(Part) * num_reducers);
    // if (part == NULL)
    // {
    //     printf("Memory Allocation Failed\n");
    //     exit(1);
    // }
    // for(int i = 0; i < num_reducers; i++)
    // {
    //     part[i].index = 0;
    // }

    // // Mapping partitions to their threads
    // for (i = 0; i < num_partitions; i++) {
    //     pos = i % num_reducers;
    //     if(pos == 0) {
    //         part[num_reducers - 1].partition_no[part[num_reducers - 1].index] = i;
    //         part[num_reducers - 1].index++;
    //     }
    //     else {
    //         part[pos - 1].partition_no[part[pos - 1].index] = i;
    //         part[pos - 1].index++;
    //     }   
    // }
    // for(i = 0; i < num_reducers; i++) {
    //     if(part[i].index == 0) {
    //         part[i].partition_no[0] = 0;
    //     }
    // }
    // printf("Done mapping 2\n");

    // Free Stuff
    free(fname);
    fname = NULL;

    for(i = 0; i < num_partitions; i++) {
        free(table[i]);
        table[i] = NULL;
    }
    free(table);
    table = NULL;

    free(pnum);
    pnum = NULL;

    printf("Freed stuff\n");
}

