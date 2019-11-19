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
    unsigned long *partition_no;
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

char *get_next(char *key, int part_num) {
    int flag = 0;
    for(int i = pnum[part_num].get_index; i < pnum[part_num].index; i++)
    {
        if(strcmp(table[part_num][i].key, key) == 0)
        {
            flag = 1;
            pnum[part_num].get_index++;
            return table[part_num][i].value;
        }
    }
    if(flag == 0)
        return NULL;
    return NULL;
}

void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 

    // printf("In MR_Emit()\n");
    unsigned long pno;
    pno = (*partitions)(key, part_no);
    // printf("pno = %lu\n", pno);
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

    //strncpy(table[pno][pnum[pno].index].key, key, strlen(key));
    //strncpy(table[pno][pnum[pno].index].value, value, strlen(value));
    strcpy(table[pno][pnum[pno].index].key, key);
    strcpy(table[pno][pnum[pno].index].value, value);
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
    int i, j;

    maps = map;
    reducers = reduce;
    partitions = partition;
    part_no = num_partitions;

    //printf("Starting MR_Run()\n");

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
    //printf("Created table\n");

    // TODO: Need to do some sort of scheduling to map the files to the mappers
    // and maybe pass those as parameters to mappers_exe

    int pos;
    fname = (fileName *)malloc(sizeof(fileName) * num_mappers);
    if (fname == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for( i = 0; i < num_mappers; i++)
    {
        fname[i].index = 0;
    }

    // Mapping files to their threads
    // TODO: Sort files according to their size and then map them
    for (i = 1; i < argc; i++) {
        pos = i % num_mappers;
        if(pos == 0) {
            fname[num_mappers - 1].name[fname[num_mappers - 1].index] = (char *)malloc(sizeof(char)*strlen(argv[i]));
            fname[num_mappers - 1].name[fname[num_mappers - 1].index] = argv[i];
            fname[num_mappers - 1].index++;
        }
        else {
            fname[pos - 1].name[fname[pos - 1].index] = (char *)malloc(sizeof(char)*strlen(argv[i]));
            fname[pos - 1].name[fname[pos - 1].index] = argv[i];
            fname[pos - 1].index++;
        }   
    }
    // TODO: num_files < num_mappers
    if(argc - 1 < num_mappers) {
        for(i = 0; i < num_mappers; i++) {
            if(fname[i].index == 0) {
                strcpy(fname[i].name[0], "\0");
            }
        }
    }
    //printf("Done mapping 1\n");

    // printf("::DEBUG:: Printing mapped filenames\n");
    // printf("#files = %d num_mappers = %d\n", argc - 1, num_mappers);
    // for(i = 0; i < num_mappers; i++) {
    //     for( j = 0; j < fname[i].index; j++) {
    //         printf("mapper = %d filename = %s index = %d\n", i, fname[i].name[j], j);
    //     }
    // }

    for (i = 0; i < num_mappers; i++) {
        if(fname[i].index != 0) {
            for( j = 0; j < fname[i].index; j++) {
                map(fname[i].name[j]);
            }
        }
    }

    for(i = 0; i < num_partitions; i++) {
        qsort(table[i], pnum[i].index, sizeof(MR), cmpstringp);
    }
    
    // printf("DEBUG:: Hash Table\n");
    // for(i = 0; i < num_partitions; i++) {
    //     for(j = 0; j < pnum[i].index; j++) {
    //         printf("key = %s value = %s pno = %d\n", table[i][j].key, table[i][j].value, i);
    //     }
    //     printf("\n");
    // }

    // TODO: map partitions to reducers and pass that as an arg to the reducer
    part = (Part *)malloc(sizeof(Part) * num_reducers);
    if (part == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for( i = 0; i < num_reducers; i++)
    {
        part[i].index = 0;
        part[i].partition_no = (unsigned long *)malloc(sizeof(unsigned long)*num_partitions);
    }

    // Mapping partitions to their threads
    for (unsigned long i = 0; i < num_partitions; i++) {
        if(i == 0) {
            part[0].partition_no[part[0].index] = i;
            //printf("1 part[0].partition_no[%d] = %ld %ld\n", part[0].index, part[0].partition_no[part[0].index], i);
            part[0].index++;
        }
        else {
            pos = i % num_reducers;
            if(pos == 0) {
                part[num_reducers - 1].partition_no[part[num_reducers - 1].index] = i;
                //printf("2 part[%d].partition_no[%d] = %ld\n", num_reducers - 1, part[num_reducers - 1].index, part[num_reducers - 1].partition_no[part[num_reducers - 1].index]);
                part[num_reducers - 1].index++;
            }
            else {
                part[pos].partition_no[part[pos].index] = i;
                //printf("3 part[%d].partition_no[%d] = %ld\n", pos, part[pos].index, part[pos].partition_no[part[pos].index]);
                part[pos].index++;
            }   
        }
    }
    // TODO: num_partitions < num_reducers
    // for(i = 0; i < num_reducers; i++) {
    //     if(part[i].index == 0) {
    //         part[i].partition_no[0] = 0;
    //     }
    // }
    //printf("Done mapping 2\n");

    // printf("::DEBUG:: Printing mapped partitions\n");
    // fflush(stdout);
    // printf("num_reducers = %d num_partitions = %d\n", num_reducers, num_partitions);
    // for(i = 0; i < num_reducers; i++) {
    //     for(j = 0; j < part[i].index; j++) {
    //         printf("reducer = %d partition# = %ld index = %d\n", i, part[i].partition_no[j], j);
    //     }
    // }

    unsigned long partition_num;
    char *distinctKey, *prevKey;
    prevKey = malloc(sizeof(char)*100);
    distinctKey = malloc(sizeof(char)*100);
    //printf("Reducer starts here\n");
    for (i = 0; i < num_reducers; i++) {
        //printf("num_reducers = %d, i = %d\n", num_reducers, i);
        for(int j = 0; j < part[i].index; j++) {
            //printf("part[i].index = %d, j = %d\n", part[i].index, j);
            partition_num = part[i].partition_no[j];
            //printf("partition_num = %ld\n", partition_num);
            if(pnum[partition_num].index != 0) {
                //printf("pnum[%ld].index = %d\n", partition_num, pnum[partition_num].index);
                 strncpy(prevKey, table[partition_num][0].key, strlen(table[partition_num][0].key));
                 //printf("prevKey = %s\n", prevKey);
                // //strcpy(prevKey, table[partition_num][0].key);
                 for(int k = 0; k < pnum[partition_num].index; k++) {
                     //strncpy(distinctKey, table[partition_num][k].key, strlen(table[partition_num][k].key));
                     strcpy(distinctKey, table[partition_num][k].key);
                     //printf("distinctKey = %s\n", distinctKey);
                //     //strcpy(distinctKey, table[partition_num][j].key);
                     if(strcmp(distinctKey, prevKey) != 0 || k == 0) {
                         reduce(distinctKey, get_next, partition_num);
                     }
                // strncpy(prevKey, distinctKey, strlen(distinctKey));
                strcpy(prevKey, distinctKey);
                 }
            }
        }
    }

    // printf("::DEBUG:: Hash Table\n");
    // for(i = 0; i < num_partitions; i++) {
    //     for( j = 0; j < pnum[i].index; j++) {
    //         printf("key = %s value = %s pno = %d\n", table[i][j].key, table[i][j].value, i);
    //     }
    //     printf("\n");
    // }

    // Free Stuff

    // TODO: Free key and value of table

    // free(fname);
    // fname = NULL;

    // for(i = 0; i < num_partitions; i++) {
    //     free(table[i]);
    //     table[i] = NULL;
    // }
    // free(table);
    // table = NULL;

    // free(pnum);
    // pnum = NULL;

    // printf("Freed stuff\n");
}

