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

static int cmpstringp(const void *p1, const void *p2)
{
    const MR table1 =  * (MR *)p1;
    const MR table2 = * (MR *)p2;
    return strcmp(table1.key, table2.key);
}

char *get_next(char *key, int num_partitions) {
    int flag = 0;
    unsigned long pno;
    pno = (*partitions)(key, num_partitions);
    for(int i = pnum[pno].get_index; i <= pnum[pno].index; i++)
    {
        if(strcmp(table[pno][i].key, key) == 0)
        {
            flag = 1;
            pnum[pno].get_index++;
            return table[pno][i].value;
        }
    }
    if(flag == 0)
        return NULL;
    return NULL;
}

void *mapper_exe(void *arg) { 
    fileName *fname = (fileName *)arg;
    for(int i = 0; i < fname->index; i++) {
        maps(fname->name[i]);
    }
    return NULL;
}
void *reducer_exe(void *arg) {
    Part *part = (Part *)arg;
    unsigned long partition_num;
    char *distinctKey = NULL, *prevKey = NULL;
    for(int i = 0; i < part->index; i++) {
        partition_num = part->partition_no[i];
        if(pnum[partition_num].index != 0) {
            strncpy(prevKey, table[partition_num][0].key, strlen(table[partition_num][0].key));
            for(int j = 0; j < pnum[partition_num].index; j++) {
                strncpy(distinctKey, table[partition_num][j].key, strlen(table[partition_num][0].key));
                if(strcmp(distinctKey, prevKey) != 0 || j == 0) {
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
    // TODO: ensures  that keys are in a sorted order across the partitions 
    // (i.e., keys are not hashed into random partitions as in the default partition function)

    // int bits = 0;
    // int temp = num_partitions;
    // unsigned long temp_key = 0;
    
    // while (temp >>= 1) bits++;
    // // printf("Bits: %d\n",bits);
    
    // int len = 0;
    // // printf("Length: %d\n", strlen(key));
    // if(strlen(key) >= 4)
    //     len = 4;
    // else 
    //     len = strlen(key);
    
    // // printf("Before: %lu \n", temp_key);
    // if(len == 4) 
    // {
    //     for(int i = 0; i < 4; i++)
    //     {
    //         // printf("key[%d] = %lu\n", i , (unsigned long) key[i]);
    //         temp_key = temp_key << 8;
    //         if(key[i]!= NULL)
    //             temp_key += (unsigned long) key[i];
    //         // printf("Inside: %lu \n", temp_key);
    //     }
    // }
    // else
    // {
    //     int i = 0;
    //     int mov = (8 *(4 - len));
    //     temp_key = temp_key << mov;
    //     // printf("Temp: %lu \n", temp_key);
    //     while(len != 0)
    //     {
    //         temp_key = temp_key << 8;
    //         temp_key += (unsigned long) key[i];
    //         // printf("Inside: %lu \n", temp_key);
    //         len--;
    //         i++;
    //     }
    // }
    // temp_key = temp_key >> (32 - bits);
    
    // // printf("After: %lu \n", temp_key);
    
    // printf("After: %lu \n", temp_key);

    // return temp_key;
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
    
    pthread_t p[num_mappers];
    pthread_t q[num_reducers];

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

    // Creating mapper threads
    for(i = 0; i < num_mappers; i++) {
        pthread_create(&p[i], NULL, mapper_exe, (void *)&fname[i]);
    }

    for(i = 0; i < num_mappers; i++) {
        pthread_join(p[i], NULL);
    }
    printf("Created mapper threads\n");
    
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
    printf("Done mapping 2\n");

    // Creating reducer threads
    for(i = 0; i < num_reducers; i++) {
        pthread_create(&q[i], NULL, reducer_exe, (void *)&part[i]);
    }

    for(i = 0; i < num_reducers; i++) {
        pthread_join(q[i], NULL);
    }
    printf("Created reducer threads\n");

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
