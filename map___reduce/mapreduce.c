#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <time.h>

pthread_mutex_t *lock;

// Declaring data structure
typedef struct MR{
  char *key;
  char *value;
}MR;

typedef struct {
    int index;
    char **name;
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
    for(int i = pnum[part_num].get_index; i < pnum[part_num].index; i++)
    {
        if(strcmp(table[part_num][i].key, key) == 0)
        {
            pnum[part_num].get_index++;
            return table[part_num][i].value;
        }
        else
        {
            break;
        }
    }
    return NULL;
}

void MR_Emit(char *key, char *value) {
    // TODO: Take key and value from different mappers and store them in a partition
    // such that later reducers can access them, given constraints. 
    unsigned long pno;
    pno = (*partitions)(key, part_no);
    pthread_mutex_lock(&lock[pno]);
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
    strcpy(table[pno][pnum[pno].index].key, key);
    strcpy(table[pno][pnum[pno].index].value, value);
    pnum[pno].index++;
    pthread_mutex_unlock(&lock[pno]);
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
    int bits = 0;
    int temp = num_partitions;
    unsigned long temp_key = 0;
    while (temp >>= 1) bits++;
    
    temp_key = (unsigned long) atoi(key);
    
    double lim =  (2147483647)/(double)num_partitions;

    int flag = 0;
    for(int i = 0; i < num_partitions; i++)
    {
        // printf("i= %lf\n", i);
        if(i * lim <= temp_key && temp_key < (i + 1)*lim) {
            temp_key = i;
            flag = 1;
            break;
        }
    }
    if (flag == 0)
    {
        temp_key = num_partitions - 1;
    }

    return temp_key;

}

// Adding a mapper wrapper

void *mapper_exe(void *arg) { 
    fileName *fname = (fileName *)arg;
    for(int i = 0; i < fname->index; i++) {
        
        maps(fname->name[i]);
        
    }
    return NULL;
}

// Adding a reducer wrapper

void *reducer_exe(void *arg) {
    Part *part = (Part *)arg;
    unsigned long partition_num;
    char *distinctKey, *prevKey;
    distinctKey = malloc(sizeof(char)*100);
    for(int j = 0; j < part->index; j++) {
        partition_num = part->partition_no[j];
        // Sort here to improve speed!!
        qsort(table[partition_num], pnum[partition_num].index, sizeof(MR), cmpstringp);
        if(pnum[partition_num].index != 0) {
            for(int k = 0; k < pnum[partition_num].index;) {
                strcpy(distinctKey, table[partition_num][k].key);   
                reducers(distinctKey, get_next, partition_num);
                k = pnum[partition_num].get_index;
            }
        }
    }
    free(distinctKey);
    distinctKey = NULL;
    return NULL;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    int i;

    maps = map;
    reducers = reduce;
    partitions = partition;
    part_no = num_partitions;

    lock = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * num_partitions);
    for(int i = 0; i < num_partitions; i++) {
        if(pthread_mutex_init(&lock[i] , NULL) != 0) {
            printf("Failed to init lock\n");
            exit(1);
        }
    }

    pthread_t p[num_mappers];
    pthread_t q[num_reducers];

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

    // TODO: Need to do some sort of scheduling to map the files to the mappers
    // and maybe pass those as parameters to mappers_exe

    int pos;
    int mthread_size;
    if(argc - 1 >= num_mappers) {
        mthread_size = num_mappers;
    }
    else {
        mthread_size = argc - 1;
    }
    fname = (fileName *)malloc(sizeof(fileName) * mthread_size);
    if (fname == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for( i = 0; i < mthread_size; i++)
    {
        fname[i].index = 0;
        fname[i].name = (char**)malloc(sizeof(char*)*10);
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

    for(i = 0; i < mthread_size; i++) {
        if(fname[i].index !=0) // Create threads only if there are files assigned to it.
            pthread_create(&p[i], NULL, mapper_exe, (void *)&fname[i]);
    }

    for(i = 0; i < mthread_size; i++) {
        if(fname[i].index !=0)
            pthread_join(p[i], NULL);
    }

    // TODO: map partitions to reducers and pass that as an arg to the reducer
    int rthread_size;
    if(num_partitions >= num_reducers) {
        rthread_size = num_reducers;
    }
    else {
        rthread_size = num_partitions;
    }
    part = (Part *)malloc(sizeof(Part) * rthread_size);
    if (part == NULL)
    {
        printf("Memory Allocation Failed\n");
        exit(1);
    }
    for( i = 0; i < rthread_size; i++)
    {
        part[i].index = 0;
        part[i].partition_no = (unsigned long *)malloc(sizeof(unsigned long)*num_partitions);
    }

    // Mapping partitions to their threads
    for (unsigned long i = 0; i < num_partitions; i++) {
        if(i == 0) {
            part[0].partition_no[part[0].index] = i;
            part[0].index++;
        }
        else {
            pos = i % num_reducers;
            if(pos == 0) {
                part[num_reducers - 1].partition_no[part[num_reducers - 1].index] = i;
                part[num_reducers - 1].index++;
            }
            else {
                part[pos].partition_no[part[pos].index] = i;
                part[pos].index++;
            }   
        }
    }

    for(i = 0; i < rthread_size; i++) {
            pthread_create(&q[i], NULL, reducer_exe, (void *)&part[i]);
    }

    for(i = 0; i < rthread_size; i++) {
            pthread_join(q[i], NULL);
    }

    // For MR_emit

    for(int i = 0; i < num_partitions; i++)
    {
        for(int j = 0; j < pnum[i].index; j++)
        {
            free(table[i][j].key);
            table[i][j].key = NULL;
            free(table[i][j].value);
            table[i][j].value = NULL;
        }
        free(table[i]);
        table[i] = NULL;
    }
    free(table);
    table = NULL;

    // MR_run

    free(lock);
    lock = NULL;

    free(pnum);
    pnum = NULL;

    for(int i = 0; i < mthread_size; i++)
    {
        for(int j = 0; j < 10; j++)
        {
            fname[i].name[j] = NULL;
            free(fname[i].name[j]);
            
        }
        free(fname[i].name);
        fname[i].name = NULL;
    }
    free(fname);
    fname = NULL;
        
    for(int i = 0; i < rthread_size; i++)
    {
        free(part[i].partition_no);
        part[i].partition_no = NULL;
    }
    free(part);
    part = NULL;
}

