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
    char *filename[];
    int index;
}fileName;

typedef struct {
    int index;
    int MAX_SIZE;
    int get_index;
}Counter;

Map maps;
Reducer reduces;
Partitioner partitions;

MR **table;
fileName *fname;
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
    int bits = 0;
    int temp = num_partitions;
    unsigned long temp_key;
    while (temp >>= 1) bits++;
    temp_key = strtoul(key, NULL, 0);

    temp_key = temp_key >> (32 - bits);

    int bits = 0;
    int temp = num_partitions;
    unsigned long temp_key = 0;
    char key[] = "}}}}";
    
    
    while (temp >>= 1) bits++;
    // printf("Bits: %d\n",bits);
    
    int len = 0;
    // printf("Length: %d\n", strlen(key));
    if(strlen(key) >= 4)
        len = 4;
    else 
        len = strlen(key);
    
    // printf("Before: %lu \n", temp_key);
    if(len == 4) 
    {
        for(int i = 0; i < 4; i++)
        {
            // printf("key[%d] = %lu\n", i , (unsigned long) key[i]);
            temp_key = temp_key << 8;
            if(key[i]!= NULL)
                temp_key += (unsigned long) key[i];
            // printf("Inside: %lu \n", temp_key);
        }
    }
    else
    {
        int i = 0;
        int mov = (8 *(4 - len));
        temp_key = temp_key << mov;
        // printf("Temp: %lu \n", temp_key);
        while(len != 0)
        {
            temp_key = temp_key << 8;
            temp_key += (unsigned long) key[i];
            // printf("Inside: %lu \n", temp_key);
            len--;
            i++;
        }
    }
    temp_key = temp_key >> (32 - bits);
    
    // printf("After: %lu \n", temp_key);
    
    printf("After: %lu \n", temp_key);

    return temp_key;
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, 
            int num_reducers, Partitioner partition, int num_partitions) {
    int i, j;

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
    for(int i = 0; i < num_mappers; i++)
    {
        fname[i].index = 0;
    }

    // Mapping files to their threads
    // TODO: Sort files according to their size and then map them
    for (i = 1; i < argc; i++) {
        pos = i % num_mappers;
        if(pos == 0) {
            fname[num_mappers - 1].filename[fname[num_mappers - 1].index] = argv[i];
            fname[num_mappers - 1].index++;
        }
        else {
            fname[pos - 1].filename[fname[pos - 1].index] = argv[i];
            fname[pos - 1].index++;
        }   
    }
    for(i = 0; i < num_mappers; i++) {
        if(fname[i].index == 0) {
            fname[i].filename = NULL;
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
    
    // TODO: map partitions to reducers and pass that as an arg to 

    // Creating reducer threads
    for(i = 0; i < num_reducers; i++) {
        pthread_create(&q[i], NULL, reducer_exe, (void *)??);
    }

    for(i = 0; i < num_reducers; i++) {
        pthread_join(q[i], NULL);
    }
}

void *mapper_exe(void *arg) { 
    struct fileName *fname = (struct MR *)arg;
    for(int i = 0; i < fname.index; i++){
        map(fname.filename[i]);
    }
    return NULL;
}
void *reducer_exe(void *arg) {

}

char *get_next(char *key, int num_partitions) {
    unsigned long pno;
    int flag = 0;
    pno = (*partitions)(key, num_partitions);
    for(int i = pnum[pno].get_index; i <= pnum[pno]->index; i++)
    {
        if(strcmp(table[pno][i].key, key) == 0)
        {
            flag = 1;
            pnum[pno].get_index++;
            return table[pno][i]->value;
        }
    }
    //if key is not found
    if(flag == 0)
        return NULL;
}

