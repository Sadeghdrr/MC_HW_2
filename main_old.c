#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

typedef struct {
    char *ptr;
    size_t length;
} StringMetadata;

typedef struct {
    size_t data_size;
    int threads;
    size_t tsize;
    const char *filename;
} ProgramArgs;

typedef struct {
    size_t start;             /* inclusive */
    size_t end;               /* exclusive */
    StringMetadata *meta;
    size_t *out_indices;      /* per‑line output */
    size_t *collision_count;  /* per-thread collision count */
} WorkerArgs;

int preprocess(const char *filename, size_t *lineCount, size_t *totalDataSize);
int allocate_memory(size_t lineCount, size_t totalDataSize, StringMetadata **metadata, char **data);
int read_data(const char *filename, size_t lineCount, StringMetadata *metadata, char *data);
int parse_arguments(int argc, char *argv[], ProgramArgs *args);
size_t parse_size(const char *str);
static inline uint64_t fnv1a64(const char *data, size_t len);
static void *worker(void *arg);
int run_app(const ProgramArgs *args);

/* ------------------------------------------------------------
 *  Shared table, counters, and synchronisation primitives
 * ----------------------------------------------------------*/
static char           **hashTable        = NULL;   /* NULL == empty, otherwise pointer to string */
static size_t           hashTableSize    = 0;
static pthread_mutex_t *bucketLocks = NULL;

size_t parse_size(const char *str) {
    size_t len = strlen(str);
    size_t multiplier = 1;

    if (str[len - 1] == 'K' || str[len - 1] == 'k') {
        multiplier = 1000;
        len--;
    } else if (str[len - 1] == 'M' || str[len - 1] == 'm') {
        multiplier = 1000000;
        len--;
    }

    char num[32];
    strncpy(num, str, len);
    num[len] = '\0';

    return strtoull(num, NULL, 10) * multiplier;
}

int parse_arguments(int argc, char *argv[], ProgramArgs *args) {
    if (argc != 9) {
        fprintf(stderr, "Usage: %s --data_size <size> --threads <num> --tsize <size> --input <file>\n", argv[0]);
        return 1;
    }

    for (int i = 1; i < argc; i += 2) {
        if (strcmp(argv[i], "--data_size") == 0) {
            args->data_size = parse_size(argv[i + 1]);
        } else if (strcmp(argv[i], "--threads") == 0) {
            args->threads = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "--tsize") == 0) {
            args->tsize = parse_size(argv[i + 1]);
        } else if (strcmp(argv[i], "--input") == 0) {
            args->filename = argv[i + 1];
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    return 0;
}

int preprocess(const char *filename, size_t *lineCount, size_t *totalDataSize) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 1;
    }

    char line[1024];
    *lineCount = 0;
    *totalDataSize = 0;

    while (fgets(line, sizeof(line), file)) {
        (*lineCount)++;
        *totalDataSize += strlen(line);
    }

    fclose(file);
    return 0;
}

int allocate_memory(size_t lineCount, size_t totalDataSize, StringMetadata **metadata, char **data) {
    *metadata = (StringMetadata *)malloc(lineCount * sizeof(StringMetadata));
    if (!*metadata) {
        perror("Memory allocation failed for metadata");
        return 1;
    }

    *data = (char *)malloc(totalDataSize + 1);
    if (!*data) {
        perror("Memory allocation failed for data");
        free(*metadata);
        return 1;
    }

    return 0;
}

int read_data(const char *filename, size_t lineCount, StringMetadata *metadata, char *data) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 1;
    }

    char line[1024];
    size_t totalDataAllocated = 0;
    size_t totalStrings = 0;

    while (fgets(line, sizeof(line), file) && totalStrings < lineCount) {
        size_t len = strlen(line);
        if (line[len - 1] == '\n') {
            line[len - 1] = '\0';
            len--;
        }

        memcpy(data + totalDataAllocated, line, len);
        data[totalDataAllocated + len] = '\0';

        metadata[totalStrings].ptr = data + totalDataAllocated;
        metadata[totalStrings].length = len;

        totalDataAllocated += len + 1;
        totalStrings++;
    }

    fclose(file);
    return 0;
}

/* Hash function: 64‑bit FNV‑1a with avalanche */
static inline uint64_t fnv1a64(const char *data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (unsigned char)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static void *worker(void *arg) {
    WorkerArgs *workerArg = (WorkerArgs *)arg;
    size_t thread_collisions = 0;
    
    for (size_t itemIndex = workerArg->start; itemIndex < workerArg->end; ++itemIndex) {
        const char  *currentString   = workerArg->meta[itemIndex].ptr;
        size_t       stringLength    = workerArg->meta[itemIndex].length;

        uint64_t hash    = fnv1a64(currentString, stringLength);
        size_t   tablePos = hash % hashTableSize;

        size_t local_collisions = 0;
        while (1) {
            /* First, try to read without lock */
            char *existingEntry = hashTable[tablePos];
            
            if (existingEntry == NULL) {
                /* Empty slot - try to claim it with lock */
                pthread_mutex_lock(&bucketLocks[tablePos]);
                /* Double-check after acquiring lock */
                if (hashTable[tablePos] == NULL) {
                    hashTable[tablePos] = workerArg->meta[itemIndex].ptr;
                    workerArg->out_indices[itemIndex] = tablePos;
                    thread_collisions += local_collisions;
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    break;  /* unique key successfully inserted */
                } else {
                    /* Someone else claimed it, unlock and retry */
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    existingEntry = hashTable[tablePos];
                }
            }
            
            /* Check if it's a duplicate (no lock needed for reading) */
            if (existingEntry != NULL && strlen(existingEntry) == stringLength &&
                memcmp(existingEntry, currentString, stringLength) == 0) {
                /* Duplicate – reuse slot; do NOT count collisions for duplicates */
                workerArg->out_indices[itemIndex] = tablePos;
                break;
            }

            /* Unique key but collided with different entry */
            ++local_collisions;
            tablePos = (tablePos + 1) % hashTableSize;  /* linear probing, step = 1 */
        }
    }
    
    /* Store thread's collision count */
    *(workerArg->collision_count) = thread_collisions;
    return NULL;
}

int run_app(const ProgramArgs *args) {
    size_t lineCount = 0, totalDataSize = 0;
    if (preprocess(args->filename, &lineCount, &totalDataSize) != 0) return 1;

    if (lineCount == 0) {
        printf("The file is empty.\n");
        return 1;
    }

    StringMetadata *metadata = NULL;
    char *data = NULL;

    if (allocate_memory(lineCount, totalDataSize, &metadata, &data) != 0) return 1;

    if (read_data(args->filename, lineCount, metadata, data) != 0) {
        free(metadata);
        free(data);
        return 1;
    } 

    /* ----------------  Begin mapping phase (timed)  ---------------- */
    hashTableSize  = args->tsize;
    hashTable  = (char **)calloc(hashTableSize, sizeof(char *));
    if (!hashTable) {
        perror("Unable to allocate hash table");
        free(metadata); free(data);
        return 1;
    }

    bucketLocks = (pthread_mutex_t *)malloc(hashTableSize * sizeof(pthread_mutex_t));
    if (!bucketLocks) {
        perror("Unable to allocate locks");
        free(hashTable);
        free(metadata); free(data);
        return 1;
    }
    for (size_t i = 0; i < hashTableSize; i++) {
        pthread_mutex_init(&bucketLocks[i], NULL);
    }

    size_t *indices = (size_t *)malloc(lineCount * sizeof(size_t));
    if (!indices) {
        perror("Unable to allocate indices array");
        free(bucketLocks); free(hashTable);
        free(metadata); free(data);
        return 1;
    }

    int nthreads = args->threads;
    if (nthreads < 1) nthreads = 1;
    if ((size_t)nthreads > lineCount) nthreads = (int)lineCount;

    pthread_t   *threads = (pthread_t *)malloc(nthreads * sizeof(pthread_t));
    WorkerArgs  *wargs   = (WorkerArgs  *)malloc(nthreads * sizeof(WorkerArgs));
    size_t      *thread_collisions = (size_t *)calloc(nthreads, sizeof(size_t));
    if (!threads || !wargs || !thread_collisions) {
        perror("Thread allocation failed");
        free(threads); free(wargs); free(thread_collisions);
        free(indices); free(bucketLocks); free(hashTable);
        free(metadata); free(data);
        return 1;
    }

    size_t chunk = (lineCount + nthreads - 1) / nthreads;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int t = 0; t < nthreads; ++t) {
        size_t start = t * chunk;
        size_t end   = start + chunk;
        if (end > lineCount) end = lineCount;

        wargs[t] = (WorkerArgs){ 
            .start = start, 
            .end = end, 
            .meta = metadata, 
            .out_indices = indices,
            .collision_count = &thread_collisions[t]
        };
        pthread_create(&threads[t], NULL, worker, &wargs[t]);
    }

    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    long long elapsed_ms = (t1.tv_sec  - t0.tv_sec) * 1000LL + (t1.tv_nsec - t0.tv_nsec) / 1000000LL;

    /* Sum up collision counts from all threads */
    size_t total_col = 0;
    for (int t = 0; t < nthreads; ++t) {
        total_col += thread_collisions[t];
    }

    /* ----------------  End mapping phase (timed)  ---------------- */

    /* Write results */
    char outfile[256];
    snprintf(outfile, sizeof(outfile),
             "results/Results_MCC_030402_401106039_%zu_%d_%zu.txt",
             args->data_size, args->threads, args->tsize);

    FILE *out = fopen(outfile, "w");
    if (!out) {
        perror("Cannot open results file");
    } else {
        fprintf(out, "ExecutionTime: %lld ms\n", elapsed_ms);
        fprintf(out, "NumberOfHandledCollision: %zu\n", total_col);
        for (size_t i = 0; i < lineCount; ++i) {
            fprintf(out, "%zu", indices[i]);
            if (i + 1 < lineCount) fputc(',', out);
        }
        fputc('\n', out);
        fclose(out);
    }

    /* Cleanup */
    for (size_t i = 0; i < hashTableSize; i++) {
        pthread_mutex_destroy(&bucketLocks[i]);
    }
    free(bucketLocks); free(threads); free(wargs);
    free(thread_collisions); free(indices); free(hashTable);

    free(metadata); free(data);
    return 0;
}

int main(int argc, char *argv[]) {
    ProgramArgs args;
    if (parse_arguments(argc, argv, &args) != 0) {
        return 1;
    }

    return run_app(&args);
}
