#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>

#define MAX_OPERATIONS 16

typedef struct {
    char *action[MAX_OPERATIONS];
    char *input_files[MAX_OPERATIONS];
    int num_operations;
    size_t data_size;
    int threads;
    size_t tsize;
} ProgramArgs;

typedef struct {
    char *ptr;
    size_t length;
} StringMetadata;

// Hash table entry with tombstone support
typedef struct {
    StringMetadata *key;  // NULL -> empty, non-NULL -> valid key
    uint8_t tombstone;    // 1 -> deleted (tombstone), 0 -> valid/empty
} HashEntry;

// Worker thread arguments
typedef struct {
    size_t start;             // inclusive
    size_t end;               // exclusive
    StringMetadata *meta;
    size_t *out_indices;      // per-line output indices
    char *out_results;        // per-line output results (T/F)
    size_t *collision_count;  // per-thread collision count
    const char *action;       // "insert" or "delete"
} WorkerArgs;

// Global hash table and synchronization
static HashEntry *g_table = NULL;
static size_t g_table_size = 0;
static pthread_mutex_t *bucketLocks = NULL;

// Forward declarations
static inline uint64_t fnv1a64(const char *data, size_t len);
static void *worker(void *arg);
static int ensure_table_and_locks(size_t size);
static void cleanup_table_and_locks(void);
static void write_operation_results(const ProgramArgs *args, int op_index, const char *action,
                                   size_t lineCount, StringMetadata *metadata, 
                                   size_t *indices, char *results, 
                                   long long elapsed_ms, size_t total_collisions);
static int execute_hash_operation(const ProgramArgs *args, int op_index, const char *action,
                                 size_t lineCount, StringMetadata *metadata);

// Function to parse size with K/M suffix
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

// Function to deparse size to string with K/M suffix
char* deparse_size(size_t size, char *buffer, size_t buffer_size) {
    if (size % 1000000 == 0) {
        snprintf(buffer, buffer_size, "%zuM", size / 1000000);
    } else if (size % 1000 == 0) {
        snprintf(buffer, buffer_size, "%zuK", size / 1000);
    } else {
        snprintf(buffer, buffer_size, "%zu", size);
    }
    return buffer;
}

// Function to parse command-line arguments
int parse_arguments(int argc, char *argv[], ProgramArgs *args) {
    args->num_operations = 0;
    int found_data_size = 0, found_threads = 0, found_tsize = 0;
    int found_flow = 0, found_input = 0;

    for (int i = 1; i < argc;) {
        if (strcmp(argv[i], "--data_size") == 0 && i + 1 < argc) {
            args->data_size = parse_size(argv[++i]);
            found_data_size = 1;
            i++;
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            args->threads = atoi(argv[++i]);
            found_threads = 1;
            i++;
        } else if (strcmp(argv[i], "--tsize") == 0 && i + 1 < argc) {
            args->tsize = parse_size(argv[++i]);
            found_tsize = 1;
            i++;
        } else if (strcmp(argv[i], "--flow") == 0) {
            found_flow = 1;
            i++;
            while (i < argc && argv[i][0] != '-') {
                if (args->num_operations >= MAX_OPERATIONS) {
                    fprintf(stderr, "Error: Too many flow actions (max %d)\n", MAX_OPERATIONS);
                    return 1;
                }
                args->action[args->num_operations++] = argv[i++];
            }
            if (args->num_operations == 0) {
                fprintf(stderr, "Error: --flow must be followed by at least one action\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--input") == 0) {
            found_input = 1;
            i++;
            int count = 0;
            while (i < argc && argv[i][0] != '-') {
                if (count >= MAX_OPERATIONS) {
                    fprintf(stderr, "Error: Too many input files (max %d)\n", MAX_OPERATIONS);
                    return 1;
                }
                args->input_files[count++] = argv[i++];
            }
            if (count != args->num_operations) {
                fprintf(stderr, "Error: Number of input files (%d) must match number of actions (%d)\n", count, args->num_operations);
                return 1;
            }
        } else {
            fprintf(stderr, "Error: Unknown or misplaced argument '%s'\n", argv[i]);
            return 1;
        }
    }

    if (!found_data_size || !found_threads || !found_tsize || !found_flow || !found_input) {
        fprintf(stderr, "Error: Missing one or more required arguments\n");
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  --data_size <size>\n");
        fprintf(stderr, "  --threads <num>\n");
        fprintf(stderr, "  --tsize <size>\n");
        fprintf(stderr, "  --flow <action1> <action2> ...\n");
        fprintf(stderr, "  --input <file1> <file2> ...\n");
        return 1;
    }

    return 0;
}

// Hash function: 64-bit FNV-1a
static inline uint64_t fnv1a64(const char *data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (unsigned char)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

// Ensure global hash table and locks are allocated
static int ensure_table_and_locks(size_t size) {
    if (g_table) return 0;

    g_table_size = size;
    g_table = (HashEntry *)calloc(g_table_size, sizeof(HashEntry));
    if (!g_table) {
        perror("Unable to allocate hash table");
        return -1;
    }

    bucketLocks = (pthread_mutex_t *)malloc(g_table_size * sizeof(pthread_mutex_t));
    if (!bucketLocks) {
        perror("Unable to allocate locks");
        free(g_table);
        g_table = NULL;
        return -1;
    }

    for (size_t i = 0; i < g_table_size; i++) {
        pthread_mutex_init(&bucketLocks[i], NULL);
    }

    return 0;
}

// Cleanup global resources
static void cleanup_table_and_locks(void) {
    if (bucketLocks) {
        for (size_t i = 0; i < g_table_size; i++) {
            pthread_mutex_destroy(&bucketLocks[i]);
        }
        free(bucketLocks);
        bucketLocks = NULL;
    }

    if (g_table) {
        for (size_t i = 0; i < g_table_size; i++) {
            if (g_table[i].key) {
                free(g_table[i].key->ptr);
                free(g_table[i].key);
            }
        }
        free(g_table);
        g_table = NULL;
    }
}

// Worker thread function
static void *worker(void *arg) {
    WorkerArgs *workerArg = (WorkerArgs *)arg;
    size_t thread_collisions = 0;

    for (size_t itemIndex = workerArg->start; itemIndex < workerArg->end; ++itemIndex) {
        const char *currentString = workerArg->meta[itemIndex].ptr;
        size_t stringLength = workerArg->meta[itemIndex].length;

        uint64_t hash = fnv1a64(currentString, stringLength);
        size_t tablePos = hash % g_table_size;
        size_t first_tombstone = (size_t)(-1);
        size_t local_collisions = 0;

        if (strcmp(workerArg->action, "insert") == 0) {
            // Insert operation
            while (1) {
                pthread_mutex_lock(&bucketLocks[tablePos]);
                HashEntry *e = &g_table[tablePos];

                if (e->key == NULL) {
                    if (e->tombstone) {
                        // Found tombstone, remember first one
                        if (first_tombstone == (size_t)(-1)) {
                            first_tombstone = tablePos;
                        }
                        pthread_mutex_unlock(&bucketLocks[tablePos]);
                    } else {
                        // Empty slot - use either first tombstone or this slot
                        size_t target = (first_tombstone == (size_t)(-1)) ? tablePos : first_tombstone;
                        if (target != tablePos) {
                            pthread_mutex_unlock(&bucketLocks[tablePos]);
                            pthread_mutex_lock(&bucketLocks[target]);
                        }

                        g_table[target].key = malloc(sizeof(StringMetadata));
                        if (g_table[target].key) {
                            g_table[target].key->ptr = malloc(stringLength + 1);
                            if (g_table[target].key->ptr) {
                                memcpy(g_table[target].key->ptr, currentString, stringLength);
                                g_table[target].key->ptr[stringLength] = '\0';
                                g_table[target].key->length = stringLength;
                            } else {
                                free(g_table[target].key);
                                g_table[target].key = NULL;
                            }
                        }
                        g_table[target].tombstone = 0;
                        workerArg->out_indices[itemIndex] = target;
                        workerArg->out_results[itemIndex] = 'F'; // did not exist
                        thread_collisions += local_collisions;
                        pthread_mutex_unlock(&bucketLocks[target]);
                        break;
                    }
                } else if (e->key->length == stringLength && 
                          memcmp(e->key->ptr, currentString, stringLength) == 0) {
                    // Key already exists
                    workerArg->out_indices[itemIndex] = tablePos;
                    workerArg->out_results[itemIndex] = 'T'; // existed
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    break;
                } else {
                    // Different key, continue probing
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    // Don't count collisions after first tombstone found
                    if (first_tombstone == (size_t)(-1)) local_collisions++;
                }
                tablePos = (tablePos + 1) % g_table_size;
            }
        } else if (strcmp(workerArg->action, "delete") == 0) {
            // Delete operation
            while (1) {
                pthread_mutex_lock(&bucketLocks[tablePos]);
                HashEntry *e = &g_table[tablePos];

                if (e->key == NULL) {
                    if (e->tombstone) {
                        // Keep probing past tombstones
                        pthread_mutex_unlock(&bucketLocks[tablePos]);
                        local_collisions++;
                    } else {
                        // Empty bucket - key not found
                        workerArg->out_results[itemIndex] = 'F'; // not found
                        pthread_mutex_unlock(&bucketLocks[tablePos]);
                        break;
                    }
                } else if (e->key->length == stringLength && 
                          memcmp(e->key->ptr, currentString, stringLength) == 0) {
                    // Found key - delete it
                    free(e->key->ptr);
                    free(e->key);
                    e->key = NULL;
                    e->tombstone = 1;
                    workerArg->out_indices[itemIndex] = tablePos;
                    workerArg->out_results[itemIndex] = 'T'; // found and deleted
                    thread_collisions += local_collisions;
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    break;
                } else {
                    // Different key, continue probing
                    pthread_mutex_unlock(&bucketLocks[tablePos]);
                    local_collisions++;
                }

                tablePos = (tablePos + 1) % g_table_size;
            }
        }
    }

    *(workerArg->collision_count) = thread_collisions;
    return NULL;
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

// Helper function to write operation results to file
static void write_operation_results(const ProgramArgs *args, int op_index, const char *action,
                                   size_t lineCount, StringMetadata *metadata, 
                                   size_t *indices, char *results, 
                                   long long elapsed_ms, size_t total_collisions) {
    // Build flow string for filename
    char flow[256] = "";
    for (int j = 0; j < args->num_operations; ++j) {
        strcat(flow, args->action[j]);
        if (j + 1 < args->num_operations) strcat(flow, "_");
    }

    // Write results to file
    char outfile[512];
    char data_size_str[32], tsize_str[32];
    deparse_size(args->data_size, data_size_str, sizeof(data_size_str));
    deparse_size(args->tsize, tsize_str, sizeof(tsize_str));
    snprintf(outfile, sizeof(outfile),
             "results/Results_HW2_MCC_030402_401106039_%s_%d_%s_%s.txt",
             data_size_str, args->threads, tsize_str, flow);

    FILE *out = fopen(outfile, (op_index == 0) ? "w" : "a");
    if (!out) {
        perror("Cannot open results file");
        return;
    }

    fprintf(out, "Actions: %s\n", action);
    fprintf(out, "ExecutionTime: %lld ms\n", elapsed_ms);
    fprintf(out, "NumberOfHandledCollision: %zu\n", total_collisions);

    if (strcmp(action, "insert") == 0) {
        for (size_t j = 0; j < lineCount; ++j) {
            fprintf(out, "%s:%zu:%c", metadata[j].ptr, indices[j], results[j]);
            if (j + 1 < lineCount) fprintf(out, ", ");
        }
    } else if (strcmp(action, "delete") == 0) {
        // For delete: only output successful deletions
        int first = 1;
        for (size_t j = 0; j < lineCount; ++j) {
            if (results[j] == 'T') { // Successfully deleted
                if (!first) fprintf(out, ", ");
                fprintf(out, "%s:%zu:%c", metadata[j].ptr, indices[j], results[j]);
                first = 0;
            } else {
                // Failed deletion: only output Data:F
                if (!first) fprintf(out, ", ");
                fprintf(out, "%s:%c", metadata[j].ptr, results[j]);
                first = 0;
            }
        }
    }

    fprintf(out, "\n");
    fclose(out);
}

// Helper function to execute hash operation (insert or delete)
static int execute_hash_operation(const ProgramArgs *args, int op_index, const char *action,
                                 size_t lineCount, StringMetadata *metadata) {
    printf("%s %zu records...\n", 
           (strcmp(action, "insert") == 0) ? "Inserting" : "Deleting", lineCount);

    // Ensure hash table and locks are initialized
    if (ensure_table_and_locks(args->tsize) != 0) {
        return 1;
    }

    // Allocate arrays for results
    size_t *indices = (size_t *)malloc(lineCount * sizeof(size_t));
    char *results = (char *)malloc(lineCount * sizeof(char));
    if (!indices || !results) {
        perror("Memory allocation failed for results");
        free(indices);
        free(results);
        return 1;
    }

    // Setup threading
    int nthreads = args->threads;
    if (nthreads < 1) nthreads = 1;
    if ((size_t)nthreads > lineCount) nthreads = (int)lineCount;

    pthread_t *threads = (pthread_t *)malloc(nthreads * sizeof(pthread_t));
    WorkerArgs *wargs = (WorkerArgs *)malloc(nthreads * sizeof(WorkerArgs));
    size_t *thread_collisions = (size_t *)calloc(nthreads, sizeof(size_t));

    if (!threads || !wargs || !thread_collisions) {
        perror("Thread allocation failed");
        free(threads);
        free(wargs);
        free(thread_collisions);
        free(indices);
        free(results);
        return 1;
    }

    size_t chunk = (lineCount + nthreads - 1) / nthreads;

    // Start timing
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    // Create and start worker threads
    for (int t = 0; t < nthreads; ++t) {
        size_t start = t * chunk;
        size_t end = start + chunk;
        if (end > lineCount) end = lineCount;

        wargs[t] = (WorkerArgs){
            .start = start,
            .end = end,
            .meta = metadata,
            .out_indices = indices,
            .out_results = results,
            .collision_count = &thread_collisions[t],
            .action = action
        };
        pthread_create(&threads[t], NULL, worker, &wargs[t]);
    }

    // Wait for all threads to complete
    for (int t = 0; t < nthreads; ++t) {
        pthread_join(threads[t], NULL);
    }

    // End timing
    clock_gettime(CLOCK_MONOTONIC, &t1);
    long long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000LL + (t1.tv_nsec - t0.tv_nsec) / 1000000LL;

    // Sum up collision counts
    size_t total_collisions = 0;
    for (int t = 0; t < nthreads; ++t) {
        total_collisions += thread_collisions[t];
    }

    // Write results to file
    write_operation_results(args, op_index, action, lineCount, metadata, 
                           indices, results, elapsed_ms, total_collisions);

    // Cleanup thread resources
    free(threads);
    free(wargs);
    free(thread_collisions);
    free(indices);
    free(results);

    return 0;
}

int run_app(const ProgramArgs *args) {
    for (int i = 0; i < args->num_operations; ++i) {
        printf(">>> Action: %s on file: %s\n", args->action[i], args->input_files[i]);

        size_t lineCount = 0, totalDataSize = 0;
        if (preprocess(args->input_files[i], &lineCount, &totalDataSize) != 0) return 1;

        if (lineCount == 0) {
            printf("File %s is empty.\n", args->input_files[i]);
            continue;
        }

        StringMetadata *metadata = NULL;
        char *data = NULL;

        if (allocate_memory(lineCount, totalDataSize, &metadata, &data) != 0) return 1;
        if (read_data(args->input_files[i], lineCount, metadata, data) != 0) {
            free(metadata);
            free(data);
            return 1;
        }
        if (strcmp(args->action[i], "insert") == 0) {
            if (execute_hash_operation(args, i, "insert", lineCount, metadata) != 0) {
                free(metadata);
                free(data);
                return 1;
            }
        } else if (strcmp(args->action[i], "delete") == 0) {
            if (execute_hash_operation(args, i, "delete", lineCount, metadata) != 0) {
                free(metadata);
                free(data);
                return 1;
            }
        } else {
            fprintf(stderr, "Unknown action: %s\n", args->action[i]);
        }

        free(metadata);
        free(data);
    }

    // Cleanup global resources
    cleanup_table_and_locks();

    return 0;
}

int main(int argc, char *argv[]) {
    ProgramArgs args;
    if (parse_arguments(argc, argv, &args) != 0) {
        return 1;
    }

    return run_app(&args);
}
