#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#define MAX_OPERATIONS 16

/* ===================== Added Data Structures & Helpers ===================== */
typedef struct {
    char *key;          /* NULL  -> empty bucket
                           non‑NULL + tombstone==0 -> valid key
                           NULL  + tombstone==1 -> tombstone (deleted)       */
    uint8_t tombstone;
} HashEntry;

static HashEntry *g_table      = NULL;
static size_t     g_table_size = 0;

/* -------- 64‑bit FNV‑1a hash (same as mid‑term) -------- */
static inline uint64_t fnv1a64(const char *data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (unsigned char)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

/* Ensure that the global hash table is allocated once */
static int ensure_table(size_t size) {
    if (g_table) return 0;
    g_table_size = size;
    g_table = (HashEntry *)calloc(g_table_size, sizeof(HashEntry));
    return g_table ? 0 : -1;
}

/* Insert ‑‑ returns 1 if key already existed, 0 otherwise.
 * collisions_out returns # of probes (collisions) encountered. */
static int hash_insert(const char *key, size_t len,
                       size_t *index_out, size_t *collisions_out) {
    uint64_t h  = fnv1a64(key, len);
    size_t   pos = h % g_table_size;
    size_t   first_tombstone = (size_t)(-1);
    size_t   collisions = 0;

    while (1) {
        HashEntry *e = &g_table[pos];

        if (e->key == NULL) {
            /* Empty bucket */
            if (e->tombstone) {
                /* Keep the first tombstone position in case we need to reuse it */
                if (first_tombstone == (size_t)(-1))
                    first_tombstone = pos;
            } else {
                /* Truly empty – choose either the first tombstone or this slot */
                size_t target = (first_tombstone == (size_t)(-1)) ? pos : first_tombstone;
                g_table[target].key = strdup(key);   /* store independent copy */
                g_table[target].tombstone = 0;
                *index_out       = target;
                *collisions_out  = collisions;
                return 0; /* did not previously exist */
            }
        } else if (strlen(e->key) == len && memcmp(e->key, key, len) == 0) {
            /* Already present */
            *index_out      = pos;
            *collisions_out = collisions;
            return 1;       /* existed */
        } else {
            /* Different key – keep probing */
            collisions++;
        }
        pos = (pos + 1) % g_table_size;
    }
}

/* Delete ‑‑ returns 1 if key found & deleted, 0 otherwise.
 * collisions_out returns # of probes (collisions) encountered. */
static int hash_delete(const char *key, size_t len,
                       size_t *index_out, size_t *collisions_out) {
    uint64_t h  = fnv1a64(key, len);
    size_t   pos = h % g_table_size;
    size_t   collisions = 0;

    while (1) {
        HashEntry *e = &g_table[pos];

        if (e->key == NULL) {
            if (e->tombstone) {
                /* keep probing past tombstones */
                collisions++;
            } else {
                /* empty bucket => not present */
                *collisions_out = collisions;
                return 0;
            }
        } else if (strlen(e->key) == len && memcmp(e->key, key, len) == 0) {
            /* Found – delete */
            free(e->key);          /* free the heap copy */
            e->key       = NULL;
            e->tombstone = 1;
            *index_out      = pos;
            *collisions_out = collisions;
            return 1;
        } else {
            collisions++;
        }
        pos = (pos + 1) % g_table_size;
    }
}

/* Helper to build the result file name only once */
static char results_path[512];
static int  results_path_ready = 0;
static void build_results_path(size_t data_size, int threads,
                               size_t tsize,
                               const ProgramArgs *args) {
    if (results_path_ready) return;

    /* build flow string e.g. insert_insert_delete_insert */
    char flow[256] = "";
    for (int i = 0; i < args->num_operations; ++i) {
        strcat(flow, args->action[i]);
        if (i + 1 < args->num_operations) strcat(flow, "_");
    }

    snprintf(results_path, sizeof(results_path),
             "results/Results_HW2_MCC_030402_401106039_%zu_%d_%zu_%s.txt",
             data_size, threads, tsize, flow);

    results_path_ready = 1;
}

/* ===================== Original Skeleton (unchanged) ===================== */
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

/* ----------------- helper functions provided by skeleton ----------------- */
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

/* ====================== MAIN WORKFLOW ====================== */
int run_app(const ProgramArgs *args) {
    build_results_path(args->data_size, args->threads, args->tsize, args);

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

        /* ----------------------------------------------------
         * Ensure the hash table exists (once for the program)
         * --------------------------------------------------*/
        if (ensure_table(args->tsize) != 0) {
            fprintf(stderr, "Unable to allocate hash table (size=%zu)\n", args->tsize);
            return 1;
        }

        /* Measure time */
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        size_t total_collisions = 0;

        if (strcmp(args->action[i], "insert") == 0) {
            printf("Inserting %zu records...\n", lineCount);

            ////////////////////////////////////////Write Your Code//////////////////////////////////////////////
            for (size_t j = 0; j < lineCount; ++j) {
                size_t idx, col;
                int existed = hash_insert(metadata[j].ptr,
                                          metadata[j].length,
                                          &idx, &col);
                total_collisions += col;

                /* Lazy write of output line – defer to after timing. */
                metadata[j].length = idx;         /* Overload length field to carry index for later printing */
                metadata[j].ptr     = (char *)((uintptr_t)metadata[j].ptr | ((existed ? 1 : 0) << 0)); /* tag LSB */
            }
            /////////////////////////////////////////////////////////////////////////////////////////////////////

        } else if (strcmp(args->action[i], "delete") == 0) {
            printf("Deleting %zu records...\n", lineCount);

            ////////////////////////////////////////Write Your Code//////////////////////////////////////////////
            for (size_t j = 0; j < lineCount; ++j) {
                size_t idx, col;
                int deleted = hash_delete(metadata[j].ptr,
                                          metadata[j].length,
                                          &idx, &col);
                total_collisions += col;

                metadata[j].length = idx;     /* even if not used, store to print */
                metadata[j].ptr     = (char *)((uintptr_t)metadata[j].ptr | ((deleted ? 1 : 0) << 0)); /* tag */
            }
            /////////////////////////////////////////////////////////////////////////////////////////////////////

        } else {
            fprintf(stderr, "Unknown action: %s\n", args->action[i]);
        }

        /* Measure end‑time */
        clock_gettime(CLOCK_MONOTONIC, &t1);
        long long elapsed_ms = (t1.tv_sec  - t0.tv_sec) * 1000LL + (t1.tv_nsec - t0.tv_nsec) / 1000000LL;

        /* --------------------- Write results section --------------------- */
        FILE *out = fopen(results_path, (i == 0) ? "w" : "a");
        if (!out) {
            perror("Cannot open results file for writing");
        } else {
            fprintf(out, "Actions: %s\n", args->action[i]);
            fprintf(out, "ExecutionTime: %lld ms\n", elapsed_ms);
            fprintf(out, "NumberOfHandledCollision: %zu\n", total_collisions);
            /* Re‑iterate to emit Data lines */
            for (size_t j = 0; j < lineCount; ++j) {
                int flag = ((uintptr_t)metadata[j].ptr) & 1; /* 1==T, 0==F */
                char *strptr = (char *)(((uintptr_t)metadata[j].ptr) & ~(uintptr_t)1);

                if (strcmp(args->action[i], "delete") == 0 && !flag) {
                    /* Failed delete – no index in output */
                    fprintf(out, "%s:F", strptr);
                } else {
                    fprintf(out, "%s:%zu:%c", strptr,
                            metadata[j].length,
                            flag ? 'T' : 'F');
                }
                if (j + 1 < lineCount) fputc(',', out);
            }
            fputc('\n', out);
            fclose(out);
        }

        /* ---------------------------------------------------------------- */

        free(metadata);
        free(data);
    }

    return 0;
}

int main(int argc, char *argv[]) {
    ProgramArgs args;
    if (parse_arguments(argc, argv, &args) != 0) {
        return 1;
    }

    return run_app(&args);
}
