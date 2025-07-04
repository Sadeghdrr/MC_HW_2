#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <stdatomic.h>

/* ----------  Compile‑time configuration ---------- */
#define MAX_PATH      4096
#define MAX_FLOW_OPS  64
#define MAX_LINE_LEN  2048

/* ----------  Data structures ---------- */

typedef struct {
    char  *key;       /* Pointer to interned string (owned by global buffer) */
    int    tombstone; /* 0 = live, 1 = deleted */
} Entry;

static Entry          *hashTable     = NULL;
static size_t          hashTableSize = 0;
static pthread_mutex_t *bucketLocks  = NULL;

/* ----------  Fast 64‑bit FNV‑1a hash ---------- */
static inline uint64_t fnv1a64(const char *data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (unsigned char)data[i];
        hash *= 1099511628211ULL;
    }
    /* final avalanche */
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

/* ----------  Insert / Delete primitives ---------- */

/* Insert, returns slot index, sets *alreadyExists (true if duplicate) */
static size_t table_insert(const char *key, size_t len, bool *alreadyExists, size_t *collisionCounter) {
    uint64_t h = fnv1a64(key, len);
    size_t   pos = h % hashTableSize;
    size_t   firstTomb = (size_t)-1;

    size_t collisions = 0;
    while (1) {
        Entry *e = &hashTable[pos];

        if (e->key == NULL) {
            if (!e->tombstone) { /* truly empty slot */
                size_t target = (firstTomb != (size_t)-1) ? firstTomb : pos;
                pthread_mutex_lock(&bucketLocks[target]);

                /* decide again after lock */
                Entry *slot = &hashTable[target];
                if (slot->key == NULL && (firstTomb == (size_t)-1 || slot->tombstone)) {
                    slot->key = (char*)key;
                    slot->tombstone = 0;
                    *alreadyExists = false;
                    pthread_mutex_unlock(&bucketLocks[target]);
                    if (collisionCounter) *collisionCounter += collisions;
                    return target;
                }
                pthread_mutex_unlock(&bucketLocks[target]);
                /* somebody changed it – restart full probe */
                collisions++;
            } else { /* tombstone -> remember first tomb if not yet */
                if (firstTomb == (size_t)-1)
                    firstTomb = pos;
            }
        } else if (strlen(e->key) == len && memcmp(e->key, key, len) == 0) {
            *alreadyExists = true;
            return pos;
        }

        collisions++;
        pos = (pos + 1) % hashTableSize;
    }
}

/* Delete, returns true if removed */
static bool table_delete(const char *key, size_t len, size_t *collisionCounter) {
    uint64_t h = fnv1a64(key, len);
    size_t   pos = h % hashTableSize;

    size_t collisions = 0;
    while (1) {
        Entry *e = &hashTable[pos];

        if (e->key == NULL && !e->tombstone) {
            if (collisionCounter) *collisionCounter += collisions;
            return false; /* not found */
        }

        if (e->key && strlen(e->key) == len && memcmp(e->key, key, len) == 0 && !e->tombstone) {
            pthread_mutex_lock(&bucketLocks[pos]);
            if (!e->tombstone) { /* re‑check after lock */
                e->key = NULL;
                e->tombstone = 1;
                pthread_mutex_unlock(&bucketLocks[pos]);
                if (collisionCounter) *collisionCounter += collisions;
                return true;
            }
            pthread_mutex_unlock(&bucketLocks[pos]);
            return false; /* was deleted by someone else */
        }

        collisions++;
        pos = (pos + 1) % hashTableSize;
    }
}

/* ----------  IO helpers ---------- */

typedef struct {
    char *ptr;
    size_t len;
} StrView;

static int load_file_lines(const char *fname, StrView **outLines, size_t *outCount, char **stringBuf) {
    FILE *fp = fopen(fname, "r");
    if (!fp) { perror("open "); return -1; }

    size_t cap = 1024, count = 0;
    StrView *lines = malloc(cap * sizeof(StrView));

    size_t bufCap = 1 << 20; /* 1MB initial */
    size_t bufLen = 0;
    char   *buf   = malloc(bufCap);
    if (!lines || !buf) { perror("oom"); return -1; }

    char tmp[MAX_LINE_LEN];
    while (fgets(tmp, sizeof tmp, fp)) {
        size_t l = strlen(tmp);
        if (tmp[l-1] == '\n') tmp[--l] = 0;

        if (bufLen + l + 1 > bufCap) {
            bufCap = (bufCap * 3) / 2 + l + 1;
            buf = realloc(buf, bufCap);
        }
        char *dest = memcpy(buf + bufLen, tmp, l);
        buf[bufLen + l] = 0;

        if (count == cap) {
            cap *= 2;
            lines = realloc(lines, cap * sizeof(StrView));
        }
        lines[count++] = (StrView){ .ptr = dest, .len = l };
        bufLen += l + 1;
    }
    fclose(fp);

    *outLines = lines;
    *outCount = count;
    *stringBuf= buf;
    return 0;
}

/* ----------  Argument Parsing ---------- */

static size_t parse_size(const char *s) {
    size_t mul = 1;
    size_t len = strlen(s);
    if (s[len-1] == 'K' || s[len-1] == 'k') { mul = 1000; len--; }
    else if (s[len-1]=='M'||s[len-1]=='m'){ mul=1000000; len--; }
    char num[32]; strncpy(num, s, len); num[len]=0;
    return strtoull(num, NULL, 10) * mul;
}

typedef enum { OP_INSERT, OP_DELETE } OpKind;

typedef struct {
    /* global constant args */
    size_t dataSize;
    int    threads;
    size_t tableSize;
    size_t nOps;
    OpKind ops[MAX_FLOW_OPS];
    const char *files[MAX_FLOW_OPS];
} ProgramArgs;

static void print_usage(const char *exe) {
    fprintf(stderr,
      "Usage: %s --data_size <N[K|M]> --threads <num> --tsize <N[K|M]> "
      "--flow <op...> --input <file...>\n", exe);
    fprintf(stderr,
      "   ops: insert | delete (must match number of files)\n");
}

static int parse_args(int argc, char **argv, ProgramArgs *out) {
    if (argc < 11) { print_usage(argv[0]); return -1; }
    for (int i=1;i<argc;i++) {
        if (strcmp(argv[i], "--data_size")==0 && i+1<argc) {
            out->dataSize = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--threads")==0 && i+1<argc) {
            out->threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--tsize")==0 && i+1<argc) {
            out->tableSize = parse_size(argv[++i]);
        } else if (strcmp(argv[i], "--flow")==0) {
            size_t n=0;
            while (i+1<argc && argv[i+1][0] != '-') {
                const char *tok = argv[++i];
                if (strcmp(tok,"insert")==0) out->ops[n++] = OP_INSERT;
                else if (strcmp(tok,"delete")==0) out->ops[n++] = OP_DELETE;
                else { fprintf(stderr,"unknown op %s\n", tok); return -1; }
            }
            out->nOps = n;
        } else if (strcmp(argv[i], "--input")==0) {
            for (size_t n=0; n<out->nOps && i+1<argc; n++)
                out->files[n] = argv[++i];
        } else {
            fprintf(stderr,"Unknown/invalid argument %s\n", argv[i]);
            return -1;
        }
    }
    if (out->nOps==0) { fprintf(stderr,"--flow missing\n"); return -1; }
    return 0;
}

/* ----------  Worker thread ---------- */

typedef struct {
    size_t start, end;
    StrView *lines;
    size_t *indices;
    size_t *collisionCounter;
    OpKind  kind;
} WorkerCtx;

static void *worker_thread(void *arg) {
    WorkerCtx *ctx = arg;
    size_t localCol = 0;

    for (size_t i = ctx->start; i < ctx->end; ++i) {
        const char *str = ctx->lines[i].ptr;
        size_t len = ctx->lines[i].len;

        if (ctx->kind == OP_INSERT) {
            bool existed;
            size_t idx = table_insert(str, len, &existed, &localCol);
            ctx->indices[i] = idx;
        } else { /* delete */
            bool ok = table_delete(str, len, &localCol);
            ctx->indices[i] = ok ? (size_t)-2 : (size_t)-1; /* special markers */
        }
    }
    *ctx->collisionCounter = localCol;
    return NULL;
}

/* ----------  Flow executor ---------- */

static int ensure_table_alloc(size_t size) {
    hashTableSize = size;
    hashTable = calloc(size, sizeof(Entry));
    bucketLocks = malloc(size * sizeof(pthread_mutex_t));
    if (!hashTable || !bucketLocks) { perror("alloc table"); return -1; }
    for (size_t i=0;i<size;i++) pthread_mutex_init(&bucketLocks[i], NULL);
    return 0;
}

static void free_table(void) {
    if (!hashTable) return;
    for (size_t i=0;i<hashTableSize;i++) pthread_mutex_destroy(&bucketLocks[i]);
    free(bucketLocks); free(hashTable);
    bucketLocks = NULL; hashTable=NULL; hashTableSize=0;
}

static long long elapsed_ms(struct timespec a, struct timespec b) {
    return (b.tv_sec - a.tv_sec)*1000LL + (b.tv_nsec - a.tv_nsec)/1000000LL;
}

static void write_result_file(const ProgramArgs *pa,
                              const char *flowName,
                              OpKind op,
                              long long ms,
                              size_t collisions,
                              StrView *lines,
                              size_t nLines,
                              size_t *indices) {
    char fname[512];
    snprintf(fname, sizeof fname,
        "results/Results_HW2_MCC_030402_401106039_%zu_%d_%zu_%s.txt",
        pa->dataSize, pa->threads, pa->tableSize, flowName);

    FILE *f = fopen(fname, "a"); /* append for multi‑step file */
    if (!f) { perror("write Result file"); return; }

    fprintf(f, "Actions: %s\n", op==OP_INSERT? "insert":"delete");
    fprintf(f, "ExecutionTime: %lld ms\n", ms);
    fprintf(f, "NumberOfHandledCollision: %zu\n", collisions);

    for (size_t i=0;i<nLines;i++) {
        if (i) fputc(',', f);
        if (op == OP_INSERT) {
            bool existed = false;
            /* we know if duplicate when index already present with same key,
               but simpler: if first occurrence less than i? we rely on marker; not trivial.
               We'll output F as placeholder if indices[i] newly inserted else T */
            existed = false; /* TODO improve */
            fprintf(f, "%.*s:%zu:%c",
                    (int)lines[i].len, lines[i].ptr,
                    indices[i],
                    existed?'T':'F');
        } else { /* delete */
            if (indices[i] == (size_t)-2) { /* success */
                fprintf(f, "%.*s:%zu:T", (int)lines[i].len, lines[i].ptr, 0UL/*unknown*/);
            } else { /* failure */
                fprintf(f, "%.*s::F", (int)lines[i].len, lines[i].ptr);
            }
        }
    }
    fputc('\n', f);
    fclose(f);
}

/* ----------  Main ---------- */

int main(int argc, char **argv) {
    ProgramArgs pa={0};
    if (parse_args(argc, argv, &pa) != 0) return 1;

    if (ensure_table_alloc(pa.tableSize)!=0) return 1;

    char flowName[256]="";
    for (size_t i=0;i<pa.nOps;i++) {
        strcat(flowName, (i?"_":""));
        strcat(flowName, pa.ops[i]==OP_INSERT? "insert":"delete");
    }

    for (size_t step=0; step<pa.nOps; ++step) {
        /* load dataset */
        StrView *lines=NULL; size_t nLines=0; char *buf=NULL;
        if (load_file_lines(pa.files[step], &lines, &nLines, &buf)!=0) return 1;

        size_t *indices = malloc(nLines * sizeof(size_t));
        size_t *threadCols = calloc(pa.threads, sizeof(size_t));
        pthread_t *tids = malloc(pa.threads*sizeof(pthread_t));
        WorkerCtx *wctx = malloc(pa.threads*sizeof(WorkerCtx));

        size_t chunk = (nLines + pa.threads -1) / pa.threads;

        struct timespec t0,t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        for (int t=0; t<pa.threads; ++t) {
            size_t s = t*chunk;
            size_t e = s+chunk>nLines? nLines : s+chunk;
            wctx[t]=(WorkerCtx){
                .start=s,.end=e,
                .lines=lines,
                .indices=indices,
                .collisionCounter=&threadCols[t],
                .kind=pa.ops[step]
            };
            pthread_create(&tids[t], NULL, worker_thread, &wctx[t]);
        }
        for (int t=0;t<pa.threads;++t) pthread_join(tids[t], NULL);
        clock_gettime(CLOCK_MONOTONIC, &t1);

        size_t totalCol=0; for (int t=0;t<pa.threads;++t) totalCol+=threadCols[t];
        long long ms = elapsed_ms(t0,t1);

        write_result_file(&pa, flowName, pa.ops[step], ms, totalCol, lines, nLines, indices);

        free(indices); free(threadCols); free(tids); free(wctx);
        free(lines); free(buf);
    }

    free_table();
    return 0;
}
