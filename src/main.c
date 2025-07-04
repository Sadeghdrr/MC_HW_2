#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
            printf("Inserting %zu records...\n", lineCount);
      
            ////////////////////////////////////////Write Your Code//////////////////////////////////////////////





            /////////////////////////////////////////////////////////////////////////////////////////////////////


        } else if (strcmp(args->action[i], "delete") == 0) {
            printf("Deleting %zu records...\n", lineCount);
                
            ////////////////////////////////////////Write Your Code//////////////////////////////////////////////





            /////////////////////////////////////////////////////////////////////////////////////////////////////
            
        } else {
            fprintf(stderr, "Unknown action: %s\n", args->action[i]);
        }

        

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
