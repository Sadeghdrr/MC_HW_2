# Makefile for HW2 – Multithreaded Index Mapping with Delete
# Author: Abolfazl Sheikhha
#
# Build targets:
#   make            - Optimized build (default)
#   make debug      - Debug build with symbols
#   make clean      - Remove binaries & result files
#   make run        - Quick demo run (default parameters)
#   make perf-test  - Large‑scale performance sweep
#   make help       - Print this help

# ---------------------------------------------------------------------------
# Toolchain
CC       := gcc
CFLAGS   := -Wall -Wextra -std=c11 -O2 -pthread
LDFLAGS  := -pthread

# ---------------------------------------------------------------------------
# Project layout
SRC_DIR      := src
BIN_DIR      := bin
RESULTS_DIR  := results

TARGET := $(BIN_DIR)/HW2_MCC_030402_401106039
SRC    := $(SRC_DIR)/main.c

# ---------------------------------------------------------------------------
# Default example parameters (handy for "make run")
DEFAULT_FLOW  := insert insert delete insert
DEFAULT_INPUT := 150K_set1.txt 150K_set2.txt 150K_set1.txt 150K_set2.txt

# ---------------------------------------------------------------------------
# Build rules
.PHONY: all debug clean run perf-test help

all: $(TARGET)

debug: CFLAGS := -Wall -Wextra -std=c11 -g -O0 -pthread
debug: LDFLAGS := -pthread
debug: clean all

$(TARGET): $(SRC) | $(BIN_DIR) $(RESULTS_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BIN_DIR):
	mkdir -p $@

$(RESULTS_DIR):
	mkdir -p $@

clean:
	rm -rf $(BIN_DIR) $(RESULTS_DIR) *.o

# ---------------------------------------------------------------------------
# Quick functional sanity check
run: $(TARGET)
	@echo "Running default demo..."
	@$(TARGET) --data_size 150K --threads 8 --tsize 150K \
	           --flow $(DEFAULT_FLOW) \
	           --input $(DEFAULT_INPUT)

# ---------------------------------------------------------------------------
# Comprehensive performance sweep
perf-test: $(TARGET)
	@echo "Running comprehensive performance tests..."
	@mkdir -p $(RESULTS_DIR)
	@for datasize in 150K 300K 600K; do \
		case $$datasize in \
			150K) tsize3=90K  ; tsize4=120K ; tsize5=150K ;; \
			300K) tsize3=180K ; tsize4=240K ; tsize5=300K ;; \
			600K) tsize3=360K ; tsize4=480K ; tsize5=600K ;; \
		esac; \
		for threads in 1 2 4 8 16 32 64 128 256 512 1024; do \
			for tsize in $$tsize2 $$tsize3 $$tsize4; do \
				echo "Testing: data_size=$$datasize threads=$$threads tsize=$$tsize"; \
				$(TARGET) --data_size $$datasize \
				          --threads $$threads \
				          --tsize $$tsize \
				          --flow insert insert delete insert \
				          --input $${datasize}_set1.txt $${datasize}_set2.txt $${datasize}_set1.txt $${datasize}_set2.txt \
			done; \
		done; \
	done
	@echo "Performance tests complete. Results saved under $(RESULTS_DIR)/"

# ---------------------------------------------------------------------------
# Help
help:
	@echo "Available targets:"
	@echo "  all         - Build the program (default)"
	@echo "  debug       - Build with debug symbols"
	@echo "  clean       - Remove build artifacts and result files"
	@echo "  perf-test   - Performance test with different params"
	@echo "  help        - Show this help message"
