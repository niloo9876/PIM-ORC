#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include <getopt.h>
#include <pthread.h>

#include <dpu.h>
#include <dpu_memory.h>
#include <dpu_log.h>

#include "pim_snappy.h"
#include "PIM-common/common/include/common.h"

// Buffer context struct for input and output buffers on host
typedef struct host_buffer_context
{
    uint8_t *buffer;        // Entire buffer
    uint8_t *curr;          // Pointer to current location in buffer
    uint32_t length;        // Length of buffer
} host_buffer_context_t;

// Arguments passed to a particular thread
struct thread_args {
	int data_ready;
	host_buffer_context_t **input;  // Input buffer
	host_buffer_context_t **output; // Output buffer
	int retval;
};

// Stores allocated DPUs
static struct dpu_set_t dpus;
static num_ranks = 0;
static num_dpus = 0;

// Thread variables
static pthread_mutex_t host_mutex;
static pthead_cond_t host_cond;

static pthread_mutex_t dpu_mutex;
static pthread_cond_t dpu_cond;
static pthread_t *threads;

static struct thread_args *thread_args;
static int *slots_remaining_per_thread;

/**
 * Attempt to read a varint from the input buffer. The format of a varint
 * consists of little-endian series of bytes where the lower 7 bits are data
 * and the upper bit is set if there are more bytes to read. Maximum size
 * of the varint is 5 bytes.
 *
 * @param input: holds input buffer information
 * @param val: read value of the varint
 * @return False if all 5 bytes were read and there is still more data to
 *         read, True otherwise
 */
static inline bool read_varint32(struct host_buffer_context *input, uint32_t *val)
{
    int shift = 0;
    *val = 0;

    for (uint8_t count = 0; count < 5; count++) {
        int8_t c = (int8_t)(*input->curr++);
        *val |= (c & BITMASK(7)) << shift;
        if (!(c & (1 << 7)))
            return true;
        shift += 7;
    }   

    return false;
}

/**
 * Perform the Snappy decompression on the DPU.
 *
 * @param input: holds input buffer information
 * @param output: holds output buffer information
 * @return 1 if successful, 0 otherwise
 */
static void * dpu_uncompress(void *arg) {
	// Get the thread arguments
	struct thread_args *args = (struct thread_args *)arg;

	while (args->data_ready != -1) { // -1 means the thread is being terminated
		// Wait until data is ready 
		pthread_mutex_lock(&host_mutex);
		while (args->data_ready != 1) {
			pthread_cond_wait(&dpu_cond, &host_mutex);
			

    struct dpu_set_t dpu_rank;
    struct dpu_set_t dpu;

    // Calculate input length of only the decompressed stream
	uint32_t input_offset[NR_DPUS][NR_TASKLETS] = {0};
	uint32_t output_offset[NR_DPUS][NR_TASKLETS] = {0};

    uint32_t dpu_idx = 0;
    DPU_RANK_FOREACH(dpus, dpu_rank) {
        uint32_t starting_dpu_idx = dpu_idx;
        DPU_FOREACH(dpu_rank, dpu) {
            // Check to get rid of array bounds compiler warning
            if (dpu_idx >= NR_DPUS)
                break;

			uint32_t input_length = input->length - (input->curr - input->buffer);
            DPU_ASSERT(dpu_copy_to(dpu, "input_length", 0, &(input_length), sizeof(uint32_t)));
            DPU_ASSERT(dpu_copy_to(dpu, "output_length", 0, &(output->length), sizeof(uint32_t)));

            DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)(input->curr + input_offset[dpu_idx][0])));
            dpu_idx++;
        }
        DPU_ASSERT(dpu_push_xfer(dpu_rank, DPU_XFER_TO_DPU, "input_buffer", 0, ALIGN(input->length, 8), DPU_XFER_DEFAULT));

        dpu_idx = starting_dpu_idx;
        DPU_FOREACH(dpu_rank, dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)input_offset[dpu_idx]));
            dpu_idx++;
        }
        DPU_ASSERT(dpu_push_xfer(dpu_rank, DPU_XFER_TO_DPU, "input_offset", 0, sizeof(uint32_t) * NR_TASKLETS, DPU_XFER_DEFAULT));

        dpu_idx = starting_dpu_idx;
        DPU_FOREACH(dpu_rank, dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)output_offset[dpu_idx]));
            dpu_idx++;
        }
        DPU_ASSERT(dpu_push_xfer(dpu_rank, DPU_XFER_TO_DPU, "output_offset", 0, sizeof(uint32_t) * NR_TASKLETS, DPU_XFER_DEFAULT));
    }

    // Launch all DPUs
    int ret = dpu_launch(dpus, DPU_SYNCHRONOUS);
    if (ret != 0)
    {
        DPU_ASSERT(dpu_free(dpus));
        return false;
    }

    // Deallocate the DPUs
    dpu_idx = 0;
    DPU_RANK_FOREACH(dpus, dpu_rank) {
        DPU_FOREACH(dpu_rank, dpu) {
            // Get the results back from the DPU
            DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)(output->buffer + output_offset[dpu_idx][0])));
            dpu_idx++;
        }

        DPU_ASSERT(dpu_push_xfer(dpu_rank, DPU_XFER_FROM_DPU, "output_buffer", 0, ALIGN(output->length, 8), DPU_XFER_DEFAULT));
    }

    return true;
}

int pim_init(void) {
	// Allocate all DPUs, then check how many were allocated
    DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpus));
	
	dpu_get_nr_ranks(dpus, &num_ranks);
	dpu_get_nr_dpus(dpus, &num_dpus);

	// Load the program to all DPUs
    DPU_ASSERT(dpu_load(dpus, DPU_PROGRAM, NULL));

	// Create a thread per rank
	threads = (pthread_t *)malloc(sizeof(pthread_t) * num_ranks);
	thread_args = (struct thread_args *)malloc(sizeof(struct thread_args) * num_ranks);
	for (int i = 0; i < num_ranks; i++) {
		struct thread_args *args = thread_args[i];
		args->data_ready = 0;
		args->input = &((host_buffer_context_t *)malloc(sizeof(host_buffer_context_t) * NR_TASKLETS * (num_dpus / num_ranks)));	
		args->output = &((host_buffer_context_t *)malloc(sizeof(host_buffer_context_t) * NR_TASKLETS * (num_dpus / num_ranks)));	

		if (pthread_create(&threads[i], NULL, dpu_decompress, args) != 0) {
			fprintf(stderr, "Failed to create dpu_decompress pthreads\n");
			return -1;
		}
	}

	// Initialize number of data slots remaining per thread
	slots_remaining_per_thread = (int *)malloc(sizeof(int) * num_ranks);
	memset(slots_remaining_per_thread, NR_TASKLETS * (num_dpus / num_ranks), sizeof(int) * num_ranks);

	// Create mutex to guard access to DPUs
	if (pthread_mutex_init(&host_mutex, NULL) != 0) {
		fprintf(stderr, "Failed to create host mutex\n");
		return -1;
	}
	if (pthread_mutex_init(&dpu_mutex, NULL) != 0) {
		fprintf(stderr, "Failed to create dpu mutex\n");
		return -1;
	}

	// Create condition variable to wake up threads when data is ready
	if (pthread_cond_init(&host_cond, NULL) != 0) {
		fprintf(stderr, "Faled to create condition variable\n");
		return -1;
	}
	if (pthread_cond_init(&dpu_cond, NULL) != 0) {
		fprintf(stderr, "Faled to create condition variable\n");
		return -1;
	}

	return 0;
}

void pim_deinit(void) {
    DPU_ASSERT(dpu_free(dpus));
	num_ranks = 0;
	num_dpus = 0;

	// Signal to terminate the threads
	for (int i = 0; i < num_ranks; i++) {
		thread_args[i]->data_ready = -1;
	}
	pthread_cond_broadcast(&cond);

	// Free all the allocated memory
	for (int i = 0; i < num_ranks; i++) {
		pthread_join(threads[i], NULL);

		struct thread_args *args = thread_args[i];
		free(*(args->input));
		free(*(args->output));
		free(args);
	}	
	free(threads);
	free(thread_args);
	free(slots_remaining_per_thread);

	// Destroy the mutex
	pthread_mutex_destroy(&host_mutex);
	pthread_mutex_destroy(&dpu_mutex);

	// Destroy the condition variable
	pthread_cond_destroy(&host_cond);
	pthread_cond_destroy(&dpu_cond);
}

int pim_decompress(const char *compressed, size_t compressed_length, char *uncompressed) {
	for (int i = 0; i < num_ranks; i++) {
		pthread_mutex_lock(&host_mutex);
		
		slot = slots_remaining_per_thread[i];
		if (slot != 0) {
			struct thread_args *args = thread_args[i];	

			args->input[slot - 1]->buffer = compressed;
			args->input[slot - 1]->curr = compressed;
			args->input[slot - 1]->length = compressed_length;

			args->output[slot - 1]->buffer = uncompressed;
			args->output[slot - 1]->curr = uncompressed;
			args->output[slot - 1]->length = 0;

			// Read the decompressed length
			if (!read_varint32(args->input[slot - 1], &(args->output[slot - 1]->length))) {
				fprintf(stderr, "Failed to read decompressed length\n");
				return false;
			}

			slots_remaining_per_thread[i]--;
			if (slots_remaining_per_thread[i] == 0) {
				args->data_ready = 1;
				pthread_cond_broadcast(&dpu_cond);
			}

			// Wait for the data to be returned
			while (args->data_ready != 0) {
				pthread_cond_wait(&host_cond, &host_mutex);
			}
			pthread_mutex_unlock(&host_mutex);

			return args->retval;
		}

		pthread_mutex_unlock(&host_mutex);
	}

	return false;
}
