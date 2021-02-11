#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include <getopt.h>
#include <pthread.h>
#include <sys/time.h>

#include <dpu.h>
#include <dpu_memory.h>
#include <dpu_log.h>

#include "pim_snappy.h"
#include "PIM-common/common/include/common.h"

// Parameters to tune
#define REQUESTS_TO_WAIT_FOR 3 // Number of requests to wait for before sending
#define MAX_TIME_WAIT_MS 5     // Time in ms to wait before sending current requests
#define MAX_TIME_WAIT_S (MAX_TIME_WAIT_MS / 1000)

// TODO: consolidate these with what is in dpu_task.c, should be in one place
#define MAX_INPUT_SIZE (256 * 1024)
#define MAX_OUTPUT_SIZE (512 * 1024)

// Buffer context struct for input and output buffers on host
typedef struct host_buffer_context
{
    char *buffer;        // Entire buffer
    char *curr;          // Pointer to current location in buffer
    uint32_t length;     // Length of buffer
} host_buffer_context_t;

// Arguments passed by a particular thread
typedef struct caller_args {
	int data_ready;	               // 1 if request is waiting, 0 once request is handled
	host_buffer_context_t *input;  // Input buffer
	host_buffer_context_t *output; // Output buffer
	int retval;                    // Return error code from processing request
} caller_args_t;

// Argument to DPU handler thread
typedef struct master_args {
	int stop_thread;                // Set to 1 to end dpu_master_thread
	uint32_t req_head;              // Next free slot in caller_args
	uint32_t req_tail;              // Next busy slot in caller_args
	uint32_t req_tail_dispatched;   // Next slot to be loaded to DPU in caller_args 
	uint32_t req_count;             // Number of occupied slots in caller_args
	uint32_t req_waiting;           // Number of requests waiting that haven't been dispatched
	caller_args_t **caller_args;    // Request buffer
} master_args_t;

// Stores number of allocated DPUs
static struct dpu_set_t dpus;
static uint32_t num_ranks = 0;
static uint32_t num_dpus = 0;

// Thread variables
static pthread_mutex_t mutex;
static pthread_cond_t caller_cond;
static pthread_cond_t dpu_cond;

static pthread_t dpu_master_thread;
static master_args_t args;
static uint32_t total_request_slots = 0;

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
 * Get a bitmap of the free ranks currently available.
 *
 * @param free_ranks: each bit is set to 1 if that rank is
 *                    currently available
 */
static void get_free_ranks(uint32_t* free_ranks) {
    struct dpu_set_t dpu_rank;
	uint32_t rank_id = 0;
	
	DPU_RANK_FOREACH(dpus, dpu_rank) {
		bool done = 0, fault = 0;

		// Check if any rank is free
		dpu_status(dpu_rank, &done, &fault);
		if (fault) {
			fprintf(stderr, "Fault on DPU rank %d\n", rank_id);
			// TODO: error handle
		}

		if (done) 
			*free_ranks |= (1 << rank_id);

		rank_id++;
	}
}

/**
 * Load a set of requests to a DPU rank.
 *
 * @param dpu_rank: pointer to the rank handle to load to
 * @param args: pointer to the DPU handler thread args
 */
static void load_rank(struct dpu_set_t *dpu_rank, master_args_t *args) {
	uint32_t idx = args->req_tail_dispatched;
	uint32_t start_idx = args->req_tail_dispatched;

	// Zero out the rank
	uint32_t zero = 0;
	DPU_ASSERT(dpu_copy_to(*dpu_rank, "input_length", 0, &zero, NR_TASKLETS * sizeof(uint32_t)));

	struct dpu_set_t dpu;
	for (int i = 0; i < NR_TASKLETS; i++) {
		if (idx == args->req_head)
			break;

		// Copy the index of the request, input and output lengths
		uint32_t max_input_length = 0;
		uint32_t total_dpu_count = 0;
		DPU_FOREACH(*dpu_rank, dpu) {
			if (idx == args->req_head)
				break;

			DPU_ASSERT(dpu_copy_to(dpu, "req_idx", i * sizeof(uint32_t), &idx, sizeof(uint32_t)));

			uint32_t input_length = args->caller_args[idx]->input->length - (args->caller_args[idx]->input->curr - args->caller_args[idx]->input->buffer);
			DPU_ASSERT(dpu_copy_to(dpu, "input_length", i * sizeof(uint32_t), &(input_length), sizeof(uint32_t)));
			DPU_ASSERT(dpu_copy_to(dpu, "output_length", i * sizeof(uint32_t), &(args->caller_args[idx]->output->length), sizeof(uint32_t))); 

			// Update max input length
			max_input_length = MAX(max_input_length, ALIGN(input_length, 8));

			idx = (idx + 1) % total_request_slots;
			total_dpu_count++;
		}

		// Copy the input buffer 
		// TODO: adjust how this is done so we don't have to malloc a massive buffer every time
		idx = start_idx;
		uint32_t dpu_count = 0;
		uint8_t *buf = malloc(max_input_length * total_dpu_count);
		DPU_FOREACH(*dpu_rank, dpu) {
			if (idx == args->req_head)
				break;

			memcpy(&buf[dpu_count * max_input_length], args->caller_args[idx]->input->curr, args->caller_args[idx]->input->length - (args->caller_args[idx]->input->curr - args->caller_args[idx]->input->buffer));
			DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)&buf[dpu_count * max_input_length]));

			idx = (idx + 1) % total_request_slots;
			dpu_count++;
			args->req_waiting--;
		}

		DPU_ASSERT(dpu_push_xfer(*dpu_rank, DPU_XFER_TO_DPU, "input_buffer", i * MAX_INPUT_SIZE, max_input_length, DPU_XFER_DEFAULT));

		free(buf);
		start_idx = idx;
	}

	args->req_tail_dispatched = idx;
	
	// Launch the rank
	dpu_launch(*dpu_rank, DPU_ASYNCHRONOUS);
}

/**
 * Unload the finished requests off of a ran.
 *
 * @param dpu_rank: pointer to DPU rank handle to unload
 * @param args: pointer to the DPU handler thread args
 */
static void unload_rank(struct dpu_set_t *dpu_rank, master_args_t *args) {
	struct dpu_set_t dpu;
	for (int i = 0; i < NR_TASKLETS; i++) {
		uint32_t max_output_length = 0;
		uint32_t total_dpu_count = 0;
		DPU_FOREACH(*dpu_rank, dpu) {
			// Get the output length
			uint32_t output_length = 0;
			DPU_ASSERT(dpu_copy_from(dpu, "output_length", i * sizeof(uint32_t), &output_length, sizeof(uint32_t)));
			max_output_length = MAX(max_output_length, ALIGN(output_length, 8));
			if (output_length == 0)
				break;
			total_dpu_count++;
		}

		// Get the decompressed buffer
		// TODO: update how this is done so we don't need to malloc massive buffer
		uint32_t dpu_count = 0;
		uint8_t *buf = malloc(max_output_length * total_dpu_count);
		DPU_FOREACH(*dpu_rank, dpu) {
			if (dpu_count == total_dpu_count)
				break;

			// Set up the transfer
			DPU_ASSERT(dpu_prepare_xfer(dpu, (void *)&buf[dpu_count * max_output_length]));
			dpu_count++;
		}

		DPU_ASSERT(dpu_push_xfer(*dpu_rank, DPU_XFER_FROM_DPU, "output_buffer", i * MAX_OUTPUT_SIZE, max_output_length, DPU_XFER_DEFAULT));
		
		dpu_count = 0;
		DPU_FOREACH(*dpu_rank, dpu) {
			if (dpu_count == total_dpu_count)
				break;

			// Get the request index
			uint32_t req_idx = 0;
			DPU_ASSERT(dpu_copy_from(dpu, "req_idx", i * sizeof(uint32_t), &req_idx, sizeof(uint32_t)));

			// TODO fix this in case the ranks don't complete out of order
			if (req_idx == args->req_tail) {
				args->req_count--;
				args->req_tail = (args->req_tail + 1) % total_request_slots;
			}

			// Get the return value
			DPU_ASSERT(dpu_copy_from(dpu, "retval", i * sizeof(uint32_t), &(args->caller_args[req_idx]->retval), sizeof(uint32_t)));
			args->caller_args[req_idx]->data_ready = 0;

			// Copy the data
			memcpy(args->caller_args[req_idx]->output->curr, &buf[dpu_count * max_output_length], args->caller_args[req_idx]->output->length);
			dpu_count++;
		}

		free(buf);
	}
}

/**
 * Calculate the time difference in seconds between start and end.
 *
 * @param start: start time
 * @param end: end time
 * @return difference in seconds
 */
static inline double timediff(struct timeval *start, struct timeval *end) {
	double start_time = start->tv_sec + start->tv_usec / 1000000.0;
	double end_time = end->tv_sec + end->tv_usec / 1000000.0;
	return (end_time - start_time);
}

/**
 * DPU Hander Thread
 *
 * @param arg: pointer to master_args_t structure
 */
static void * dpu_uncompress(void *arg) {
	// Get the thread arguments
	master_args_t *args = (master_args_t *)arg;

	struct timespec time_to_wait;
	struct timeval first, second;

	struct timeval x1, x2, x3, x4;
	uint32_t ranks_dispatched = 0;
	while (args->stop_thread != 1) { 
		gettimeofday(&x1, NULL);
		pthread_mutex_lock(&mutex);

		gettimeofday(&first, NULL);
		gettimeofday(&second, NULL);

		time_to_wait.tv_sec = second.tv_sec;
		time_to_wait.tv_nsec = (second.tv_usec + MAX_TIME_WAIT_MS * 1000) * 1000; // Wait MAX_TIME_WAIT_MS
		
		pthread_cond_timedwait(&dpu_cond, &mutex, &time_to_wait);
		
		// Check if our conditions to send the requests are satisfied
		bool send_req = false;
		gettimeofday(&second, NULL);
		
		if ((args->req_waiting >= REQUESTS_TO_WAIT_FOR) || (timediff(&first, &second) >= MAX_TIME_WAIT_S)) {
			send_req = true;
		}
		pthread_mutex_unlock(&mutex);
		gettimeofday(&x2, NULL);
		// Get the list of ranks currently free
		uint32_t free_ranks = 0;
		get_free_ranks(&free_ranks);

		// If any previously dispatched requests are done, read back the data
		uint32_t rank_id = 0;
		struct dpu_set_t dpu_rank;
		DPU_RANK_FOREACH(dpus, dpu_rank) {
			if (ranks_dispatched & (1 << rank_id)) {
				if (free_ranks & (1 << rank_id)) {
					pthread_mutex_lock(&mutex);
					unload_rank(&dpu_rank, args);
					pthread_mutex_unlock(&mutex);
			
					ranks_dispatched &= ~(1 << rank_id);

					// Signal that data is ready
					pthread_cond_broadcast(&caller_cond);
				}
			}
			rank_id++;
		}
		gettimeofday(&x3, NULL);
		// Dispatch all the requests we currently have
		rank_id = 0;	
		if (send_req) {
			DPU_RANK_FOREACH(dpus, dpu_rank) {
				if ((free_ranks & (1 << rank_id)) && args->req_waiting) {
					pthread_mutex_lock(&mutex);
					load_rank(&dpu_rank, args); 
					pthread_mutex_unlock(&mutex);

					ranks_dispatched |= (1 << rank_id);
				}
				rank_id++;
			}
		}
		gettimeofday(&x4, NULL);
		printf("%.5lf %.5lf %.5lf\n", timediff(&x1,&x2), timediff(&x2,&x3), timediff(&x3,&x4));
	}	

	return NULL;
}


/*************************************************/
/*                Public Functions               */
/*************************************************/

int pim_init(void) {
	// Allocate all DPUs, then check how many were allocated
	DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL, NULL, &dpus));
	
	dpu_get_nr_ranks(dpus, &num_ranks);
	dpu_get_nr_dpus(dpus, &num_dpus);
	total_request_slots = num_dpus * NR_TASKLETS;

	// Load the program to all DPUs
	DPU_ASSERT(dpu_load(dpus, DPU_PROGRAM, NULL));

	// Create the DPU master host thread
	args.stop_thread = 0;
	args.req_head = 0;
	args.req_tail = 0;
	args.req_count = 0;
	args.req_waiting = 0;
	args.caller_args = (caller_args_t **)malloc(sizeof(caller_args_t *) * total_request_slots);
	
	if (pthread_create(&dpu_master_thread, NULL, dpu_uncompress, &args) != 0) {
		fprintf(stderr, "Failed to create dpu_decompress pthreads\n");
		return -1;
	}

	// Create mutex
	if (pthread_mutex_init(&mutex, NULL) != 0) {
		fprintf(stderr, "Failed to create mutex\n");
		return -1;
	}

	// Create condition variables for both directions
	if (pthread_cond_init(&caller_cond, NULL) != 0) {
		fprintf(stderr, "Faled to create calller condition variable\n");
		return -1;
	}
	if (pthread_cond_init(&dpu_cond, NULL) != 0) {
		fprintf(stderr, "Faled to create dpu condition variable\n");
		return -1;
	}

	return 0;
}

void pim_deinit(void) {
	// Signal to terminate the dpu master thread
	pthread_mutex_lock(&mutex);
	args.stop_thread = 1;
	pthread_mutex_unlock(&mutex);
	pthread_cond_broadcast(&dpu_cond);

	pthread_join(dpu_master_thread, NULL);

	// Free the DPUs
	DPU_ASSERT(dpu_free(dpus));
	num_ranks = 0;
	num_dpus = 0;

	// Free all the allocated memory
	free(args.caller_args);

	// Destroy the mutex
	pthread_mutex_destroy(&mutex);

	// Destroy the condition variables
	pthread_cond_destroy(&caller_cond);
	pthread_cond_destroy(&dpu_cond);
}

int pim_decompress(const char *compressed, size_t compressed_length, char *uncompressed) {
	// Set up in the input and output buffers
	host_buffer_context_t input = {
		.buffer = (char *)compressed,
		.curr   = (char *)compressed,
		.length = compressed_length
	};

	host_buffer_context_t output = {
		.buffer = uncompressed,
		.curr   = uncompressed,
		.length = 0
	};

	// Read the decompressed length
	if (!read_varint32(&input, &(output.length))) {
		fprintf(stderr, "Failed to read decompressed length\n");
		return false;
	}

	// Set up the caller arguments
	caller_args_t m_args = {
		.data_ready = 1,
		.input = &input,
		.output = &output,
		.retval = 0
	};	
	
	pthread_mutex_lock(&mutex);

	// Wait until there is space to take in more requests
	while (args.req_count == total_request_slots) {
		pthread_cond_wait(&caller_cond, &mutex);
	}
	
	args.caller_args[args.req_head] = &m_args;
	args.req_head = (args.req_head + 1) % total_request_slots;
	args.req_count++;
	args.req_waiting++;
	pthread_cond_broadcast(&dpu_cond);

	// Wait for request to be processed
	while (m_args.data_ready != 0) {
		pthread_cond_wait(&caller_cond, &mutex);
	}

	pthread_mutex_unlock(&mutex);
	return m_args.retval;
}
