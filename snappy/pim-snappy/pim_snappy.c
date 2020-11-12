#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include <getopt.h>

#include <dpu.h>
#include <dpu_memory.h>
#include <dpu_log.h>

#include "pim_snappy.h"
#include "PIM-common/common/include/common.h"

#define GET_ELEMENT_TYPE(_tag)  (_tag & BITMASK(2))
#define GET_LENGTH_1_BYTE(_tag) ((_tag >> 2) & BITMASK(3))
#define GET_OFFSET_1_BYTE(_tag) ((_tag >> 5) & BITMASK(3))
#define GET_LENGTH_2_BYTE(_tag) ((_tag >> 2) & BITMASK(6))

#define ALIGN_LONG(_p, _width) (((long)_p + (_width-1)) & (0-_width))

// Snappy tag types
enum element_type
{
    EL_TYPE_LITERAL,
    EL_TYPE_COPY_1,
    EL_TYPE_COPY_2,
    EL_TYPE_COPY_4
};

// Buffer context struct for input and output buffers on host
typedef struct host_buffer_context
{
    uint8_t *buffer;        // Entire buffer
    uint8_t *curr;          // Pointer to current location in buffer
    uint32_t length;       // Length of buffer
} host_buffer_context_t;

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
 * Read an unsigned integer from the input buffer. Increments
 * the current location in the input buffer.
 *
 * @param input: holds input buffer information
 * @return Unsigned integer read
 */
static uint32_t read_uint32(struct host_buffer_context *input)
{
    uint32_t val = 0;
    for (uint8_t i = 0; i < sizeof(uint32_t); i++) {
        val |= (*input->curr++) << (8 * i); 
    }   

    return val;
}

/**
 * Read the size of the long literal tag, which is used for literals with
 * length greater than 60 bytes.
 *
 * @param input: holds input buffer information
 * @param len: length in bytes of the size to read
 * @return 0 if we reached the end of input buffer, size of literal otherwise
 */
static inline uint32_t read_long_literal_size(struct host_buffer_context *input, uint32_t len)
{
    if ((input->curr + len) >= (input->buffer + input->length))
        return 0;

    uint32_t size = 0;
    for (uint32_t i = 0; i < len; i++) {
        size |= (*input->curr++ << (i << 3));
    }
    return size;
}

/**
 * Read a 1-byte offset tag and return the offset of the copy that is read.
 *
 * @param tag: tag byte to parse
 * @param input: holds input buffer information
 * @return 0 if we reached the end of input buffer, offset of the copy otherwise
 */
static inline uint16_t make_offset_1_byte(uint8_t tag, struct host_buffer_context *input)
{
    if (input->curr >= (input->buffer + input->length))
        return 0;
    return (uint16_t)(*input->curr++) | (uint16_t)(GET_OFFSET_1_BYTE(tag) << 8);
}

/**
 * Read a 2-byte offset tag and return the offset of the copy that is read.
 *
 * @param tag: tag byte to parse
 * @param input: holds input buffer information
 * @return 0 if we reached the end of input buffer, offset of the copy otherwise
 */
static inline uint16_t make_offset_2_byte(uint8_t tag, struct host_buffer_context *input)
{
    UNUSED(tag);

    uint16_t total = 0;
    if ((input->curr + sizeof(uint16_t)) > (input->buffer + input->length))
        return 0;
    else {
        total = (*input->curr & 0xFF) | ((*(input->curr + 1) & 0xFF) << 8);
        input->curr += sizeof(uint16_t);
        return total;
    }
}

/**
 * Read a 4-byte offset tag and return the offset of the copy that is read.
 *
 * @param tag: tag byte to parse
 * @param input: holds input buffer information
 * @return 0 if we reached the end of input buffer, offset of the copy otherwise
 */
static inline uint32_t make_offset_4_byte(uint8_t tag, struct host_buffer_context *input)
{
    UNUSED(tag);
    uint32_t total = 0;
    if ((input->curr + sizeof(uint32_t)) > (input->buffer + input->length))
        return 0;
    else {
        total = (*input->curr & 0xFF) |
                ((*(input->curr + 1) & 0xFF) << 8) |
                ((*(input->curr + 2) & 0xFF) << 16) |
             ((*(input->curr + 3) & 0xFF) << 24);
        input->curr += sizeof(uint32_t);
        return total;
    }
}

/**
 * Copy and append data from the input bufer to the output buffer.
 *
 * @param input: holds input buffer information
 * @param output: holds output buffer information
 * @param len: length of data to copy over
 */
static void writer_append_host(struct host_buffer_context *input, struct host_buffer_context *output, uint32_t len)
{
    //printf("Writing %u bytes at 0x%x\n", len, (input->curr - input->buffer));
    while (len &&
        (input->curr < (input->buffer + input->length)) &&
        (output->curr < (output->buffer + output->length)))
    {
        *output->curr = *input->curr;
        input->curr++;
        output->curr++;
        len--;
    }
}

/**
 * Copy and append previously uncompressed data to the output buffer.
 *
 * @param output: holds output buffer information
 * @param copy_length: length of data to copy over
 * @param offset: where to copy from, offset from current output pointer
 * @return False if offset if invalid, True otherwise
 */
static bool write_copy_host(struct host_buffer_context *output, uint32_t copy_length, uint32_t offset)
{
    //printf("Copying %u bytes from offset=0x%lx to 0x%lx\n", copy_length, (output->curr - output->buffer) - offset, output->curr - output->buffer);
    const uint32_t diff = output->curr - output->buffer;
    if (offset > diff)
    {
        printf("bad offset! %u %u\n", offset, diff);
        return false;
    }
    while (copy_length &&
        output->curr < (output->buffer + output->length))
    {
        *output->curr = *(output->curr - offset);
        output->curr++;
        copy_length -= 1;
    }

    return true;
}

/**
 * Perform the Snappy decompression on the host.
 *
 * @param input: holds input buffer information
 * @param output: holds output buffer information
 * @return 1 if successful, 0 otherwise
 */
int host_uncompress(host_buffer_context_t *input, host_buffer_context_t *output) {
	// Read the decompressed block size
//	uint32_t dblock_size;
//	if (!read_varint32(input, &dblock_size)) {
//		fprintf(stderr, "Failed to read decompressed block size\n");
//		return false;
//	}
	uint8_t*  input_end = input->buffer + input->length;	
	while (input->curr < input_end) {
		// Read the compressed block size
//		uint32_t compressed_size = read_uint32(input);	
//		uint8_t *block_end = input->curr + compressed_size;
	
//		while (input->curr != block_end) {	
			uint32_t length;
			uint32_t offset;
			const uint8_t tag = *input->curr++;

			/* There are two types of elements in a Snappy stream: Literals and
			copies (backreferences). Each element starts with a tag byte,
			and the lower two bits of this tag byte signal what type of element
			will follow. */
			switch (GET_ELEMENT_TYPE(tag))
			{
			case EL_TYPE_LITERAL:
				/* For literals up to and including 60 bytes in length, the upper
				 * six bits of the tag byte contain (len-1). The literal follows
				 * immediately thereafter in the bytestream.
				 */
				length = GET_LENGTH_2_BYTE(tag) + 1;
				if (length > 60)
				{
					length = read_long_literal_size(input, length - 60) + 1;
				}

				writer_append_host(input, output, length);
				break;

			/* Copies are references back into previous decompressed data, telling
			 * the decompressor to reuse data it has previously decoded.
			 * They encode two values: The _offset_, saying how many bytes back
			 * from the current position to read, and the _length_, how many bytes
			 * to copy.
			 */
			case EL_TYPE_COPY_1:
				length = GET_LENGTH_1_BYTE(tag) + 4;
				offset = make_offset_1_byte(tag, input);
				if (!write_copy_host(output, length, offset))
					return false;
				break;

			case EL_TYPE_COPY_2:
				length = GET_LENGTH_2_BYTE(tag) + 1;
				offset = make_offset_2_byte(tag, input);
				if (!write_copy_host(output, length, offset))
					return false;
				break;

			case EL_TYPE_COPY_4:
				length = GET_LENGTH_2_BYTE(tag) + 1;
				offset = make_offset_4_byte(tag, input);
				if (!write_copy_host(output, length, offset))
					return false;
				break;
			}
		}
//	}

	return true;
}

/**
 * Perform the Snappy decompression on the DPU.
 *
 * @param input: holds input buffer information
 * @param output: holds output buffer information
 * @return 1 if successful, 0 otherwise
 */
static int dpu_uncompress(host_buffer_context_t *input, host_buffer_context_t *output) {
	// Allocate the DPUs
    struct dpu_set_t dpus;
    struct dpu_set_t dpu_rank;
    struct dpu_set_t dpu;
    DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &dpus));

    DPU_ASSERT(dpu_load(dpus, DPU_PROGRAM, NULL));

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

    DPU_ASSERT(dpu_free(dpus));

    return true;
}

int pim_decompress(const char *compressed, size_t compressed_length, char *uncompressed) {
	// Setup input and output buffer contexts
	host_buffer_context_t input = {
		.buffer = (uint8_t *)compressed,
		.curr   = (uint8_t *)compressed,
		.length = compressed_length
	};

	host_buffer_context_t output = {
		.buffer = (uint8_t *)uncompressed,
		.curr   = (uint8_t *)uncompressed,
		.length = 0
	};

	// Read the decompressed length
	if (!read_varint32(&input, &(output.length))) {
		fprintf(stderr, "Failed to read decompressed length\n");
		return false;
	}

	return dpu_uncompress(&input, &output);
}
