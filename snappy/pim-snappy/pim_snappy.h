#ifndef _PIM_SNAPPY_H_
#define _PIM_SNAPPY_H_

#ifdef __cplusplus
	extern "C" {
#endif
		/**
		 * Initialize the PIM-assisted Snappy decompressor. Allocates all DPUs, creates the DPU handler
		 * thread and the request buffer.
		 *
		 * @returns 0 if initialization was successful, -1 if there was an error
		 */
		int pim_init(void);

		/**
		 * Deinitialize the PIM-assisted Snappy decompressor. Deallocates all DPUs and frees all dynamic 
		 * memory.
		 */
		void pim_deinit(void);

		/**
		 * Performs Snappy decompression using PIM by submitting a request to the DPU handler thread
		 * and waiting for the data to be processed and returned.
		 *
		 * @param compressed: pointer to the compressed data stream
		 * @param compressed_length: length in bytes of the compressed data stream
		 * @param uncompressed: pointer to where the decompressed data stream should be stored
		 * @returns 1 if successful, 0 if there was an error
		 */
		int pim_decompress(const char *compressed, size_t compressed_length, char *uncompressed);
#ifdef __cplusplus
	}
#endif

#endif	/* _PIM_SNAPPY_H_ */

