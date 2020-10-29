#ifndef _PIM_SNAPPY_H_
#define _PIM_SNAPPY_H_

#ifdef __cplusplus
	extern "C" {
#endif

		int pim_decompress(const char *compressed, size_t compressed_length, char *uncompressed);
#ifdef __cplusplus
	}
#endif

#endif	/* _PIM_SNAPPY_H_ */

