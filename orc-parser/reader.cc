#include "orc/OrcFile.hh"
#include "orc/ColumnPrinter.hh"
#include "orc/Statistics.hh"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <iostream>

using namespace orc;

void read(char *filename) {
	// Read in the file as a stream
	ORC_UNIQUE_PTR<InputStream> inStream = readLocalFile(filename);

	// Allocate the ORC reader
	ReaderOptions readerOpts;
	ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), readerOpts);

	// Allocate the row reader	
	RowReaderOptions rowReaderOptions;
	ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOptions);
	ORC_UNIQUE_PTR<ColumnVectorBatch> batch = rowReader->createRowBatch(reader->getRowIndexStride());

	StructVectorBatch *root = dynamic_cast<StructVectorBatch *>(batch.get());
	LongVectorBatch *first_col = dynamic_cast<LongVectorBatch *>(root->fields[0]); // Get first column

	// Loop through and read each row	
	uint64_t rows = 0;
	uint64_t batches = 0;
	uint64_t first_col_sum = 0;
	while (rowReader->next(*batch)) {
		batches++;
		rows += batch->numElements;

		for (uint64_t elem = 0; elem < batch->numElements; elem++) {
			if (first_col->notNull[elem]) {
				first_col_sum += first_col->data[elem]; }
		}		
	}
	std::cout << "Rows: " << rows << "\n";
	std::cout << "Batches: " << batches << "\n";
	std::cout << "Sum first col: " << first_col_sum << "\n";
}

int main(int argc, char *argv[]) {
	int opt;
	char *input_file = NULL;

	while ((opt = getopt(argc, argv, "f:")) != -1) {
		switch(opt) {
			case 'f':
				input_file = optarg;
				break;
			default:
				std::cout << "Unknown Option: " << optopt << "\n";
				exit(1);
		}
	}

	if (input_file == NULL) {
		std::cout << "Specify an input file with -f\n";
		exit(1);
	}

	read(input_file);
	return 0;
}
