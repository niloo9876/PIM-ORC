#include "orc/OrcFile.hh"
#include "orc/ColumnPrinter.hh"

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

	// Allocate the column printer
	std::string line;
	ORC_UNIQUE_PTR<ColumnPrinter> columnPrinter = createColumnPrinter(line, &(reader->getType()));

	// Loop through and read each row	
	while (rowReader->next(*batch)) {
		columnPrinter->reset(*batch);
		for (uint64_t i = 0; i < batch->numElements; i++) {
			line.clear();
			columnPrinter->printRow(i);
			std::cout << line << "\n";
		}
	}
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
