#include "orc/OrcFile.hh"

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
	RowReaderOptions rowReaderOpts;
    std::list<std::string> columns_to_read({"ss_sold_date_sk"});
	rowReaderOpts.include(columns_to_read);
	ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOpts);

	ORC_UNIQUE_PTR<ColumnVectorBatch> batch = rowReader->createRowBatch(reader->getRowIndexStride());

	StructVectorBatch *root = dynamic_cast<StructVectorBatch *>(batch.get());
	LongVectorBatch *ss_sold_date_sk = NULL;
	
	// Get the column
	const Type& rowReaderType = rowReader->getSelectedType();
	for (uint64_t c = 0; c < rowReaderType.getSubtypeCount(); c++) {
		if (!root->notNull[c])
			continue;
		
		if (rowReaderType.getFieldName(c).compare("ss_sold_date_sk") == 0) {
			ss_sold_date_sk = dynamic_cast<LongVectorBatch *>(root->fields[c]);
		}
	}	
	
	// Read through the rows to find the desired column
	std::vector<bool> sel_col = rowReader->getSelectedColumns();
	const Type& readerType = reader->getType();
	for (uint64_t i = 1; i < sel_col.size(); i++) {
		if (sel_col[i])
			std::cout << readerType.getFieldName(i - 1) << ": " << ss_sold_date_sk->data[i] << "\n";
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
