import os
import sys
import argparse

bins = {"Initialization" : 0,
		"ColumnReader" : 0,
		"Snappy Decompression" : 0,
		"RLE Decoder" : 0,
		"Deinitialization" : 0,
		"Other" : 0}

parser = argparse.ArgumentParser(description='Bin runtimes of different parts of ORC parsing program')
requiredArgs = parser.add_argument_group('required arguments')
requiredArgs.add_argument('PATH', help='Output file of stackcollapse-perf.pl')

args = parser.parse_args()
path = args.PATH

with open(path, "r") as f:
        line = f.readline()
	while (line):
		line_delim = line.split(" ")
		count = int(line_delim[-1])
		
		if "orc::SnappyDecompressionStream::decompress" in line:
			bins["Snappy Decompression"] += count
		elif "orc::RleDecoderV2::next" in line:
			bins["RLE Decoder"] += count
		elif "ColumnReader::next" in line:
			bins["ColumnReader"] += count
		elif "~" in line:
			bins["Deinitialization"] += count
		elif "protobuf" in line:
			bins["Initialization"] += count
		else:
			bins["Other"] += count

                line = f.readline()

total_sum = sum(bins.values())
for k, v in bins.iteritems():
    print(k, "{:.2f}".format(float(v) / total_sum * 100))
