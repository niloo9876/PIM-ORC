import os
import sys
import argparse

bins = {"Initialization" : 0,
		"ColumnReader" : 0,
		"Snappy Decompression" : 0,
		"RLE Decoder" : 0,
		"Deinitialization" : 0,
		"Other" : 0}

with open(path, "r") as f:
	while (line = f.readline()):
		line_delim = line.split(" ")
		count = line_delim[1]
		
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

total_sum = sum(bins.values())
for k, v in bins.iteritems():
	print(k, " ", v / total_sum)
