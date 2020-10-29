# Scripts

## Calculating Runtime Breakdown of ORC Reader
The method involves the use of `perf` to first profile the program, then utilizes the scripts in this repository to report the values.

```bash
cd ../orc-parser
perf record -g --call-graph dwarf -- ./reader -f [TESTFILE]
perf script | ../scripts/stackcollapse-perf.pl > out.perf-folded
python ../scripts/parse_stackcollapse.py out.perf-folded
```  
