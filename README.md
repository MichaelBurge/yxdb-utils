# yxdb-utils
Utilities for parsing Alteryx Database format

## Yxdb2Csv.hs
Usage: ```yxdb2csv [OPTIONS...] <input_file>```
```
example 1: yxdb2csv alteryx_db.yxdb > output.psv  # output to pipe-delimited flat file
example 2: yxdb2csv alteryx_db.yxdb | gzip -f > output_zipped.psv.gz  # pipe to compression tool, forced overwrite
example 3: yxdb2csv alteryx_db.yxdb | head  # from stdout pipe to head 
```
```
Options:
-m, --metadata: retrieve file metadata only
-b, --block: output _x_ blocks of data
-r, --num-records: output _x_ number of records, per block if -b is given
-v, --verbose: extra debugging messages to stderr
-d, --decompress-blocks: decompress blocks without interpreting
```

## Csv2Yxdb.hs
Input data in flat pipe-delimited format (UTF-8 encoded)

Usage: ```csv2yxdb my_flat_file.psv -o my_alteryx_db.yxdb```

```
example: csv2yxdb input.csv -o example.yxdb  # input a pipe-delimited flat file (not comma-delimited)
```
```
Options:
-o, --output: output file path
-h, --header: user input header line for output file
-m, --metadata: retrieve file metadata only
-i, --internal: dump external representation of parsed records
-v, --verbose: extra debugging messages to stderr
```
## Limitations:
* csv2yxdb does not support vstring or vwstring types. You can use fixed-length strings instead.
* Neither tool supports the 'blob', 'spatial index', or 'unknown' types
* These tools only support YXDB and CSV. In particular, there are no tools for Calgary database files.
