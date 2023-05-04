# data_format_and_optimization

In this repo, we test and evaluate different data format. And recommend different data format for different use cases:

Based on different use cases, we divide data formats into three categories:
- Short term or ephemeral storage
- Mid, long term storage
- Archive storage data format (We don't evaluate data formats of this category in this repo. For more information, please
visit the [archive file format](https://en.wikipedia.org/wiki/Archive_file))

## Short term or ephemeral storage data format
- Arrow (IPC, Feather)


## Long term storage data format: 
- avro (structured)
- csv (semi-structured)
- json (semi-structured)
- orc (structured)
- parquet (structured) More details can be found [here](https://github.com/pengfei99/ParquetDataFormat.git)
- [lance](https://github.com/eto-ai/lance) (To be tested)

We evaluate the data formats by measuring the latency of the following data operations :

- Disk usage
- Read/Write latency
- Random data lookup
- Filtering/GroupBy(column-wise)
- Distinct(row-wise)
