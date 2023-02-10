# data_format_and_optimization

In this repo, we test different data format. And recommend different data format for different use cases


Data format: 
- avro (structured)
- csv (semi-structured)
- json (semi-structured)
- orc (structured)
- parquet (structured)
- [lance](https://github.com/eto-ai/lance) (To be tested)

We evaluate the data formats by measuring the latency of the following data operations :

- Disk usage
- Read/Write latency
- Random data lookup
- Filtering/GroupBy(column-wise)
- Distinct(row-wise)
