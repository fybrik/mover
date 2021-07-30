## Mover matrix

Different types of data can be moved in different ways  e.g. in stream or batch mode.
How the mover interpret these different modes depends on the nature of source to be read from and the target to be written to.
To facilitate this the following describes a matrix of possible operations and if they are supported or not. The source/target of the data
(thus e.g. Kafka or COS) plays a huge role over what default assumptions about the below described parameters are for a given system. 
In the end the system specific implementation has to 
decide which cases are supported. The matrix below tries to be as general as possible.

The first configurable parameter is the data flow type which can be:

- **batch** Batch is a one-of-transfer that copies a snapshot of the data from the source. 
- **stream**  Stream is an continuously running job that interprets event to update the taget

Two data types are supported:
- **log data**: This is structured data with no special interpretation. (e.g. rows of a CSV file or the values of a [KStream](https://docs.confluent.io/current/streams/index.html)
- **change data**: This represents data with a representation of a changing data set. It requires a `key` and a `value` struct being present. This is mostly known
from e.g. a [KTable](https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/)  that can represent a CDC stream. If other data sources than Kafka are used a `key` and a `value` field could be present or other mappings could be defined to 
represent data of type *change data*.

Data types can be defined for source and for target data.

The last parameter is the write operation. Here we have the following:
- **overwrite**: Completely overwrite the data at the target location. (Only for batch)
- **append**: Append the data to the already existing data at the target location
- **update**: Update the values in the target location according to change stream (only for change data)

#### Matrix for batch data flow

The meaning of the batch data flow is dependent on the the source data type and the write operation. It is *NOT* dependent on the target data type.
| **Write operation** | **Source data type: Log data (KStream)** | **Source data type: Change data (KTable)** |
|---|---|---| 
| Overwrite | Copy of log data (stored as normal rows). Overwrite any existing table/object of the same name. | Data is interpreted as changes and used to "compute" a table. Snapshot of data (stored as normal rows), will overwrite any existing table of the same name. |
| Append | Append the entire content of the log data to already existing data (stored as normal rows). Use case: aggregate several source data into one target or if source is repeatedly deleted then it can be aggregated over time. | doesn't make sense |
| Update | doesn't make sense as there is no key | Read the data verbatim (no interpretation) and update the target data by applying the changes to the existing target table. |


The meaning of the stream data flow is dependent on the the source data type and the write operation *AND* is also dependent on the target data type.

#### Matrix for stream data flow

| **Write operation (into log data)** | **Source data type: Log data (KStream)** | **Source data type: Change data (KTable)** |
|---|---|---| 
| Overwrite(log data) | doesn't make sense as stream => use batch | doesn't make sense as stream => use batch |
| Append(log data) | Read log data and write log data in an append mode to a target (examples: write logs from Kafka to COS as archive, or read log files from COS) this writes the full Kafka data (including key, value, timestamp, etc...) | write only value part as output. This is to be used when e.g. writing data from Kafka to COS without having the key/value structure |
| Update(log data) | doesn't make sense as there is no key | "Confluent" use case: read verbatim (key / value) and interpret during write: insert, update and delete records as needed in the target. Requires that target supports these operations. | 

| **Write operation (into change data)** | **Source data type: Log data (KStream)** | **Source data type: Change data (KTable)** |
|---|---|---| 
| Overwrite(change data) | doesn't make sense as stream => use batch | doesn't make sense as stream => use batch |
| Append(change data) | doesn't make sense. Can't go from log data to change data | Read verbatim (key / value) and write verbatim (key /value) Use case: Read CDC stream, apply transformations (anonymizations, redactions) and write CDC stream out to target. If no transformations are applied this is like MirrorMaker |
| Update(change data) | Cannot be supported without primary key. Not planned for now. | doesn't make sense | 


#### Example configurations

##### Dump json data from Kafka to COS

* data flow type: Stream
* source: Kafka
* target: COS
* write operation: Append
* source data type: Change data
* target data type: Log data
* Data in Kafka: Keys are null and values contain JSON records

=> Result: Will write JSON record of value into COS. Thus no Kafka internal info or keys are written to COS

#### Do a batch snapshot of a KTable to COS

* data flow type: Batch
* source: Kafka
* target: COS
* write operation: Overwrite
* source data type: Change data
* target data type: Log data
* Data in Kafka: Keys are keys of change data and values contain the full records (in avro)

=> Result: Will write only the latest values to COS. (Grouping by key first to sort out earlier updates)