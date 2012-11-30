sudo cassandra-cli -host localhost

# Create keyspace - uncomment if keyspace OSN does not exist
#create keyspace OSN;

# Authenticate keyspace
use OSN;

# Create ColumnFamily: Master 
# Master i exists in server j
CREATE COLUMN FAMILY Master WITH comparator = 'UTF8Type'
with column_metadata =
        [
        {column_name: 'server', validation_class: 'LongType', index_type: KEYS},
        ]; 

# Create ColumnFamily: Replica
# Replica of master node_id exists in servers i,j,k,...
CREATE COLUMN FAMILY Replica WITH comparator = 'UTF8Type'
with column_metadata =
        [
	{column_name: 'node_id', validation_class: 'LongType', index_type: KEYS}
        {column_name: 'server', validation_class: 'LongType', index_type: KEYS},
        ]; 

# Create ColumnFamily: Edge
# Edge i has two endpoint: u and v
CREATE COLUMN FAMILY Edge WITH comparator = 'UTF8Type'
with column_metadata =
        [
	{column_name: 'u', validation_class: 'LongType', index_type: KEYS}
	{column_name: 'v', validation_class: 'LongType', index_type: KEYS}
        ]; 

# Create ColumnFamily: Master_Master_Edge
# master1 and master2 are connected in the same server i
CREATE COLUMN FAMILY Master_Master_Edge WITH comparator = 'UTF8Type'
with column_metadata =
        [
	{column_name: 'master1', validation_class: 'LongType', index_type: KEYS}
	{column_name: 'master2', validation_class: 'LongType', index_type: KEYS},
	{column_name: 'server', validation_class: 'LongType', index_type: KEYS},
        ]; 

# Create ColumnFamily: Master_Slave_Edge
# master1 and slave are connected in the same server i
CREATE COLUMN FAMILY Master_Slave_Edge WITH comparator = 'UTF8Type'
with column_metadata =
        [
	{column_name: 'master', validation_class: 'LongType', index_type: KEYS}
	{column_name: 'slave', validation_class: 'LongType', index_type: KEYS},
	{column_name: 'server', validation_class: 'LongType', index_type: KEYS},
        ]; 

