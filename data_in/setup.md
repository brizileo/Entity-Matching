🛠️ Place your input filess in csv format in this folder.

The module `load_entities_from_csv()` will load the files in the DuckDB database.

The target table is `tbl_entities (entity_id, entity_name, partition_criteria, cluster_id)`

The default format assumed for the .csv files is the following:

<pre>
entity_id   INT  
name        VARCHAR  
surname     VARCHAR  
address     VARCHAR  
postcode    VARCHAR
</pre>

The loading module concatenate the following attributes: 
<pre>
entity_name = name + surname + address + postcode
</pre>

The partion_criteria is defaulted to `partition1`

‼️ Edit the loading module to adjust to your specific format.  
If you have a different partition, provide it as an additional field in the .csv file and ensure the loading module handles it correctly.
