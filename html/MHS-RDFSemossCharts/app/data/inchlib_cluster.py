import inchlib_clust

#instantiate the Cluster object
c = inchlib_clust.Cluster()

# read csv data file with specified delimiter, also specify whether there is a header row, the type of the data (numeric/binary) and the string representation of missing/unknown values
c.read_csv(filename="clusterData.csv", delimiter=",", header=bool, missing_value=False, datatype="numeric/binary")
# c.read_data(data, header=bool, missing_value=str/False, datatype="numeric/binary") use read_data() for list of lists instead of a data file

# normalize data to (0,1) scale, but after clustering write the original data to the heatmap
c.normalize_data(feature_range=(0,1), write_original=bool)

# cluster data according to the parameters
c.cluster_data(row_distance="hamming", row_linkage="single", axis="row", column_distance="euclidean", column_linkage="ward")

# instantiate the Dendrogram class with the Cluster instance as an input
d = inchlib_clust.Dendrogram(c)

# create the cluster heatmap representation and define whether you want to compress the data by defining the maximum number of heatmap rows, the resulted value of compressed (merged) rows and whether you want to write the features
d.create_cluster_heatmap(compress=int, compressed_value="median", write_data=bool)

# read metadata file with specified delimiter, also specify whether there is a header row
d.add_metadata_from_file(metadata_file="clusterMetaData.csv", delimiter=",", header=bool, metadata_compressed_value="frequency")

# read column metadata file with specified delimiter, also specify whether there is a 'header' column
d.add_column_metadata_from_file(column_metadata_file="clusterColumnMetaData.csv", delimiter=",", header=bool)

# export the cluster heatmap on the standard output or to the file if filename specified
d.export_cluster_heatmap_as_json("inchlibTest.json")
#d.export_cluster_heatmap_as_html("/path/to/directory") function exports simple HTML page with embedded cluster heatmap and dependencies to given directory 