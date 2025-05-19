This application takes an input a file with a list of entities and return the same list enriched with a Cluster ID regrouping entities based on their similarity.
The input file should contain a “partition” column. The partition column is used to mitigate the n^2 dependency of the pairwise comparison of all entities. 
The application first identifies potential candidate pairs using pairwise Jaccard identity based on matching tokens.
Each candidate pair is then reviewed by a Language Model to assess whether the match is correct. 
Once the matches are verified, entites are clustered via a connected components algorithm.