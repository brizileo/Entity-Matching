### Decription

This application takes as input a file with a list of entities to be matched and return the same list enriched with a Cluster ID regrouping entities based on their similarity.

The input file contains a “partition” column used to mitigate the n^2 dependency of the pairwise comparison of all entities. 

The application first identifies potential candidate pairs using pairwise Jaccard identity based on matching tokens. Token matching is soften using the Jaro Winkler similarity to account for mispelling and typos:

$$
s_{a,b} = \frac{Z}{|a| + |b| - Z}, \quad \text{with}
$$

$$
Z = \frac{
  \sum_{i=1}^{|a|} \max_{j=1}^{|b|} (w_{a_i, b_j}) +
  \sum_{j=1}^{|b|} \max_{i=1}^{|a|} (w_{a_i, b_j})
}{2}
$$

where quantities |a| and |b| represent the number of tokens in strings a and b, respectively and the quantity Z captures the similarity between tokens from string a matching in string b, and from string b matching in string a. 

Each candidate pair is then reviewed by a Language Model to assess whether the match is correct. 
Once the matches are verified, entites are clustered via a connected components algorithm (WIP)

### 📄 References

[Combining Multiple String Similarity Metrics for Effective Toponym Matching](https://eprints.lancs.ac.uk/id/eprint/89481/1/Manusc_Combining_Multiple_String_Similarity_Metrics_for_Effective_Toponym_Matching.pdf)  
A. G. Reis, J. C. Freitas, A. M. Silva, F. Moura-Pires. *Combining Multiple String Similarity Metrics for Effective Toponym Matching*. Lancaster University ePrints, 2017.
