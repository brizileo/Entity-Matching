### 🔍 Entity Matching

This application is used to perform entity matching. It uses a hybrid approach where traditional similarity metrics are combine with GenAI techniques.

This application takes as input a file with a list of entities to be matched and assign them to clusters based on their similarity.

The input file contains a “partition” column used to reduce the computational complexity of the $n^2$ pairwise comparisons of all entities. 

The application first identifies potential candidate pairs using pairwise Jaccard identity based on matching tokens. Token matching is softened using the Jaro-Winkler similarity to account for potential mispelling and typos:

$$
s_{a,b} = \frac{Z}{|a| + |b| - Z}, \quad \text{with}
$$

$$
Z = \frac{
  \sum_{i=1}^{|a|} \max_{j=1}^{|b|} (w_{a_i, b_j}) +
  \sum_{j=1}^{|b|} \max_{i=1}^{|a|} (w_{a_i, b_j})
}{2}
$$

where quantities $|a|$ and $|b|$ represent the number of tokens in strings a and b, respectively, the quantity $Z$ captures the similarity between tokens from string $a$ matching in string $b$, and from string $b$ matching in string $a$ where $w_{a_i, b_j}$ is the Jaro-Winkler similarity. 

Each candidate pair is then reviewed by a locally running GenAI model to assess whether the match is correct. 
Once the matches are verified by the GenAI model, entities are clustered via a connected components algorithm, which is currently under development.

### 🛠️ Setup

Clone this repository and run `poetry install` to setup the virtual environment and `poetry shell` to activate it.

The application requires Ollama and the Phi4-mini model. You can download Ollama from [https://ollama.com](https://ollama.com) and then run `ollama pull phi4-mini` to obtain the Phi4-mini model.

Start the Ollama server to enable API interaction: `ollama serve`.  
Note: Ensure that you have administrative privileges and that the Ollama tool is properly installed before running this command.
Verify Ollama is running: `curl http://localhost:11434` should return `Ollama is running`.  
If the command does not return the expected output, ensure that the Ollama server is running by executing `ollama serve` in a terminal.  
Additionally, check that no firewall or network restrictions are blocking access to port 11434.

Place you input .csv file in the `data_in`directory. Read the instruction in the `.gitkeep` for more details. 

### 📄 References

[Combining Multiple String Similarity Metrics for Effective Toponym Matching](https://eprints.lancs.ac.uk/id/eprint/89481/1/Manusc_Combining_Multiple_String_Similarity_Metrics_for_Effective_Toponym_Matching.pdf)  
A. G. Reis, J. C. Freitas, A. M. Silva, F. Moura-Pires. *Combining Multiple String Similarity Metrics for Effective Toponym Matching*. Lancaster University ePrints, 2017.
