# LUMINER: A Customizable and Easy-to-Use Pipeline for Deep Learning- and Dictionary-based Named Entity Recognition from Medical Text

LUMINER is a customizable End-to-End Information Retrieval Pipeline developed at Lund University for extracting named entities from medcine-related texts. The pipeline comes with pre-trained models and dictionaries that can retrieve many biomedical entities: cell-lines, chemicals, disease, gene/proteins, species, COVID-19 related terms.  

![](tutorials/imgs/pipeline3.png)

## Quick start guide

1. Before installation: Downnload and install anaconda from https://www.anaconda.com/


2. Clone the repository to your target folder


```console
git clone https://github.com/Aitslab/LUMINER

```

3. Set up an conda environment

```console
conda env create -f environment.yml
```

4. After installation activate the environment:
```console
conda activate env_pipeline
```

5. Provide documents in the form of PubMed IDs, CORD metadata, or through free text.


6. Edit the [config file](config.json) with correct paths to your documents.


7. Run the following command:

```python
python main.py
```

8. Th output will be like [this](results/sample_output/analysis_mtorandtsc1_chemical/)
___


### The complete configuration and inference tutorial can be found in this [collection of tutorials](tutorials/Tutorial-pipeline.md)  
#### For BioBERT model training script follow this [tutorial](tutorials/Tutorial-BioBERT_model_training.ipynb)
#### All preprocessing scripts can be found [here](supplementary/preprocessing_scripts/)
