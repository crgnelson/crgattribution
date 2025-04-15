# Use Anaconda base image (includes conda and all Anaconda packages)
FROM continuumio/anaconda3

# Set the working directory inside the container
WORKDIR /app

# Copy the local script file (main.py) into the container as /app/script.py
COPY main.py /app/script.py

# Create a new conda environment with necessary dependencies
RUN conda create -n myenv python=3.9 -y && \
    /bin/bash -c "source ~/.bashrc && conda activate myenv && \
    conda install -n myenv pandas=1.3.3 matplotlib=3.4.3 seaborn=0.11.2 numpy tqdm -y && \
    pip install google-cloud-bigquery pandas-gbq markovclick marketing_attribution_models networkx graphviz && \
    conda clean --all -y"

# Set the PATH environment variable to use the new conda environment
ENV PATH /opt/conda/envs/myenv/bin:$PATH

# Set the default command to run your script in the conda environment
CMD ["bash", "-c", "source activate myenv && python /app/script.py"]