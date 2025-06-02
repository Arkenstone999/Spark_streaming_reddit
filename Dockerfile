FROM jupyter/pyspark-notebook:spark-3.4.1

USER root
RUN pip install praw textblob && \
    python -m textblob.download_corpora

USER ${NB_UID}
WORKDIR /home/jovyan/work

COPY . /home/jovyan/work

EXPOSE 8888 9998
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
