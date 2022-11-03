# start by pulling the python image
FROM python:3.8

# copy the requirements file into the image
COPY ./requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

RUN  mkdir -p /app/Logs
# install the dependencies and packages in the requirements file
RUN pip3 install -r requirements.txt

RUN python3 -m pip install pyyaml
# copy every content from the local file to the image
COPY . /app

# configure the container to run in an executed manner
ENTRYPOINT [ "python3" ]

CMD ["main.py" ]