From python:3.7-buster as basepython

WORKDIR /opt/app-root/
ENV PATH=/opt/app-root/bin:$PATH

# Create an apt-root dir and set permissions, add a python
# virtual environment and install pip.
RUN /usr/local/bin/python -m venv /opt/app-root/ && \
    /opt/app-root/bin/pip install -U pip && \
    useradd -m -N -u 1001 -s /bin/bash -g 0 user && \
    chown -R 1001:0 /opt/app-root && \
    chmod -R og+rx /opt/app-root

# Install the goes_viewer dependencies from requirements.txt and 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

# Build a wheel so we can install it later
FROM basepython as wheelbuild
COPY . /src
RUN cd /src/ && \
    pip install --no-cache-dir wheel && \
    python setup.py bdist_wheel

FROM amacneil/dbmate:v1.11.0 as dbmate

# Install goes_viewer from the built wheel
FROM basepython
COPY --from=wheelbuild /src/dist/*.whl /opt/app-root/.
COPY --from=wheelbuild /src/static /opt/app-root/static
RUN pip install --no-cache-dir /opt/app-root/*.whl

EXPOSE 8080
USER 1001
