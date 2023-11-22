FROM python:3.11

SHELL ["/bin/bash", "-c"]

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils \
    && apt-get install -y gcc libjpeg-dev libxslt-dev libpq-dev libmariadb-dev libmariadb-dev-compat gettext cron openssh-client flake8 locales vim  netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

RUN useradd -rms /bin/bash user && chmod 777 /opt /run

WORKDIR /user

COPY --chown=user:user . .

RUN pip install -r requirements.txt

USER user

WORKDIR /user/
CMD ["bash", "-c", "chmod +x run.sh && ./run.sh"]

