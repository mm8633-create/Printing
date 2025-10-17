FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY cards ./cards
COPY samples ./samples
COPY cards/data ./cards/data
COPY .env.example ./

RUN pip install --upgrade pip && pip install .

EXPOSE 8000

CMD ["uvicorn", "cards.api:app", "--host", "0.0.0.0", "--port", "8000"]
