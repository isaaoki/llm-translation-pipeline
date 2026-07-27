# LLM Translation Pipeline (RabbitMQ + Local LLM)

[![Python](https://img.shields.io/badge/Python-3.x-blue)]()
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-async%20queue-orange)]()
[![Ollama](https://img.shields.io/badge/Ollama-local%20LLM-black)]()
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED)]()

An asynchronous translation workflow originally built for the InReDD Research
Group (USP), automating translation of user-submitted content across a
research platform's website and dental application — without blocking the
API or sending user data to a third-party translation service.´

> **Note:** This is a public, standalone reference implementation of the
> architecture and pipeline logic. The original integration lives in a
> private institutional codebase; this repo reproduces the approach so it
> can be run, tested inspected independently.

## 🎯 Problem
User-submitted fields needed to be translated (PT-BR / EN / ES) without
blocking the request cycle or paying per-request for an external
translation API — with data privacy as a hard requirement, given the
health-data-adjacent context.

## 🏗️ Architecture
1. **Producer** — publishes a translation job to a RabbitMQ queue whenever
   new user content is submitted (`test_publish.py` simulates this). 
2. **Consumer / Worker** — a background worker (`translation_worker.py`)
   consumes jobs asynchronously and sends the text to a locally-hosted LLM
   (TranslateGemma via Ollama).
3. **Local inference** — Ollama runs the model on-device, avoiding external
   API cost/latency and keeping data in-house.
4. **Result delivery** — the translated content is published back to a
   response queue.

## ⚙️ Why this design
- **RabbitMQ**: decouples translation from the request cycle — the caller
  isn't blocked on LLM inference, and jobs can be retried/queued under load.
  Malformed messages are `nack`'d without requeue; processing failures are
  requeued for retry.
- **Local LLM (Ollama)**: no per-request API cost, no user data leaving the
  infrastructure.


## 🚀 Running it locally
```bash
git clone https://github.com/isaaoki/llm-translation-pipeline.git
cd llm-translation-pipeline
cp .env.example .env
 
docker compose up --build
```
Once RabbitMQ, Ollama, and the worker are up, pull the model and send a
sample request:
```bash
docker exec -it <ollama-container> ollama pull translategemma
python test_publish.py
```
`test_publish.py` publishes a sample translation request and prints the
response received from the worker — a runnable end-to-end demo of the
pipeline.
 
## 📊 Performance (informal testing)
Average response time: under 60 seconds per translation job in local
testing on a single-request basis. Formal throughput/latency benchmarks
under concurrent load are a planned next step.

## 🛠️ Tech Stack
Python · RabbitMQ (pika) · Ollama (TranslateGemma) · Docker / docker-compose

## 📄 Status
Derived from a component actively used in production for the InReDD
platform. This repo is a sanitized, standalone reference implementation
for portfolio and demonstration purposes.

## TODO
- [ ] Formal latency/throughput benchmarks under load
- [ ] Fix tests dir with localhost