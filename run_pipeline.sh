#!/bin/bash

echo "=================================================="
echo "🚀 INICIANDO PIPELINE BEFLY (DATA: $(date))"
echo "=================================================="

# Define a variable for the python executable
PYTHON_EXEC="python"

# 1. Bronze
echo "=================================================="
echo "[1/3] Executando Ingestão Bronze..."
echo "=================================================="
$PYTHON_EXEC src/jobs/ingest_bronze.py
if [ $? -ne 0 ]; then
    echo "❌ Erro na Ingestão Bronze. Parando pipeline."
    exit 1
fi

# 2. Silver
echo "=================================================="
echo "[2/3] Executando Transformação Silver..."
echo "=================================================="
$PYTHON_EXEC src/jobs/transform_silver.py
if [ $? -ne 0 ]; then
    echo "❌ Erro na Transformação Silver. Parando pipeline."
    exit 1
fi

# 3. Gold
echo "=================================================="
echo "[3/3] Executando Agregação Gold..."
echo "=================================================="
$PYTHON_EXEC src/jobs/aggregate_gold.py
if [ $? -ne 0 ]; then
    echo "❌ Erro na Agregação Gold. Parando pipeline."
    exit 1
fi

echo "=================================================="
echo "✅ PIPELINE CONCLUÍDO COM SUCESSO!"
echo "=================================================="