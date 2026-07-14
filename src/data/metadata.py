{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "12432aac",
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import datetime\n",
    "import pandas as pd\n",
    "\n",
    "\n",
    "def gerar_catalogo_processamento(\n",
    "    df: pd.DataFrame,\n",
    "    ano: int,\n",
    "    tabela: str,\n",
    "    camada: str = \"silver\"\n",
    ") -> pd.DataFrame:\n",
    "    \"\"\"\n",
    "    Gera o catálogo da tabela processada.\n",
    "    \"\"\"\n",
    "\n",
    "    catalogo = {\n",
    "\n",
    "        \"DT_EXECUCAO\": datetime.now(),\n",
    "\n",
    "        \"CAMADA\": camada,\n",
    "\n",
    "        \"ANO\": ano,\n",
    "\n",
    "        \"TABELA\": tabela,\n",
    "\n",
    "        \"LINHAS\": len(df),\n",
    "\n",
    "        \"COLUNAS\": len(df.columns),\n",
    "\n",
    "        \"MEMORIA_MB\":\n",
    "            round(\n",
    "                df.memory_usage(deep=True).sum()/1024/1024,\n",
    "                2\n",
    "            ),\n",
    "\n",
    "        \"COLUNAS_LISTA\":\n",
    "            \",\".join(df.columns),\n",
    "\n",
    "        \"STATUS\": \"OK\"\n",
    "    }\n",
    "\n",
    "    return pd.DataFrame([catalogo])"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
