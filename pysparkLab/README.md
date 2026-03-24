# Análise de Clientes e Pedidos com PySpark

Este projeto analisa dados de clientes e pedidos usando o script principal `main.py` com PySpark.

## 🚀 Como executar

1. Instale dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Execute:
   ```bash
   python main.py
   ```

## 📊 O que o `main.py` faz (resumo)

- Configura ambiente PySpark e inicializa sessão local
- Lê `data-clients.json` e `data-pedidos.json`
- Valida qualidade de pedidos:
  - Valores nulos
  - Valores <= 0
  - Cliente inexistente
  - IDs duplicados
- Agrega pedidos por cliente:
  - Quantidade de pedidos
  - Valor total, média, mediana, P10 e P90
- Junta com clientes e ordena por `Valor_Total` decrescente
- Exibe relatório completo (todos, acima da média, P10-P90) e métricas gerais

## 📁 Estrutura do projeto

```
pyspark-project/
├── main.py             # Script Python principal
├── requirements.txt    # Dependências Python
└── README.md
```

## 🐛 Troubleshooting

- **Erro de Java**: verifique `JAVA_HOME` e versão compatível com PySpark.
- **Erro de PySpark**: confirme versões de `pyspark` e `java`.
- **Erro de arquivo**: confirme que `data-clients.json` e `data-pedidos.json` existem no diretório de execução.