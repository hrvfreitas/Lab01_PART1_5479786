import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5432/pncp_db')

def run_sanity_check():
    print("🔍 Iniciando Sanity Check (Camada Gold)...")
    
    queries = {
        "Total na Fato": "SELECT COUNT(*) FROM fato_contratos",
        "Órgãos s/ Nome": "SELECT COUNT(*) FROM fato_contratos f LEFT JOIN dim_orgaos o ON f.orgao_entidade_id = o.orgao_entidade_id WHERE o.nome_orgao IS NULL",
        "Fornecedores s/ Nome": "SELECT COUNT(*) FROM fato_contratos f LEFT JOIN dim_fornecedores forn ON f.cnpj_contratada = forn.cnpj_contratada WHERE forn.nome_contratada IS NULL",
        "Contratos s/ Modalidade": "SELECT COUNT(*) FROM fato_contratos WHERE id_modalidade IS NULL",
        "Soma Total (R$ Bi)": "SELECT ROUND(SUM(valor_global)/1e9, 2) FROM fato_contratos"
    }

    results = {}
    for name, sql in queries.items():
        results[name] = pd.read_sql(sql, engine).iloc[0, 0]

    print("\n--- RELATÓRIO DE INTEGRIDADE ---")
    for k, v in results.items():
        status = "✅ OK" if (v == 0 if "s/" in k else v > 0) else "⚠️ REVISAR"
        print(f"{k.ljust(25)}: {v} {status}")

    # Validação Cruzada: Fato vs Dimensões
    if results["Órgãos s/ Nome"] > 0:
        print("\n💡 Dica CSiS: Você tem contratos apontando para CNPJs que não estão na dim_orgaos. Verifique o script de carga das dimensões.")

if __name__ == "__main__":
    run_sanity_check()
