# Approach

- Modular ETL (clean → enrich → embed → query)

- Vector + SQL hybrid design

# Embedding Model Choice

- all-MiniLM-L6-v2

- Fast, lightweight, strong semantic similarity

# Edge Cases

- Revenue: handled text, currency, ranges

- Dates: multiple formats + fallback parsing

- Fuzzy matching for company names

# Trade-offs

- Accuracy vs speed in fuzzy matching

- DuckDB Integration 

- Cosine similarity 