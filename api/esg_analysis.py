# -*- coding: utf-8 -*-
"""
Created on Tue May  6 09:28:38 2025

@author: joaot
"""

# -*- coding: utf-8 -*-
"""
ESG Analysis for Sustainacore using ESG_AI
"""

import sys
import os
import matplotlib.pyplot as plt

# Add the ESG_AI repo path to the Python environment
sys.path.append("C:/Users/joaot/OneDrive/Documentos/My Project/ESG_AI")

# Import the ESGAnalyzer class
from esg_ai import ESGAnalyzer

# Create analyzer and run analysis
company = "Microsoft"  # Change this to any company name you want
analyzer = ESGAnalyzer()
results = analyzer.analyze(company)

# Print results
print(f"ESG Analysis for {company}:")
print(results)

# Simple visualization if results are in dict format
if isinstance(results, dict):
    plt.figure(figsize=(10, 5))
    plt.bar(results.keys(), results.values())
    plt.title(f"ESG Scores for {company}")
    plt.xlabel("ESG Category")
    plt.ylabel("Score")
    plt.tight_layout()
    plt.show()
