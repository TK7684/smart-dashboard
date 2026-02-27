"""
Market Basket Analysis Module
=============================
Find product associations using the Apriori algorithm.

Identifies products that are frequently bought together,
enabling cross-selling and bundle recommendations.
"""

import pandas as pd
import numpy as np
from itertools import combinations


def create_basket_matrix(df, order_id_col='Order_ID', product_col='Product_Name',
                         quantity_col='Quantity'):
    """
    Create a binary basket matrix from transaction data.

    Parameters:
    -----------
    df : DataFrame
        Transaction data with order ID and product columns
    order_id_col : str
        Name of order identifier column
    product_col : str
        Name of product name column
    quantity_col : str
        Name of quantity column (used to check if product was bought)

    Returns:
    --------
    DataFrame : Binary matrix (orders x products)
    """
    # Create pivot table
    basket = df.groupby([order_id_col, product_col])[quantity_col].sum().unstack().fillna(0)

    # Convert to binary (1 if bought, 0 if not)
    basket_encoded = basket.applymap(lambda x: 1 if x > 0 else 0)

    return basket_encoded


def calculate_support(basket_matrix, min_support=0.01):
    """
    Calculate support for all itemsets.

    Support = (Transactions containing itemset) / (Total transactions)

    Parameters:
    -----------
    basket_matrix : DataFrame
        Binary basket matrix
    min_support : float
        Minimum support threshold (default 1%)

    Returns:
    --------
    dict : Itemsets with support values
    """
    n_transactions = len(basket_matrix)
    support_dict = {}

    # Single items support
    for col in basket_matrix.columns:
        support = basket_matrix[col].sum() / n_transactions
        if support >= min_support:
            support_dict[(col,)] = support

    return support_dict, n_transactions


def find_frequent_itemsets(basket_matrix, min_support=0.01, max_length=3):
    """
    Find all frequent itemsets using Apriori algorithm.

    Parameters:
    -----------
    basket_matrix : DataFrame
        Binary basket matrix
    min_support : float
        Minimum support threshold
    max_length : int
        Maximum itemset length to consider

    Returns:
    --------
    DataFrame : Frequent itemsets with support values
    """
    n_transactions = len(basket_matrix)
    frequent_itemsets = {}

    # Get single items
    single_items = {}
    for col in basket_matrix.columns:
        support = basket_matrix[col].sum() / n_transactions
        if support >= min_support:
            single_items[(col,)] = support

    frequent_itemsets.update(single_items)
    current_itemsets = list(single_items.keys())

    # Find larger itemsets
    for length in range(2, max_length + 1):
        candidate_itemsets = set()

        # Generate candidates by combining previous itemsets
        for i, itemset1 in enumerate(current_itemsets):
            for itemset2 in current_itemsets[i+1:]:
                # Combine and get unique items
                combined = tuple(sorted(set(itemset1 + itemset2)))
                if len(combined) == length:
                    candidate_itemsets.add(combined)

        # Calculate support for candidates
        new_itemsets = {}
        for itemset in candidate_itemsets:
            # Check if all items in itemset are present together
            mask = basket_matrix[list(itemset)].all(axis=1)
            support = mask.sum() / n_transactions
            if support >= min_support:
                new_itemsets[itemset] = support

        if not new_itemsets:
            break

        frequent_itemsets.update(new_itemsets)
        current_itemsets = list(new_itemsets.keys())

    # Convert to DataFrame
    result = pd.DataFrame([
        {'itemsets': itemset, 'support': support}
        for itemset, support in frequent_itemsets.items()
    ])

    if len(result) > 0:
        result['length'] = result['itemsets'].apply(len)
        result = result.sort_values('support', ascending=False).reset_index(drop=True)

    return result


def generate_associion_rules(frequent_itemsets, min_confidence=0.3, min_lift=1.0):
    """
    Generate association rules from frequent itemsets.

    Rule: A -> B means "if A is bought, then B is likely to be bought"

    Parameters:
    -----------
    frequent_itemsets : DataFrame
        Frequent itemsets with support values
    min_confidence : float
        Minimum confidence threshold (default 30%)
    min_lift : float
        Minimum lift threshold (default 1.0)

    Returns:
    --------
    DataFrame : Association rules with metrics
    """
    rules = []

    # Get support as dictionary for quick lookup
    support_dict = {row['itemsets']: row['support']
                    for _, row in frequent_itemsets.iterrows()}

    # Only consider itemsets with length >= 2
    multi_itemsets = frequent_itemsets[frequent_itemsets['length'] >= 2]

    for _, row in multi_itemsets.iterrows():
        itemset = row['itemsets']
        itemset_support = row['support']

        # Generate all possible antecedent -> consequent combinations
        for i in range(1, len(itemset)):
            for antecedent in combinations(itemset, i):
                consequent = tuple(sorted(set(itemset) - set(antecedent)))

                if not consequent:
                    continue

                # Get antecedent support
                antecedent_support = support_dict.get(antecedent, 0)

                if antecedent_support == 0:
                    continue

                # Calculate confidence = support(A∪B) / support(A)
                confidence = itemset_support / antecedent_support

                if confidence < min_confidence:
                    continue

                # Get consequent support
                consequent_support = support_dict.get(consequent, 0)

                # Calculate lift = confidence / support(B)
                if consequent_support > 0:
                    lift = confidence / consequent_support
                else:
                    lift = 0

                if lift < min_lift:
                    continue

                rules.append({
                    'antecedents': antecedent,
                    'consequents': consequent,
                    'antecedent_support': antecedent_support,
                    'consequent_support': consequent_support,
                    'support': itemset_support,
                    'confidence': confidence,
                    'lift': lift
                })

    rules_df = pd.DataFrame(rules)

    if len(rules_df) > 0:
        rules_df = rules_df.sort_values('lift', ascending=False).reset_index(drop=True)

    return rules_df


def market_basket_analysis(df, order_id_col='Order_ID', product_col='Product_Name',
                           quantity_col='Quantity', min_support=0.005,
                           min_confidence=0.2, min_lift=1.0, max_length=3):
    """
    Full market basket analysis pipeline.

    Parameters:
    -----------
    df : DataFrame
        Transaction data
    order_id_col : str
        Order identifier column
    product_col : str
        Product name column
    quantity_col : str
        Quantity column
    min_support : float
        Minimum support for itemsets
    min_confidence : float
        Minimum confidence for rules
    min_lift : float
        Minimum lift for rules
    max_length : int
        Maximum itemset length

    Returns:
    --------
    tuple : (frequent_itemsets, rules)
    """
    # Create basket matrix
    basket = create_basket_matrix(df, order_id_col, product_col, quantity_col)

    # Find frequent itemsets
    frequent_itemsets = find_frequent_itemsets(basket, min_support, max_length)

    # Generate rules
    rules = generate_associion_rules(frequent_itemsets, min_confidence, min_lift)

    return frequent_itemsets, rules


def get_product_recommendations(rules, product_name, top_n=5):
    """
    Get product recommendations based on association rules.

    Parameters:
    -----------
    rules : DataFrame
        Association rules
    product_name : str
        Product to get recommendations for
    top_n : int
        Number of recommendations to return

    Returns:
    --------
    DataFrame : Recommended products with metrics
    """
    # Find rules where the product is in antecedents
    relevant_rules = rules[
        rules['antecedents'].apply(lambda x: product_name in x)
    ].copy()

    if len(relevant_rules) == 0:
        return pd.DataFrame()

    # Get consequent products with highest lift
    recommendations = []
    for _, row in relevant_rules.iterrows():
        for product in row['consequents']:
            recommendations.append({
                'product': product,
                'confidence': row['confidence'],
                'lift': row['lift'],
                'support': row['support']
            })

    rec_df = pd.DataFrame(recommendations)
    rec_df = rec_df.sort_values('lift', ascending=False).head(top_n)

    return rec_df


def get_bundle_opportunities(rules, min_lift=2.0, min_confidence=0.3, top_n=10):
    """
    Identify top bundle opportunities.

    Parameters:
    -----------
    rules : DataFrame
        Association rules
    min_lift : float
        Minimum lift threshold
    min_confidence : float
        Minimum confidence threshold
    top_n : int
        Number of bundles to return

    Returns:
    --------
    DataFrame : Bundle opportunities with metrics
    """
    # Filter for strong rules
    strong_rules = rules[
        (rules['lift'] >= min_lift) &
        (rules['confidence'] >= min_confidence)
    ].copy()

    if len(strong_rules) == 0:
        return pd.DataFrame()

    # Format for display
    strong_rules['bundle'] = strong_rules.apply(
        lambda x: f"{', '.join(x['antecedents'])} → {', '.join(x['consequents'])}",
        axis=1
    )

    return strong_rules[['bundle', 'antecedents', 'consequents',
                         'support', 'confidence', 'lift']].head(top_n)


def get_cross_sell_products(rules, product_name, top_n=5):
    """
    Get products to cross-sell with a given product.

    Parameters:
    -----------
    rules : DataFrame
        Association rules
    product_name : str
        Product to cross-sell with
    top_n : int
        Number of products to return

    Returns:
    --------
    list : Product names sorted by association strength
    """
    recommendations = get_product_recommendations(rules, product_name, top_n)

    if len(recommendations) == 0:
        return []

    return recommendations['product'].tolist()
