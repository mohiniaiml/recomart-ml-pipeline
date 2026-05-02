import numpy as np


def precision_at_k(actual, predicted, k=5):
    predicted = predicted[:k]
    return len(set(predicted) & set(actual)) / k


def recall_at_k(actual, predicted, k=5):
    predicted = predicted[:k]
    return len(set(predicted) & set(actual)) / len(actual) if actual else 0


def ndcg_at_k(actual, predicted, k=5):
    predicted = predicted[:k]

    dcg = 0.0
    for i, p in enumerate(predicted):
        if p in actual:
            dcg += 1 / np.log2(i + 2)

    ideal_dcg = sum(1 / np.log2(i + 2) for i in range(min(len(actual), k)))

    return dcg / ideal_dcg if ideal_dcg > 0 else 0


# -----------------------------
# Simulated business metrics
# -----------------------------
def compute_ctr(recommended, clicked):
    return len(set(recommended) & set(clicked)) / len(recommended)


def compute_conversion_rate(recommended, purchased):
    return len(set(recommended) & set(purchased)) / len(recommended)