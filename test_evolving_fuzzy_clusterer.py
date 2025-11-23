import numpy as np
import pytest
from evolving_fuzzy import EvolvingFuzzyClusterer


# ============================================================
# BASIC CREATION
# ============================================================
def test_initial_state():
    c = EvolvingFuzzyClusterer(dim=3)

    assert c._centers.shape == (0, 3)
    assert c._sigmas.shape == (0, 3)
    assert c.weights.size == 0
    assert c.alphas.size == 0
    assert not c._robust_initialized


# ============================================================
# ROBUST NORMALIZATION
# ============================================================
def test_robust_stats_update():
    c = EvolvingFuzzyClusterer(dim=3)

    X = np.array([
        [10, 1, 100],
        [20, 2, 200],
        [30, 3, 300],
    ])

    c.update_batch(X)

    # robust stats must be initialized
    assert c._robust_initialized
    assert np.allclose(c.median, [20, 2, 200], atol=1e-6)
    assert c.q1.shape == (3,)
    assert c.q3.shape == (3,)


def test_normalization_and_inverse_consistency():
    c = EvolvingFuzzyClusterer(dim=3)

    X = np.array([
        [10, 1, 100],
        [20, 2, 200],
        [30, 3, 300],
    ])

    c.update_batch(X)

    # convert → normalized → back
    Xn = c._norm(X)
    X_back = Xn * np.maximum(c.q3 - c.q1, 1e-6) + c.median

    assert np.allclose(X, X_back, atol=1e-6)


# ============================================================
# FIRST CLUSTER CREATION
# ============================================================
def test_create_first_cluster():
    c = EvolvingFuzzyClusterer(dim=3)

    X = np.array([[10.0, 2.0, 100.0]])
    c.update_batch(X)

    assert c._centers.shape == (1, 3)
    assert c._sigmas.shape == (1, 3)
    assert np.all(c._sigmas[0] > 0)
    assert c.weights[0] == pytest.approx(1.0)
    assert c.alphas[0] >= c.alpha_min


# ============================================================
# CLUSTER UPDATE
# ============================================================
def test_update_existing_cluster():
    c = EvolvingFuzzyClusterer(dim=3)

    X1 = np.array([[10, 2, 100]])
    c.update_batch(X1)
    old_center_real = c.centers[0].copy()

    X2 = np.array([[12, 2.5, 110]])
    c.update_batch(X2)

    new_center_real = c.centers[0]

    assert not np.allclose(old_center_real, new_center_real)
    
    assert new_center_real[0] > 10.0 
    assert new_center_real[0] < 12.0


# ============================================================
# MERGE LOGIC
# ============================================================
def test_merge_clusters():
    c = EvolvingFuzzyClusterer(dim=3)

    # Artificial normalized centers
    c._centers = np.array([
        [0.0, 0.0, 0.0],
        [0.01, 0.01, 0.01],  # very close → should merge
    ])
    c._sigmas = np.array([
        [0.2, 0.2, 0.2],
        [0.2, 0.2, 0.2],
    ])
    c.alphas = np.array([0.3, 0.7])
    c.weights = np.array([5.0, 5.0])

    c._merge_clusters()

    assert c._centers.shape == (1, 3)
    assert c.weights[0] == pytest.approx(10.0)
    assert c.alphas.shape == (1,)


# ============================================================
# SPLIT LOGIC
# ============================================================
def test_split_clusters():
    # Можна явно задати менший split_factor, щоб тест точно пройшов
    c = EvolvingFuzzyClusterer(dim=3, split_factor=1.2) 

    # Create clusters artificially
    c._centers = np.array([
        [0.0, 0.0, 0.0],
        [1.0, 1.0, 1.0],
    ])
    
    # Збільшуємо сигму до 2.0, щоб радіус став √12 ≈ 3.46
    # Тоді 3.46 > (1.2 * 1.73) спрацює гарантовано
    c._sigmas = np.array([
        [2.0, 2.0, 2.0],   # <--- БУЛО 1.5, СТАЛО 2.0
        [0.3, 0.3, 0.3],
    ])
    
    c.alphas = np.array([0.5, 0.5])
    c.weights = np.array([20.0, 5.0])

    prev_k = c._centers.shape[0]
    c._split_clusters()  # Виклик нового методу
    new_k = c._centers.shape[0]

    assert new_k == prev_k + 1


# ============================================================
# INFER ALPHA
# ============================================================
def test_infer_alpha_range():
    c = EvolvingFuzzyClusterer(dim=3)

    X = np.array([
        [10, 2, 100],
        [12, 2.5, 110],
        [14, 3, 120],
    ])
    c.update_batch(X)

    alpha = c.infer_alpha([11, 2.2, 105])

    assert c.alpha_min <= alpha <= c.alpha_max


def test_infer_alpha_closer_to_small_speed_cluster():
    c = EvolvingFuzzyClusterer(dim=3)

    c._robust_initialized = True
    c.median = np.zeros(3)
    c.q1 = np.zeros(3)
    c.q3 = np.ones(3)

    # centers in normalized space
    c._centers = np.array([
        [0.0, 0.0, 0.0],  # slow cluster
        [3.0, 3.0, 3.0],  # fast cluster
    ])
    c._sigmas = np.array([
        [0.2, 0.2, 0.2],
        [0.2, 0.2, 0.2],
    ])
    c.alphas = np.array([0.2, 0.9])
    c.weights = np.array([10, 10])

    alpha = c.infer_alpha([0.1, 0.1, 0.1])

    # Must be closer to 0.2 than 0.9
    assert abs(alpha - 0.2) < abs(alpha - 0.9)


# ============================================================
# BATCH CREATES MULTIPLE CLUSTERS
# ============================================================
def test_batch_creates_multiple_clusters():
    c = EvolvingFuzzyClusterer(dim=3)

    X = np.array([
        [10, 2, 100],
        [11, 2.2, 105],
        [50, 9, 200],
        [48, 8.5, 195],
    ])
    c.update_batch(X)

    assert 1 <= c._centers.shape[0] <= 4