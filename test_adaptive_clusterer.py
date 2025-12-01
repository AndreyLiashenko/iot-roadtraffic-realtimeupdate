import pytest
import numpy as np
import time
from adaptive_phase_clusterer import AdaptivePhaseSpaceClusterer

# --- FIXTURES ---

@pytest.fixture
def clusterer():
    """
    Створює екземпляр для тестування з новою сигнатурою.
    """
    return AdaptivePhaseSpaceClusterer(
        dim=3,
        dimension_weights=[1.0, 1.0, 1.0], 
        min_sigma_vec=[0.01, 0.01, 0.01], 
        # НОВИЙ ПАРАМЕТР ЗАМІСТЬ match_threshold
        bhattacharyya_threshold=0.5,       
        max_clusters=5,
        prune_age_seconds=100
    )

# --- TESTS ---

def test_bhattacharyya_merge_logic(clusterer):
    """
    Перевіряє, чи працює злиття на основі Бхаттачарія.
    """
    # 1. Існуючий кластер (Центр 0)
    c_global = {
        'center': np.array([0.0, 0.0, 0.0]),
        'sigma': np.array([1.0, 1.0, 1.0]),
        'weight': 100.0,
        'alpha': 0.1,
        'last_seen': time.time()
    }
    clusterer._clusters.append(c_global)

    # 2. Тест на злиття (Відстань 2.0 -> BC ~0.6 > 0.5)
    m_center_close = np.array([2.0, 0.0, 0.0])
    m_sigma = np.array([1.0, 1.0, 1.0])
    m_weight = 50.0

    clusterer._integrate_micro_cluster(m_center_close, m_sigma, m_weight)
    
    assert len(clusterer._clusters) == 1, "Кластери мали злитися (BC > 0.5)"
    
    # Центр має зміститися в бік нового
    merged_center = clusterer._clusters[0]['center'][0]
    assert merged_center > 0.5

    # 3. Тест на створення нового (Відстань 4.0 -> BC ~0.13 < 0.5)
    # Скидаємо кластери для чистоти
    clusterer._clusters = [c_global]
    clusterer._clusters[0]['center'] = np.array([0.0, 0.0, 0.0]) # Reset

    m_center_far = np.array([4.0, 0.0, 0.0]) 
    
    clusterer._integrate_micro_cluster(m_center_far, m_sigma, m_weight)
    
    assert len(clusterer._clusters) == 2, "Мав створитися новий кластер (BC < 0.5)"


def test_yellow_bell_fusion_accuracy(clusterer):
    clusterer._clusters.append({
        'center': np.array([0.0, 0.0, 0.0]),
        'sigma': np.array([10.0, 10.0, 10.0]),
        'weight': 1000.0,
        'alpha': 0.1,
        'last_seen': time.time()
    })

    m_center = np.array([100.0, 0.0, 0.0])
    m_sigma = np.array([1.0, 1.0, 1.0]) 

    clusterer.bhattacharyya_threshold = -1.0
    
    is_merge, result = clusterer._try_get_new_cluster(clusterer._clusters[0], m_center, m_sigma)
    
    fusion_center = result['center'][0]
    assert fusion_center > 90.0
    
    fusion_sigma = result['sigma'][0]
    assert fusion_sigma < 2.0


def test_alpha_update_on_instability(clusterer):
    """
    Перевіряє, чи змінюється Alpha при нестабільних даних.
    """
    # Стабільний кластер
    clusterer._clusters.append({
        'center': np.array([10.0, 0.0, 0.0]),
        'sigma': np.array([2.0, 2.0, 2.0]),
        'weight': 100.0,
        'alpha': 0.01, 
        'last_seen': time.time()
    })

    m_sigma = np.array([2.0, 2.0, 2.0])

    # 1. Ідеальний збіг -> Alpha лишається 0.01
    clusterer._integrate_micro_cluster(np.array([10.0, 0.0, 0.0]), m_sigma, 10.0)
    assert clusterer._clusters[0]['alpha'] == pytest.approx(0.01, abs=0.001)

    # 2. Стрес (зсув) -> Alpha росте
    # Зсув на 3.0 (це ще merge, але BC низький)
    clusterer._integrate_micro_cluster(np.array([13.0, 0.0, 0.0]), m_sigma, 10.0)
    
    new_alpha = clusterer._clusters[0]['alpha']
    assert new_alpha > 0.02, f"Alpha мала вирости через нестабільність, зараз {new_alpha}"


def test_full_batch_lifecycle(clusterer):
    """
    Інтеграційний тест: чи працює update_batch з нуля.
    """
    # Генеруємо дві групи точок
    X1 = np.random.normal([10, 0, 0], 0.5, (50, 3))
    X2 = np.random.normal([50, 0, 0], 0.5, (50, 3))
    X = np.vstack([X1, X2])
    
    clusterer.update_batch(X)
    
    # Має знайти 2 кластери
    assert len(clusterer.centers) == 2
    
    centers_x = sorted(clusterer.centers[:, 0])
    assert centers_x[0] == pytest.approx(10, abs=2.0)
    assert centers_x[1] == pytest.approx(50, abs=2.0)


def test_infer_alpha(clusterer):
    """
    Перевіряє логіку виводу (inference).
    """
    clusterer._clusters = [
        {'center': np.array([0,0,0]), 'sigma': np.ones(3), 'alpha': 0.1, 'weight': 100, 'last_seen':0},
        {'center': np.array([10,0,0]), 'sigma': np.ones(3), 'alpha': 0.9, 'weight': 100, 'last_seen':0}
    ]
    
    # Точка біля 0 -> Alpha ~ 0.1
    val = clusterer.infer_alpha([0.1, 0, 0])
    assert val == pytest.approx(0.1, abs=0.1)
    
    # Точка біля 10 -> Alpha ~ 0.9
    val = clusterer.infer_alpha([9.9, 0, 0])
    assert val == pytest.approx(0.9, abs=0.1)
    
    # Точка далеко -> Alpha ~ 0.9 (Unknown/High)
    val = clusterer.infer_alpha([100, 0, 0])
    assert val > 0.8