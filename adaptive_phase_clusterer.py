import numpy as np
import time
from sklearn.cluster import KMeans

class AdaptivePhaseSpaceClusterer:
    def __init__(
        self,
        dim: int = 3,
        dimension_weights: list = None,
        min_sigma_vec: list = None,
        
        # Поріг злиття за Бхаттачарія (0.5 = ~2.35 sigma separation)
        bhattacharyya_threshold: float = 0.5, 
        
        # Налаштування життєвого циклу
        max_clusters: int = 20,
        prune_age_seconds: int = 600, 
        min_batch_size: int = 10      
    ):
        self.dim = dim
        self.weights_vec = np.array(dimension_weights if dimension_weights else [1.0] * dim)
        
        if min_sigma_vec is not None:
            self.min_sigma_vec = np.array(min_sigma_vec)
        else:
            self.min_sigma_vec = np.array([0.05] * dim)

        self.bhattacharyya_threshold = bhattacharyya_threshold
        
        self.max_clusters = max_clusters
        self.prune_age = prune_age_seconds
        self.min_batch_size = min_batch_size
        
        self._clusters = []
        self._initialized = False

    # --- Properties (API Compatibility) ---
    @property
    def centers(self):
        if not self._clusters: return np.empty((0, self.dim))
        return np.array([c['center'] for c in self._clusters])

    @property
    def sigmas(self):
        if not self._clusters: return np.empty((0, self.dim))
        return np.array([c['sigma'] for c in self._clusters])
    
    @property
    def alphas(self):
        if not self._clusters: return np.array([])
        return np.array([c['alpha'] for c in self._clusters])

    @property
    def weights(self):
        if not self._clusters: return np.array([])
        return np.array([c['weight'] for c in self._clusters])
    
    @property
    def is_initialized(self):
        return self._initialized

    # --- Main Logic ---

    def update_batch(self, X):
        X = np.asarray(X, dtype=float)
        if len(X) < self.min_batch_size: return 

        X_weighted = X * self.weights_vec
        
        # 1. Мікро-кластеризація (K-Means Elbow)
        best_k = self._find_optimal_k_elbow_geometric(X_weighted, max_k=5)
        
        kmeans = KMeans(n_clusters=best_k, init='k-means++', n_init=3, max_iter=20, random_state=42)
        kmeans.fit(X_weighted)
        
        local_labels = kmeans.labels_
        local_centers = kmeans.cluster_centers_ / self.weights_vec

        # 2. Інтеграція кожного мікро-кластера
        for i in range(best_k):
            points = X[local_labels == i]
            if len(points) < 2: continue 
            
            m_center = local_centers[i]
            m_sigma = np.std(points, axis=0)
            m_sigma = np.maximum(m_sigma, self.min_sigma_vec)
            m_weight = float(len(points))
            
            self._integrate_micro_cluster(m_center, m_sigma, m_weight)

        self._prune_clusters()
        self._initialized = True

    def _integrate_micro_cluster(self, m_center, m_sigma, m_weight):
        """
        Знаходить найкращий існуючий кластер і намагається злитися через Бхаттачарія.
        """
        if not self._clusters:
            self._create_new_cluster(m_center, m_sigma, m_weight)
            return

        centers = self.centers
        sigmas = self.sigmas
        
        # --- ЕТАП 1: Швидкий фільтр (Mahalanobis Pooled Variance) ---
        diffs = m_center - centers
        weighted_diff_sq = (diffs * self.weights_vec) ** 2
        
        # Симетрична дисперсія (sigma_macro^2 + sigma_micro^2)
        denom = (sigmas ** 2 + m_sigma ** 2 + 1e-9)
        
        # Відстань до кожного кластера
        dist_sq = np.sum(weighted_diff_sq / denom, axis=1)
        best_idx = np.argmin(dist_sq)

        # --- ЕТАП 2: Точна перевірка та Розрахунок Злиття (Yellow Bell) ---
        best_cluster = self._clusters[best_idx]
        
        should_merge, merge_data = self._try_get_new_cluster(best_cluster, m_center, m_sigma)

        if should_merge:
            # Зливаємо, використовуючи параметри з merge_data
            self._update_existing_cluster(best_idx, merge_data, m_weight)
        else:
            # Створюємо новий
            self._create_new_cluster(m_center, m_sigma, m_weight)

    def _try_get_new_cluster(self, global_cluster, m_center, m_sigma):
        """
        Розраховує "Жовтий Дзвін" (Optimal Bayesian Fusion) та Коефіцієнт Бхаттачарія.
        Повертає параметри, які стануть новим станом кластера при злитті.
        """
        g_center = global_cluster['center']
        g_sigma = global_cluster['sigma']

        # --- 1. Параметри Злиття (Inverse Variance Weighting) ---
        var1 = g_sigma ** 2
        var2 = m_sigma ** 2
        sum_var = var1 + var2 + 1e-9

        # Новий центр (тяжіє до точнішого вимірювання)
        new_center = (g_center * var2 + m_center * var1) / sum_var
        
        # Нова сигма (Bhattacharyya distribution sigma / Overlap width)
        new_var_b = (2 * var1 * var2) / sum_var
        new_sigma = np.sqrt(new_var_b)
        new_sigma = np.maximum(new_sigma, self.min_sigma_vec)

        # --- 2. Коефіцієнт Бхаттачарія (BC) ---
        # BC = Size_Factor * Distance_Factor
        
        term_size = np.sqrt((2 * g_sigma * m_sigma) / sum_var)
        
        diff_weighted = (g_center - m_center) * self.weights_vec
        # У знаменнику 4 * sum_var для нормальних розподілів
        exponent = - (diff_weighted ** 2) / (4 * sum_var + 1e-9)
        term_dist = np.exp(exponent)

        bc_per_dim = term_size * term_dist
        bc_score = np.prod(bc_per_dim)

        # --- 3. Результат ---
        if bc_score > self.bhattacharyya_threshold:
            return True, {
                'center': new_center,
                'sigma': new_sigma,
                'bc_score': bc_score
            }
        
        return False, None

    def _update_existing_cluster(self, idx, merge_data, m_weight):
        """
        Пряме оновлення кластера до стану "Жовтого Дзвона".
        Вага накопичується тільки для історії.
        """
        c = self._clusters[idx]
        
        # 1. Пряме оновлення фізики (Stateless update)
        # Ми повністю довіряємо розрахунку _try_get_new_cluster
        c['center'] = merge_data['center']
        c['sigma'] = merge_data['sigma']
        
        # 2. Розрахунок Alpha (Signal Instability)
        # Використовуємо для зовнішнього світу (Consumer update)
        # Instability = 1.0 - Similarity. 
        bc_score = merge_data['bc_score']
        threshold = self.bhattacharyya_threshold
        
        if threshold >= 1.0:
            instability = 0.0
        else:
            instability = (1.0 - bc_score) / (1.0 - threshold)
        
        # Обрізаємо межі, щоб не вилізти за [0, 1] через float похибки
        instability = np.clip(instability, 0.0, 1.0)
        
        # Залишаємо мінімальний пульс 0.01, щоб графік не завмирав
        c['alpha'] = max(instability, 0.01)

        # 3. Backward Compatibility (Вага як лічильник)
        c['weight'] += m_weight
        
        # Метадані
        c['last_seen'] = time.time()
        c['merge_count'] = c.get('merge_count', 0) + 1

    def _create_new_cluster(self, center, sigma, weight):
        if len(self._clusters) >= self.max_clusters:
            self._prune_clusters(force=True)
            if len(self._clusters) >= self.max_clusters: return

        # Новий кластер завжди має високу невизначеність/альфу спочатку
        self._clusters.append({
            'center': center,
            'sigma': sigma,
            'weight': weight,
            'alpha': 0.5, 
            'last_seen': time.time(),
            'created_at': time.time(),
            'merge_count': 0
        })

    def _prune_clusters(self, force=False):
        current_time = time.time()
        survivors = []
        # Примусове видалення: залишаємо найвагоміші (історично значущі)
        sorted_clusters = sorted(self._clusters, key=lambda x: x['weight'], reverse=True)
        
        for c in sorted_clusters:
            age = current_time - c['last_seen']
            if force:
                if len(survivors) < self.max_clusters - 1:
                    survivors.append(c)
                continue
            if age < self.prune_age:
                survivors.append(c)
        self._clusters = survivors

    def infer_alpha(self, x):
        """
        Inference based on Mahalanobis distance.
        """
        if not self._clusters: return 0.5
        
        x = np.asarray(x, dtype=float)
        centers = self.centers
        sigmas = self.sigmas
        alphas = self.alphas
        
        diffs = (x - centers) * self.weights_vec
        denom = (sigmas ** 2 + 1e-9)
        
        d_sq = np.sum((diffs ** 2) / denom, axis=1)
        
        weights = np.exp(-0.5 * d_sq)
        total_weight = np.sum(weights)
        
        if total_weight < 1e-9: 
            return 0.9 # Unknown state -> high alpha
            
        return np.sum(weights * alphas) / total_weight

    def _find_optimal_k_elbow_geometric(self, X, max_k=5):
        n_samples = X.shape[0]
        limit_k = min(max_k, n_samples // 10 + 1)
        if limit_k < 2: return 1
        
        sse = []
        K_range = list(range(1, limit_k + 1))
        
        for k in K_range:
            km = KMeans(n_clusters=k, init='k-means++', n_init=1, max_iter=10, random_state=42)
            km.fit(X)
            sse.append(km.inertia_)
        
        if sse[0] == 0: return 1
        
        k_norm = (np.array(K_range) - K_range[0]) / (K_range[-1] - K_range[0])
        sse_norm = (np.array(sse) - min(sse)) / (max(sse) - min(sse) + 1e-9)
        
        points = list(zip(k_norm, sse_norm))
        p1 = np.array(points[0])
        p2 = np.array(points[-1])
        vec_line = p2 - p1
        
        max_dist = -1
        best_idx = 0
        
        for i, p in enumerate(points):
            p_arr = np.array(p)
            vec_p = p_arr - p1
            if np.linalg.norm(vec_line) == 0: continue
            
            proj = np.dot(vec_p, vec_line) / np.dot(vec_line, vec_line)
            proj_point = p1 + proj * vec_line
            dist = np.linalg.norm(p_arr - proj_point)
            
            if dist > max_dist:
                max_dist = dist
                best_idx = i
        
        if (sse[0] - sse[-1]) / (sse[0] + 1e-9) < 0.15: 
            return 1
            
        return K_range[best_idx]