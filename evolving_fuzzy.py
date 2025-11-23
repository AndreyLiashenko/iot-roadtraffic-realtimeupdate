import numpy as np
import math
from sklearn.neighbors import KDTree

class EvolvingFuzzyClusterer:

    def __init__(
        self,
        dim: int,
        alpha_min: float = 0.05,
        alpha_max: float = 0.9,
        merge_factor: float = 1.2,      # Агресивне злиття
        split_factor: float = 1.8,
        k_new: float = 4.0,             # Високий поріг (тільки явні аномалії створюють кластери)
        sigma_init: float = 1.0,
        sigma_min: float = 1.0,
        sigma_max: float = 50.0,
        robust_max_samples: int = 10000,
        merge_every: int = 5,
        split_every: int = 10,
        max_clusters: int = 50
    ):
        self.dim = dim
        self.alpha_min = alpha_min
        self.alpha_max = alpha_max
        self.theta_new = math.exp(-k_new)

        self.sigma_init = sigma_init
        self.sigma_min = sigma_min
        self.sigma_max = sigma_max
        self.merge_factor = merge_factor
        self.split_factor = split_factor
        
        self.max_clusters = max_clusters

        # Параметри кластерів
        self._centers = np.empty((0, dim))
        self._sigmas = np.empty((0, dim))
        self.alphas = np.empty((0,))
        self.weights = np.empty((0,))

        # Статистика для нормалізації
        self._robust_buffer = np.empty((0, dim))
        self._robust_max_samples = robust_max_samples
        self.median = np.zeros(dim)
        self.q1 = np.zeros(dim)
        self.q3 = np.zeros(dim)
        self._robust_initialized = False
        
        self._update_counter = 0
        self._merge_every = merge_every
        self._split_every = split_every

        self._min_iqr = np.array([5.0, 0.05, 5.0]) 
        if len(self._min_iqr) != dim:
            self._min_iqr = np.ones(dim)

    # ======================================================
    # Public properties (Denormalized)
    # ======================================================
    @property
    def centers(self):
        if not self._robust_initialized:
            return self._centers.copy()
        iqr = np.maximum(self.q3 - self.q1, self._min_iqr)
        return self._centers * iqr + self.median

    @property
    def sigmas(self):
        if not self._robust_initialized:
            return self._sigmas.copy()
        iqr = np.maximum(self.q3 - self.q1, self._min_iqr)
        return self._sigmas * iqr

    # ======================================================
    # Robust Normalization (FIXED)
    # ======================================================
    def _update_robust_stats(self, X):
        if self._robust_buffer.size == 0:
            self._robust_buffer = X.copy()
        else:
            if len(self._robust_buffer) >= self._robust_max_samples:
                 n_new = X.shape[0]
                 indices = np.random.randint(0, self._robust_max_samples, n_new)
                 self._robust_buffer[indices] = X
            else:
                self._robust_buffer = np.vstack([self._robust_buffer, X])

        self.median = np.median(self._robust_buffer, axis=0)
        self.q1 = np.quantile(self._robust_buffer, 0.25, axis=0)
        self.q3 = np.quantile(self._robust_buffer, 0.75, axis=0)
        self._robust_initialized = True

    def _norm(self, X):
        if not self._robust_initialized:
            return X.astype(float)
        # ЗАХИСТ ВІД 1e-6 (IQR Collapse)
        iqr = np.maximum(self.q3 - self.q1, self._min_iqr)
        return (X - self.median) / iqr

    # ======================================================
    # Fuzzy Math
    # ======================================================
    def _mu(self, Xn):
        K = self._centers.shape[0]
        if K == 0:
            return np.zeros((len(Xn), 0))
        diffs = Xn[:, None, :] - self._centers[None, :, :]
        denom = 2 * (self._sigmas ** 2)[None, :, :]
        d2 = np.sum((diffs ** 2) / denom, axis=2)
        MU = np.exp(-d2)
        # Відсікаємо мікро-значення для швидкості
        MU[MU < 1e-9] = 0
        return MU

    # ======================================================
    # MAIN UPDATE (OPTIMIZED & SAFE)
    # ======================================================
    def update_batch(self, X):
        X = np.asarray(X, float)
        if X.ndim == 1: X = X[None, :]

        self._update_robust_stats(X)
        Xn = self._norm(X)
        MU = self._mu(Xn)

        # --- 1. СТВОРЕННЯ КЛАСТЕРІВ (З ЛІМІТОМ І БЕЗ ЦИКЛУ) ---
        if self._centers.shape[0] < self.max_clusters:
            if MU.shape[1] == 0:
                # Перший кластер - середнє значення батчу
                c0 = Xn.mean(axis=0)
                self._add_new_clusters(c0[None, :])
                MU = self._mu(Xn)
            else:
                # Шукаємо точки, які не підходять нікуди
                max_mu = MU.max(axis=1) if MU.shape[1] > 0 else np.zeros(len(Xn))
                new_mask = max_mu < self.theta_new
                
                outliers = Xn[new_mask]
                n_outliers = outliers.shape[0]
                
                # Скільки місця залишилось до ліміту?
                space_left = self.max_clusters - self._centers.shape[0]
                
                if n_outliers > 0 and space_left > 0:
                    # Ліміт додавання за один раз (щоб CPU не згорів)
                    # Беремо максимум 10 нових за раз
                    limit_new = min(10, space_left) 
                    
                    if n_outliers > limit_new:
                        # Беремо рівномірно розподілені точки, щоб покрити діапазон
                        idx = np.linspace(0, n_outliers-1, limit_new, dtype=int)
                        outliers = outliers[idx]
                    
                    # BULK CREATE (швидко!)
                    self._add_new_clusters(outliers)
                    MU = self._mu(Xn) # Перерахунок

        K = self._centers.shape[0]
        if K == 0: return

        # --- 2. ОНОВЛЕННЯ ПАРАМЕТРІВ (З ІНЕРЦІЄЮ) ---
        w = MU.sum(axis=0)
        active_mask = w > 1e-6
        
        # Швидкість навчання (0.1 = плавна адаптація)
        lr = 0.1 
        
        if np.any(active_mask):
            # Центри
            target_centers = (MU.T @ Xn) / (w[:, None] + 1e-12)
            self._centers[active_mask] = (1 - lr) * self._centers[active_mask] + lr * target_centers[active_mask]
            
            # Сигми
            diffs = Xn[:, None, :] - self._centers[None, :, :]
            var = (MU[:, :, None] * diffs ** 2).sum(axis=0) / (w[:, None] + 1e-12)
            target_sigmas = np.sqrt(var + 1e-6)
            
            self._sigmas[active_mask] = (1 - lr) * self._sigmas[active_mask] + lr * target_sigmas[active_mask]

        # Жорстке обмеження сигми (щоб не схлопувалась)
        self._sigmas = np.clip(self._sigmas, self.sigma_min, self.sigma_max)
        
        # Оновлення ваг
        if self.weights.size == 0:
            self.weights = w.copy()
        elif self.weights.shape[0] == w.shape[0]:
             self.weights = self.weights * 0.99 + w # Забуваємо старе
        else:
            self.weights = w.copy()

        self._update_alpha(Xn, MU)

        # --- 3. ОБСЛУГОВУВАННЯ ---
        self._update_counter += 1
        if self._update_counter % self._merge_every == 0:
            self._merge_clusters()
        if self._update_counter % self._split_every == 0:
            self._split_clusters()
        
        # Очистка сміття кожні 20 тактів
        if self._update_counter % 20 == 0:
            self._prune_clusters()

    def _add_new_clusters(self, centers):
        if len(centers) == 0: return
        # Vectorized append
        self._centers = np.vstack([self._centers, centers])
        
        new_sigmas = np.tile(np.full((1, self.dim), self.sigma_init), (len(centers), 1))
        self._sigmas = np.vstack([self._sigmas, new_sigmas])
        
        mid_alpha = 0.5 * (self.alpha_min + self.alpha_max)
        self.alphas = np.append(self.alphas, np.full(len(centers), mid_alpha))
        
        self.weights = np.append(self.weights, np.zeros(len(centers)))

    def _prune_clusters(self):
        """Видаляє слабкі та непотрібні кластери."""
        K = len(self._centers)
        if K <= 1: return
        
        # Видаляємо кластери з вагою < 1.0 (майже порожні)
        keep_mask = self.weights > 1.0
        
        # Завжди залишаємо найсильніший кластер
        if not np.any(keep_mask):
            keep_mask[np.argmax(self.weights)] = True
            
        if not np.all(keep_mask):
            self._centers = self._centers[keep_mask]
            self._sigmas = self._sigmas[keep_mask]
            self.alphas = self.alphas[keep_mask]
            self.weights = self.weights[keep_mask]

    # ======================================================
    # Alpha, Merge, Split, Infer (Optimized KDTree)
    # ======================================================
    def _update_alpha(self, Xn, MU):
        speeds = Xn[:, 0]
        w = MU.sum(axis=0)
        avg = (MU * speeds[:, None]).sum(axis=0) / (w + 1e-12)
        var = (MU * (speeds[:, None] - avg) ** 2).sum(axis=0) / (w + 1e-12)
        inst = np.sqrt(var)
        if inst.max() > 1e-8: inst /= inst.max()
        self.alphas = self.alpha_min + inst * (self.alpha_max - self.alpha_min)
        self.alphas = np.clip(self.alphas, self.alpha_min, self.alpha_max)

    def _merge_clusters(self):
        K = len(self._centers)
        if K < 2: return
        tree = KDTree(self._centers)
        dist, idx = tree.query(self._centers, k=2)
        merged_idx = set()
        
        for i in range(K):
            if i in merged_idx: continue
            j = idx[i, 1]
            if j in merged_idx or i == j: continue
            d = dist[i, 1]
            
            # Поріг злиття
            thr = self.merge_factor * min(np.linalg.norm(self._sigmas[i]), np.linalg.norm(self._sigmas[j]))
            
            if d < thr:
                # Merge logic
                w1, w2 = self.weights[i], self.weights[j]
                W = w1 + w2 + 1e-12
                
                self._centers[i] = (w1 * self._centers[i] + w2 * self._centers[j]) / W
                self._sigmas[i] = (w1 * self._sigmas[i] + w2 * self._sigmas[j]) / W
                self.alphas[i] = (w1 * self.alphas[i] + w2 * self.alphas[j]) / W
                self.weights[i] = W
                
                merged_idx.add(j)

        if merged_idx:
            keep = [i for i in range(K) if i not in merged_idx]
            self._filter_keep(keep)

    def _split_clusters(self):
        K = len(self._centers)
        # Не сплітим, якщо вже досягли ліміту
        if K < 2 or K >= self.max_clusters: return
        
        tree = KDTree(self._centers)
        dist, idx = tree.query(self._centers, k=2)
        
        for i in range(K):
            d = dist[i, 1]
            if d == 0 or not np.isfinite(d): continue
            
            radius = np.linalg.norm(self._sigmas[i])
            if radius > self.split_factor * d:
                self._do_split(i)
                return # Сплітим тільки один за раз для стабільності

    def _do_split(self, i):
        if len(self._centers) >= self.max_clusters: return
        
        c = self._centers[i]
        s = self._sigmas[i]
        
        direction = np.random.randn(self.dim)
        direction /= np.linalg.norm(direction) + 1e-12
        
        shift = 0.5 * s * direction
        c1, c2 = c + shift, c - shift
        
        sigma_new = np.clip(s * 0.7, self.sigma_min, self.sigma_max)
        
        # Оновлюємо старий
        self._centers[i] = c1
        self._sigmas[i] = sigma_new
        self.weights[i] *= 0.5
        
        # Додаємо новий
        self._add_new_clusters(c2[None, :])
        
        # Копіюємо параметри для нового
        last_idx = len(self._centers) - 1
        self._sigmas[last_idx] = sigma_new
        self.alphas[last_idx] = self.alphas[i]
        self.weights[last_idx] = self.weights[i]

    def _filter_keep(self, keep_indices):
        self._centers = self._centers[keep_indices]
        self._sigmas = self._sigmas[keep_indices]
        self.alphas = self.alphas[keep_indices]
        self.weights = self.weights[keep_indices]
        
    def infer_alpha(self, x):
        if len(self._centers) == 0 or not self._robust_initialized:
            return 0.5 * (self.alpha_min + self.alpha_max)
        
        x = np.asarray(x, float)
        xn = self._norm(x[None, :])[0]
        
        diffs = self._centers - xn
        denom = 2 * (self._sigmas ** 2)
        d2 = np.sum((diffs ** 2) / denom, axis=1)
        
        MU = np.exp(-d2)
        
        if MU.sum() < 1e-12: 
            return 0.5 * (self.alpha_min + self.alpha_max)
            
        return float(np.sum(MU * self.alphas) / MU.sum())