import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

# ==========================================
# 1. СОЗДАНИЕ ФЕЙКОВОГО ДАТАСЕТА (для теста)
# ==========================================
np.random.seed(42)
n_samples = 1000

# Генерируем случайные признаки (0 или 1 для подтверждений, числа для индикаторов)
data = {
    'morris_conf': np.random.randint(0, 2, n_samples),  # 1 - было подтверждение, 0 - не было
    'level_conf': np.random.randint(0, 2, n_samples),   # 1 - коснулось уровня, 0 - нет
    'rsi': np.random.uniform(20, 80, n_samples),        # RSI от 20 до 80
    'sma_up': np.random.randint(0, 2, n_samples),       # 1 - тренд вверх, 0 - вниз
}
df = pd.DataFrame(data)

# Имитируем зависимость таргета от признаков (чтобы модель нашла закономерность)
# Допустим, уровень дает сильный сигнал вверх, а RSI > 70 тянет вниз
probability = (
    df['morris_conf'] * 1 + 
    df['level_conf'] * 1.5 + 
    (50 - df['rsi']) * 0.05 + 
    df['sma_up'] * 0.5
)
# Превращаем вероятность в бинарный таргет (1 - пошло вверх, 0 - пошло вниз)
df['target'] = (probability + np.random.normal(0, 1, n_samples) > 1.0).astype(int)

# ==========================================
# 2. ПОДГОТОВКА ДАННЫХ И ОБУЧЕНИЕ МОДЕЛИ
# ==========================================
# Разделяем на фичи (X) и таргет (y)
X = df[['morris_conf', 'level_conf', 'rsi', 'sma_up']]
y = df['target']

# Бьем на обучающую (80%) и тестовую (20%) выборки
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Создаем модель. class_weight='balanced' помогает, если успешных сигналов меньше, чем убыточных
model = LogisticRegression(class_weight='balanced')

# ОБУЧЕНИЕ
model.fit(X_train, y_train)

# Проверка точности
y_pred = model.predict(X_test)
print("--- МЕТРИКИ КАЧЕСТВА ---")
print(f"Точность (Accuracy): {accuracy_score(y_test, y_pred):.2f}")
print(classification_report(y_test, y_pred))

# ==========================================
# 3. ИЗВЛЕЧЕНИЕ ВЕСОВ (Твои новые баллы)
# ==========================================
print("\n--- ВЫЧИСЛЕННЫЕ БАЛЛЫ ДЛЯ ТЕЛЕГРАМ-БОТА ---")
weights = model.coef_[0]
features = X.columns

for feature, weight in zip(features, weights):
    # Округляем до двух знаков для красоты
    score = round(weight, 2)
    print(f"{feature:<15} : {score} баллов")

print(f"\nБазовое смещение (intercept): {round(model.intercept_[0], 2)}")