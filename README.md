# Stock Market Mathematical Models

Анализ дневных доходностей активов, обращающихся на бирже NASDAQ, с точки зрения современной теории портфеля и риск-менеджмента (2021 год, 252 торговых дня).

Цель проекта — на реальных данных пройти весь классический путь от сырых цен закрытия до оценки рисков и поиска Парето-оптимальных активов, используя только открытые данные и open-source инструменты.

### Что реализовано

- Сбор исторических дневных цен закрытия через `yfinance`
- Расчёт логарифмических доходностей
- Построение «риск–доходность» карты активов (σ vs E[r])
- Поиск Парето-оптимальных активов (эффективная граница без коротких продаж)
- Оценка Value at Risk (VaR 95%) и Conditional VaR (CVaR 95%) историческим методом
- Тестирование гипотезы случайности доходностей (runs test, Ljung-Box и др.)
- Проверка нормальности распределения доходностей (Shapiro-Wilk, Jarque-Bera, Q-Q plots)
- Исследование альтернативных распределений (t-Student, skewed t, стабильные распределения)
- Дополнительный разведочный анализ: кластеризация активов, секторальные особенности, аномалии волатильности и т.д.

### Структура репозитория
```
├── data/
│   ├── raw/              # сырые данные с yfinance (as is)
│   └── processed/        # очищенные данные: удалены пропуски, делистингованные тикеры
├── \*.ipynb            # Jupyter notebooks с основным анализом и визуализациями
├── \*.py                  # .py модули (загрузка, очистка, расчёт метрик, тесты)
├── requirements.txt
└── README.md 
```

### Технологии

- Python 3.11+
- pandas, numpy
- yfinance
- matplotlib, seaborn, plotly
- scipy, statsmodels
- VS Code

### Установка 
```cmd
git clone https://github.com/yourname/stock-market-models-pet.git
cd stock-market-models-pet
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Запуск
Всего проекта сразу:
+ [nasdaq_analisys_united.ipynb](nasdaq_analisys_united.ipynb) - файл с объединёнными заданиями по анализу рынка Nasdaq. 

По частям:
+ [data_load.py](data_load.py) - загрузка данных с yahoo finance (несколько десятков минут в среднем из-за лимита количества запросов у API). Сохранение сырых данных в `data/raw`; 
+ [data_cleanup.ipynb](data_cleanup.ipynb) - очистка данных и сохранение в `data/processed`; 
+ [normality_distr_check.ipynb](normality_distr_check.ipynb) - проверка нормальности данных по самым интересным активам; 
+ [data_analisys.ipynb](data_analisys.ipynb) - применение методов анализа данных для извлечения инсайтов из данных.
