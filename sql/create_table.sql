
CREATE TABLE project.project_data (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    gender VARCHAR(1),
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_per_item DECIMAL(10,2) NOT NULL CHECK (price_per_item >= 0),
    discount_per_item DECIMAL(10,2) DEFAULT 0 CHECK (discount_per_item >= 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price >= 0),
    purchase_datetime TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Проверка на правильность заполнения
ALTER TABLE project.project_data 
ADD CONSTRAINT valid_discount CHECK (discount_per_item <= price_per_item),
ADD CONSTRAINT valid_total_price CHECK (total_price = (price_per_item - discount_per_item) * quantity);

-- Комментарии к таблице и столбцам
COMMENT ON TABLE project.project_data IS 'Таблица данных о покупках клиентов';
COMMENT ON COLUMN project.project_data.client_id IS 'Идентификатор клиента';
COMMENT ON COLUMN project.project_data.gender IS 'Пол клиента (M/F)';
COMMENT ON COLUMN project.project_data.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN project.project_data.quantity IS 'Количество товаров в покупке';
COMMENT ON COLUMN project.project_data.price_per_item IS 'Цена за единицу товара (в денежных единицах)';
COMMENT ON COLUMN project.project_data.discount_per_item IS 'Скидка на единицу товара (в денежных единицах)';
COMMENT ON COLUMN project.project_data.total_price IS 'Общая стоимость покупки';
COMMENT ON COLUMN project.project_data.purchase_datetime IS 'Дата и время покупки';
COMMENT ON COLUMN project.project_data.created_at IS 'Дата создания записи в базе';

-- Создание индексов для улучшения производительности
CREATE INDEX idx_project_data_client_id ON project.project_data(client_id);
CREATE INDEX idx_project_data_product_id ON project.project_data(product_id);
CREATE INDEX idx_project_data_purchase_datetime ON project.project_data(purchase_datetime);
CREATE INDEX idx_project_data_client_product ON project.project_data(client_id, product_id);

-- Вьюшка для когортного анализа
-- Запрос в DataLens для ABC/XYZ-анализа с параметрами
WITH product_sales AS (
  SELECT 
    product_id,
    DATE_TRUNC('{{scale_xyz}}', DATE(purchase_datetime)) as sales_month,
    SUM(quantity) as quantity,
    SUM(total_price) as revenue
  FROM project.project_data 
  WHERE TRUE AND DATE(purchase_datetime) >= DATE('{{start_date}}') 
             AND DATE(purchase_datetime) <= DATE('{{end_date}}')
  GROUP BY product_id, DATE_TRUNC('{{scale_xyz}}', DATE(purchase_datetime))
),
product_stats AS (
  SELECT 
    product_id,
    SUM(quantity) as total_quantity,
    SUM(revenue) as total_revenue,
    -- Статистика для XYZ анализа
    COUNT(DISTINCT sales_month) as active_months,
    -- Коэффициент вариации (для XYZ)
    CASE 
      WHEN AVG(revenue) > 0 THEN STDDEV(revenue) / AVG(revenue)
      ELSE 1 
    END as variation_coefficient
  FROM product_sales
  GROUP BY product_id
  HAVING COUNT(DISTINCT sales_month) >= 2  -- Минимум 2 месяца данных для анализа
),
abc_analysis AS (
  SELECT 
    product_id,
    total_quantity,
    total_revenue,
    active_months,
    variation_coefficient,
    -- ABC по количеству
    SUM(total_quantity) OVER(ORDER BY total_quantity DESC) / SUM(total_quantity) OVER() as quantity_cumulative,
    -- ABC по выручке
    SUM(total_revenue) OVER(ORDER BY total_revenue DESC) / SUM(total_revenue) OVER() as revenue_cumulative
  FROM product_stats
)
SELECT
  product_id,
  total_quantity,
  total_revenue,
  -- ABC анализ
  CASE 
    WHEN quantity_cumulative <= 0.8 THEN 'A'
    WHEN quantity_cumulative <= 0.95 THEN 'B' 
    ELSE 'C'
  END as abc_quantity,
  CASE 
    WHEN revenue_cumulative <= 0.8 THEN 'A'
    WHEN revenue_cumulative <= 0.95 THEN 'B' 
    ELSE 'C'
  END as abc_total_price,
  -- XYZ анализ
  CASE 
    WHEN variation_coefficient <= 0.1 THEN 'X'
    WHEN variation_coefficient <= 0.25 THEN 'Y'
    ELSE 'Z'
  END as xyz_category,
  -- Дополнительные метрики
  active_months
FROM abc_analysis
ORDER BY total_revenue DESC

-- Запрос в DataLens для формирования RFM-анализа с параметрами
WITH client_rfm_data AS (
  SELECT 
    client_id,
    -- Дата первого заказа
    MIN(purchase_datetime::date) as first_order_date,
    -- Recency: количество дней с последней покупки
    CURRENT_DATE - MAX(purchase_datetime::date) as recency_days,
    -- Frequency: количество уникальных заказов
    COUNT(DISTINCT purchase_datetime::date) as frequency,
    -- Monetary: общая сумма покупок
    SUM(total_price) as monetary
  FROM project.project_data
  WHERE purchase_datetime::date >= '{{start_date}}' 
    AND purchase_datetime::date <= '{{end_date}}'
  GROUP BY client_id
),
rfm_scores AS (
  SELECT 
    client_id,
    first_order_date,
    recency_days,
    frequency,
    monetary,
    -- RFM Scores (1-3, где 3 - лучший)
    NTILE(3) OVER (ORDER BY recency_days ASC) as r_score,   -- Чем меньше дней, тем лучше
    NTILE(3) OVER (ORDER BY frequency DESC) as f_score,     -- Чем больше покупок, тем лучше
    NTILE(3) OVER (ORDER BY monetary DESC) as m_score       -- Чем больше сумма, тем лучше
  FROM client_rfm_data
)
SELECT 
  client_id,
  first_order_date,
  r_score as R,
  f_score as F,
  m_score as M
FROM rfm_scores
ORDER BY R DESC, F DESC, M DESC

-- Запрос в DataLens для когортного анализа с параметрами
WITH first_purchases AS (
  SELECT 
    client_id,
    MIN(purchase_datetime::date) as purchase_min_date
  FROM project.project_data
  WHERE purchase_datetime::date >= '{{start_date}}' 
    AND purchase_datetime::date <= '{{end_date}}'
  GROUP BY client_id
),
cohort_data AS (
  SELECT 
    p.client_id,
    f.purchase_min_date,
    p.purchase_datetime::date as purchase_date
  FROM project.project_data p
  JOIN first_purchases f ON p.client_id = f.client_id
  WHERE p.purchase_datetime::date >= '{{start_date}}' 
    AND p.purchase_datetime::date <= '{{end_date}}'
)
SELECT 
  client_id,
  purchase_min_date,
  purchase_date
FROM cohort_data
ORDER BY client_id, purchase_date