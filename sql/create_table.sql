
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
CREATE VIEW project.cohort_analysis AS
(
SELECT
    client_id,
    min(date(purchase_datetime)) OVER (PARTITION BY client_id ORDER BY purchase_datetime) AS purchase_min_date,
    date(purchase_datetime) AS purchase_date 
FROM project.project_data
)

-- Запрос для ABC анализа
CREATE VIEW project.abc_analysis AS
(
WITH product_stats AS (
  SELECT 
    product_id,
    DATE(purchase_datetime) as purchase_date, 
    SUM(quantity) as total_quantity,
    SUM(total_price) as total_revenue
  FROM project.project_data
  GROUP BY product_id, DATE(purchase_datetime)
)
SELECT 
  product_id,
  purchase_date,
  total_quantity,
  total_revenue,
  CASE 
    WHEN SUM(total_quantity) OVER(ORDER BY total_quantity DESC) / SUM(total_quantity) OVER() <= 0.8 THEN 'A'
    WHEN SUM(total_quantity) OVER(ORDER BY total_quantity DESC) / SUM(total_quantity) OVER() <= 0.95 THEN 'B' 
    ELSE 'C'
  END as abc_quantity,
  CASE 
    WHEN SUM(total_revenue) OVER(ORDER BY total_revenue DESC) / SUM(total_revenue) OVER() <= 0.8 THEN 'A'
    WHEN SUM(total_revenue) OVER(ORDER BY total_revenue DESC) / SUM(total_revenue) OVER() <= 0.95 THEN 'B' 
    ELSE 'C'
  END as abc_total_price
FROM product_stats
ORDER BY total_revenue DESC
)

-- Вообще, я использовал этот запрос для формирования SQL-датасетя с параметрами прямо в DataLens
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