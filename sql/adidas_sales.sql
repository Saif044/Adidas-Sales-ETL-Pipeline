DROP TABLE public.adidas_sales;

CREATE TABLE public.adidas_sales (
    retailer VARCHAR(100),
    retailer_id VARCHAR(50), -- Changed to VARCHAR just in case
    invoice_date DATE,
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    product VARCHAR(100),
    price_per_unit VARCHAR(50), -- Changed to VARCHAR to accept '$'
    units_sold VARCHAR(50),     -- Changed to VARCHAR to accept ','
    total_sales VARCHAR(50),    -- Changed to VARCHAR to accept '$'
    operating_profit VARCHAR(50),-- Changed to VARCHAR to accept '$'
    operating_margin VARCHAR(50),-- Changed to VARCHAR to accept '%'
    sales_method VARCHAR(50)
);

SELECT * FROM  public.adidas_sales;
SELECT COUNT(*) FROM public.adidas_sales;

CREATE TABLE public.adidas_sales_summary (
    product VARCHAR(100),
    total_units_sold INTEGER,
    total_profit NUMERIC(15, 2),
    profit_per_unit NUMERIC(15, 2)
);

SELECT * FROM adidas_sales_summary


