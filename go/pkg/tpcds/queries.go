package tpcds

// BenchmarkQueries is the canonical set of TPC-DS queries the
// iceberg-janitor benchmark runs before and after compaction. These
// are direct ports of tests/test_tpcds_benchmark.py's BENCHMARK_QUERIES
// dict — the same SQL strings, the same {ns} placeholder for the
// namespace, the same multi-table joins. Keeping the queries
// identical means the Go and Python benchmark numbers are directly
// comparable.
//
// All 10 queries reference the 12 tables in this package's schemas.go
// (3 fact tables + 9 dimension tables). The other TPC-DS tables
// (web_sales, web_returns, catalog_returns, inventory, etc.) are not
// in scope for the MVP.
//
// To run a query against DuckDB, substitute {ns} with the actual
// namespace name (e.g. "tpcds") and pass to iceberg_scan(). The
// existing make mvp-query target shows the connection setup.
var BenchmarkQueries = map[string]string{
	// Q1: Top customers by store returns (store_returns + customer + store + date_dim)
	"q1_top_return_customers": `
		SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
		       sum(sr.sr_return_amt) as total_returns
		FROM {ns}.store_returns sr
		JOIN {ns}.customer c ON sr.sr_customer_sk = c.c_customer_sk
		JOIN {ns}.store s ON sr.sr_store_sk = s.s_store_sk
		JOIN {ns}.date_dim d ON sr.sr_returned_date_sk = d.d_date_sk
		WHERE d.d_year = 2022
		GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
		ORDER BY total_returns DESC
		LIMIT 100
	`,

	// Q3: Brand revenue by year (store_sales + item + date_dim)
	"q3_brand_revenue": `
		SELECT d.d_year, i.i_brand, i.i_brand_id,
		       sum(ss.ss_ext_sales_price) as revenue
		FROM {ns}.store_sales ss
		JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE i.i_category = 'Electronics'
		GROUP BY d.d_year, i.i_brand, i.i_brand_id
		ORDER BY d.d_year, revenue DESC
		LIMIT 100
	`,

	// Q7: Promotion impact (store_sales + item + customer_demographics + promotion + date_dim)
	"q7_promo_impact": `
		SELECT i.i_item_id,
		       avg(ss.ss_quantity) as avg_qty,
		       avg(ss.ss_list_price) as avg_price,
		       avg(ss.ss_coupon_amt) as avg_coupon,
		       avg(ss.ss_sales_price) as avg_sales
		FROM {ns}.store_sales ss
		JOIN {ns}.customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
		JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
		JOIN {ns}.promotion p ON ss.ss_promo_sk = p.p_promo_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE cd.cd_gender = 'F' AND cd.cd_marital_status = 'S' AND d.d_year = 2023
		GROUP BY i.i_item_id
		ORDER BY i.i_item_id
		LIMIT 100
	`,

	// Q13: Store sales by demographics (store_sales + store + customer_demographics
	//       + customer_address + household_demographics + date_dim)
	"q13_demo_store_sales": `
		SELECT avg(ss.ss_quantity) as avg_qty,
		       avg(ss.ss_ext_sales_price) as avg_ext_price,
		       avg(ss.ss_ext_wholesale_cost) as avg_wholesale,
		       sum(ss.ss_ext_wholesale_cost) as total_wholesale
		FROM {ns}.store_sales ss
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		JOIN {ns}.customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
		JOIN {ns}.customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE cd.cd_marital_status = 'M'
		  AND d.d_year = 2022
		  AND ca.ca_state IN ('CA', 'NY', 'TX')
	`,

	// Q19: Revenue by brand-manager-zip (store_sales + item + customer + customer_address + store + date_dim)
	"q19_brand_manager_zip": `
		SELECT i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact,
		       sum(ss.ss_ext_sales_price) as revenue
		FROM {ns}.store_sales ss
		JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
		JOIN {ns}.customer c ON ss.ss_customer_sk = c.c_customer_sk
		JOIN {ns}.customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE d.d_moy = 11 AND d.d_year = 2023
		  AND ca.ca_state != s.s_state
		GROUP BY i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact
		ORDER BY revenue DESC
		LIMIT 100
	`,

	// Q25: Cross-channel returns (store_sales + store_returns + catalog_sales + date_dim + store + item)
	"q25_cross_channel_returns": `
		SELECT i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name,
		       sum(ss.ss_net_profit) as store_profit,
		       sum(sr.sr_net_loss) as return_loss
		FROM {ns}.store_sales ss
		JOIN {ns}.store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
		    AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number
		JOIN {ns}.catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
		    AND sr.sr_item_sk = cs.cs_item_sk
		JOIN {ns}.date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
		JOIN {ns}.date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
		WHERE d1.d_year = 2022
		GROUP BY i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name
		ORDER BY i.i_item_id, store_profit
		LIMIT 100
	`,

	// Q43: Weekly store sales (store_sales + store + date_dim)
	"q43_weekly_store_sales": `
		SELECT s.s_store_name, s.s_store_id,
		       sum(CASE WHEN d.d_dow = 0 THEN ss.ss_sales_price ELSE 0 END) as sun_sales,
		       sum(CASE WHEN d.d_dow = 1 THEN ss.ss_sales_price ELSE 0 END) as mon_sales,
		       sum(CASE WHEN d.d_dow = 2 THEN ss.ss_sales_price ELSE 0 END) as tue_sales,
		       sum(CASE WHEN d.d_dow = 3 THEN ss.ss_sales_price ELSE 0 END) as wed_sales,
		       sum(CASE WHEN d.d_dow = 4 THEN ss.ss_sales_price ELSE 0 END) as thu_sales,
		       sum(CASE WHEN d.d_dow = 5 THEN ss.ss_sales_price ELSE 0 END) as fri_sales,
		       sum(CASE WHEN d.d_dow = 6 THEN ss.ss_sales_price ELSE 0 END) as sat_sales
		FROM {ns}.store_sales ss
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE d.d_year = 2023
		GROUP BY s.s_store_name, s.s_store_id
		ORDER BY s.s_store_name
		LIMIT 100
	`,

	// Q46: Customer spend by city (store_sales + customer + customer_address + date_dim + household_demographics + store)
	"q46_customer_spend_city": `
		SELECT c.c_last_name, c.c_first_name, ca.ca_city,
		       sum(ss.ss_net_paid) as total_spend
		FROM {ns}.store_sales ss
		JOIN {ns}.customer c ON ss.ss_customer_sk = c.c_customer_sk
		JOIN {ns}.customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
		JOIN {ns}.household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		WHERE d.d_year IN (2022, 2023)
		  AND hd.hd_dep_count >= 2
		GROUP BY c.c_last_name, c.c_first_name, ca.ca_city
		ORDER BY total_spend DESC
		LIMIT 100
	`,

	// Q55: Brand revenue by month (store_sales + item + date_dim)
	"q55_brand_revenue_monthly": `
		SELECT i.i_brand_id, i.i_brand,
		       sum(ss.ss_ext_sales_price) as total_revenue
		FROM {ns}.store_sales ss
		JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
		JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
		WHERE d.d_moy = 6 AND d.d_year = 2023
		  AND i.i_manager_id = 15
		GROUP BY i.i_brand_id, i.i_brand
		ORDER BY total_revenue DESC
		LIMIT 100
	`,

	// Q96: Order count by time/shift (store_sales + time_dim + household_demographics + store)
	"q96_order_by_shift": `
		SELECT count(*) as order_count
		FROM {ns}.store_sales ss
		JOIN {ns}.time_dim t ON ss.ss_sold_time_sk = t.t_time_sk
		JOIN {ns}.household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
		JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
		WHERE t.t_hour = 14
		  AND hd.hd_dep_count = 3
	`,
}

// QueryNames returns the benchmark query names in a stable order.
// Used by the bench runner so the comparison report has consistent
// row ordering across runs.
func QueryNames() []string {
	return []string{
		"q1_top_return_customers",
		"q3_brand_revenue",
		"q7_promo_impact",
		"q13_demo_store_sales",
		"q19_brand_manager_zip",
		"q25_cross_channel_returns",
		"q43_weekly_store_sales",
		"q46_customer_spend_city",
		"q55_brand_revenue_monthly",
		"q96_order_by_shift",
	}
}
