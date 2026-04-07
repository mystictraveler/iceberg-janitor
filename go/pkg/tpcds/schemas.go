// Package tpcds provides Go-side TPC-DS schemas, data generators, and
// benchmark queries for the iceberg-janitor streaming benchmark
// harness. The schemas, generator behavior, and queries are direct
// ports of the corresponding pieces in the Python implementation
// under scripts/tpcds_schema.py, scripts/tpcds_datagen.py, and
// tests/test_tpcds_benchmark.py — they produce the same logical
// shape so the Go and Python benchmarks are comparable.
//
// Scope: 12 of the 24 TPC-DS tables — the subset referenced by the 10
// canonical benchmark queries. 3 fact tables (store_sales,
// store_returns, catalog_sales) plus 9 dimension tables (date_dim,
// time_dim, item, customer, customer_address, customer_demographics,
// household_demographics, store, promotion). The other 12 TPC-DS
// tables (web_sales, catalog_returns, web_returns, inventory, plus
// the rarely-used dims) are not in this MVP — they can be added later
// if a query needs them.
package tpcds

import (
	icebergpkg "github.com/apache/iceberg-go"
)

// Iceberg field IDs are stable across the schema definition. We pin
// them here so the schema is deterministic across runs.
//
// IDs use a contiguous range per table, starting at 1.

// Field constructors keep this file readable. iceberg-go uses
// PrimitiveTypes for the basic types we need.
var (
	tInt32       = icebergpkg.PrimitiveTypes.Int32
	tInt64       = icebergpkg.PrimitiveTypes.Int64
	tFloat64     = icebergpkg.PrimitiveTypes.Float64
	tString      = icebergpkg.PrimitiveTypes.String
	tDate        = icebergpkg.PrimitiveTypes.Date
)

// nf is shorthand for an optional NestedField (Required: false).
func nf(id int, name string, typ icebergpkg.Type) icebergpkg.NestedField {
	return icebergpkg.NestedField{ID: id, Name: name, Type: typ, Required: false}
}

// === Fact tables (3) ===

var StoreSales = icebergpkg.NewSchema(0,
	nf(1, "ss_sold_date_sk", tInt64),
	nf(2, "ss_sold_time_sk", tInt64),
	nf(3, "ss_item_sk", tInt64),
	nf(4, "ss_customer_sk", tInt64),
	nf(5, "ss_cdemo_sk", tInt64),
	nf(6, "ss_hdemo_sk", tInt64),
	nf(7, "ss_addr_sk", tInt64),
	nf(8, "ss_store_sk", tInt64),
	nf(9, "ss_promo_sk", tInt64),
	nf(10, "ss_ticket_number", tInt64),
	nf(11, "ss_quantity", tInt32),
	nf(12, "ss_wholesale_cost", tFloat64),
	nf(13, "ss_list_price", tFloat64),
	nf(14, "ss_sales_price", tFloat64),
	nf(15, "ss_ext_discount_amt", tFloat64),
	nf(16, "ss_ext_sales_price", tFloat64),
	nf(17, "ss_ext_wholesale_cost", tFloat64),
	nf(18, "ss_ext_list_price", tFloat64),
	nf(19, "ss_ext_tax", tFloat64),
	nf(20, "ss_coupon_amt", tFloat64),
	nf(21, "ss_net_paid", tFloat64),
	nf(22, "ss_net_paid_inc_tax", tFloat64),
	nf(23, "ss_net_profit", tFloat64),
)

var StoreReturns = icebergpkg.NewSchema(0,
	nf(1, "sr_returned_date_sk", tInt64),
	nf(2, "sr_return_time_sk", tInt64),
	nf(3, "sr_item_sk", tInt64),
	nf(4, "sr_customer_sk", tInt64),
	nf(5, "sr_cdemo_sk", tInt64),
	nf(6, "sr_hdemo_sk", tInt64),
	nf(7, "sr_addr_sk", tInt64),
	nf(8, "sr_store_sk", tInt64),
	nf(9, "sr_reason_sk", tInt64),
	nf(10, "sr_ticket_number", tInt64),
	nf(11, "sr_return_quantity", tInt32),
	nf(12, "sr_return_amt", tFloat64),
	nf(13, "sr_return_tax", tFloat64),
	nf(14, "sr_return_amt_inc_tax", tFloat64),
	nf(15, "sr_fee", tFloat64),
	nf(16, "sr_return_ship_cost", tFloat64),
	nf(17, "sr_refunded_cash", tFloat64),
	nf(18, "sr_reversed_charge", tFloat64),
	nf(19, "sr_store_credit", tFloat64),
	nf(20, "sr_net_loss", tFloat64),
)

var CatalogSales = icebergpkg.NewSchema(0,
	nf(1, "cs_sold_date_sk", tInt64),
	nf(2, "cs_sold_time_sk", tInt64),
	nf(3, "cs_ship_date_sk", tInt64),
	nf(4, "cs_bill_customer_sk", tInt64),
	nf(5, "cs_bill_cdemo_sk", tInt64),
	nf(6, "cs_bill_hdemo_sk", tInt64),
	nf(7, "cs_bill_addr_sk", tInt64),
	nf(8, "cs_ship_customer_sk", tInt64),
	nf(9, "cs_ship_cdemo_sk", tInt64),
	nf(10, "cs_ship_hdemo_sk", tInt64),
	nf(11, "cs_ship_addr_sk", tInt64),
	nf(12, "cs_call_center_sk", tInt64),
	nf(13, "cs_catalog_page_sk", tInt64),
	nf(14, "cs_ship_mode_sk", tInt64),
	nf(15, "cs_warehouse_sk", tInt64),
	nf(16, "cs_item_sk", tInt64),
	nf(17, "cs_promo_sk", tInt64),
	nf(18, "cs_order_number", tInt64),
	nf(19, "cs_quantity", tInt32),
	nf(20, "cs_wholesale_cost", tFloat64),
	nf(21, "cs_list_price", tFloat64),
	nf(22, "cs_sales_price", tFloat64),
	nf(23, "cs_ext_discount_amt", tFloat64),
	nf(24, "cs_ext_sales_price", tFloat64),
	nf(25, "cs_ext_wholesale_cost", tFloat64),
	nf(26, "cs_ext_list_price", tFloat64),
	nf(27, "cs_ext_tax", tFloat64),
	nf(28, "cs_coupon_amt", tFloat64),
	nf(29, "cs_ext_ship_cost", tFloat64),
	nf(30, "cs_net_paid", tFloat64),
	nf(31, "cs_net_paid_inc_tax", tFloat64),
	nf(32, "cs_net_paid_inc_ship", tFloat64),
	nf(33, "cs_net_paid_inc_ship_tax", tFloat64),
	nf(34, "cs_net_profit", tFloat64),
)

// === Dimension tables (9) ===

var DateDim = icebergpkg.NewSchema(0,
	nf(1, "d_date_sk", tInt64),
	nf(2, "d_date_id", tString),
	nf(3, "d_date", tDate),
	nf(4, "d_month_seq", tInt32),
	nf(5, "d_week_seq", tInt32),
	nf(6, "d_quarter_seq", tInt32),
	nf(7, "d_year", tInt32),
	nf(8, "d_dow", tInt32),
	nf(9, "d_moy", tInt32),
	nf(10, "d_dom", tInt32),
	nf(11, "d_qoy", tInt32),
	nf(12, "d_fy_year", tInt32),
	nf(13, "d_fy_quarter_seq", tInt32),
	nf(14, "d_fy_week_seq", tInt32),
	nf(15, "d_day_name", tString),
	nf(16, "d_quarter_name", tString),
	nf(17, "d_holiday", tString),
	nf(18, "d_weekend", tString),
	nf(19, "d_following_holiday", tString),
	nf(20, "d_first_dom", tInt32),
	nf(21, "d_last_dom", tInt32),
	nf(22, "d_same_day_ly", tInt32),
	nf(23, "d_same_day_lq", tInt32),
	nf(24, "d_current_day", tString),
	nf(25, "d_current_week", tString),
	nf(26, "d_current_month", tString),
	nf(27, "d_current_quarter", tString),
	nf(28, "d_current_year", tString),
)

var TimeDim = icebergpkg.NewSchema(0,
	nf(1, "t_time_sk", tInt64),
	nf(2, "t_time_id", tString),
	nf(3, "t_time", tInt32),
	nf(4, "t_hour", tInt32),
	nf(5, "t_minute", tInt32),
	nf(6, "t_second", tInt32),
	nf(7, "t_am_pm", tString),
	nf(8, "t_shift", tString),
	nf(9, "t_sub_shift", tString),
	nf(10, "t_meal_time", tString),
)

var Item = icebergpkg.NewSchema(0,
	nf(1, "i_item_sk", tInt64),
	nf(2, "i_item_id", tString),
	nf(3, "i_rec_start_date", tDate),
	nf(4, "i_rec_end_date", tDate),
	nf(5, "i_item_desc", tString),
	nf(6, "i_current_price", tFloat64),
	nf(7, "i_wholesale_cost", tFloat64),
	nf(8, "i_brand_id", tInt32),
	nf(9, "i_brand", tString),
	nf(10, "i_class_id", tInt32),
	nf(11, "i_class", tString),
	nf(12, "i_category_id", tInt32),
	nf(13, "i_category", tString),
	nf(14, "i_manufact_id", tInt32),
	nf(15, "i_manufact", tString),
	nf(16, "i_size", tString),
	nf(17, "i_formulation", tString),
	nf(18, "i_color", tString),
	nf(19, "i_units", tString),
	nf(20, "i_container", tString),
	nf(21, "i_manager_id", tInt32),
	nf(22, "i_product_name", tString),
)

var Customer = icebergpkg.NewSchema(0,
	nf(1, "c_customer_sk", tInt64),
	nf(2, "c_customer_id", tString),
	nf(3, "c_current_cdemo_sk", tInt64),
	nf(4, "c_current_hdemo_sk", tInt64),
	nf(5, "c_current_addr_sk", tInt64),
	nf(6, "c_first_shipto_date_sk", tInt64),
	nf(7, "c_first_sales_date_sk", tInt64),
	nf(8, "c_salutation", tString),
	nf(9, "c_first_name", tString),
	nf(10, "c_last_name", tString),
	nf(11, "c_preferred_cust_flag", tString),
	nf(12, "c_birth_day", tInt32),
	nf(13, "c_birth_month", tInt32),
	nf(14, "c_birth_year", tInt32),
	nf(15, "c_birth_country", tString),
	nf(16, "c_login", tString),
	nf(17, "c_email_address", tString),
	nf(18, "c_last_review_date_sk", tInt64),
)

var CustomerAddress = icebergpkg.NewSchema(0,
	nf(1, "ca_address_sk", tInt64),
	nf(2, "ca_address_id", tString),
	nf(3, "ca_street_number", tString),
	nf(4, "ca_street_name", tString),
	nf(5, "ca_street_type", tString),
	nf(6, "ca_suite_number", tString),
	nf(7, "ca_city", tString),
	nf(8, "ca_county", tString),
	nf(9, "ca_state", tString),
	nf(10, "ca_zip", tString),
	nf(11, "ca_country", tString),
	nf(12, "ca_gmt_offset", tFloat64),
	nf(13, "ca_location_type", tString),
)

var CustomerDemographics = icebergpkg.NewSchema(0,
	nf(1, "cd_demo_sk", tInt64),
	nf(2, "cd_gender", tString),
	nf(3, "cd_marital_status", tString),
	nf(4, "cd_education_status", tString),
	nf(5, "cd_purchase_estimate", tInt32),
	nf(6, "cd_credit_rating", tString),
	nf(7, "cd_dep_count", tInt32),
	nf(8, "cd_dep_employed_count", tInt32),
	nf(9, "cd_dep_college_count", tInt32),
)

var HouseholdDemographics = icebergpkg.NewSchema(0,
	nf(1, "hd_demo_sk", tInt64),
	nf(2, "hd_income_band_sk", tInt64),
	nf(3, "hd_buy_potential", tString),
	nf(4, "hd_dep_count", tInt32),
	nf(5, "hd_vehicle_count", tInt32),
)

var Store = icebergpkg.NewSchema(0,
	nf(1, "s_store_sk", tInt64),
	nf(2, "s_store_id", tString),
	nf(3, "s_rec_start_date", tDate),
	nf(4, "s_rec_end_date", tDate),
	nf(5, "s_closed_date_sk", tInt64),
	nf(6, "s_store_name", tString),
	nf(7, "s_number_employees", tInt32),
	nf(8, "s_floor_space", tInt32),
	nf(9, "s_hours", tString),
	nf(10, "s_manager", tString),
	nf(11, "s_market_id", tInt32),
	nf(12, "s_geography_class", tString),
	nf(13, "s_market_desc", tString),
	nf(14, "s_market_manager", tString),
	nf(15, "s_division_id", tInt32),
	nf(16, "s_division_name", tString),
	nf(17, "s_company_id", tInt32),
	nf(18, "s_company_name", tString),
	nf(19, "s_street_number", tString),
	nf(20, "s_street_name", tString),
	nf(21, "s_street_type", tString),
	nf(22, "s_suite_number", tString),
	nf(23, "s_city", tString),
	nf(24, "s_county", tString),
	nf(25, "s_state", tString),
	nf(26, "s_zip", tString),
	nf(27, "s_country", tString),
	nf(28, "s_gmt_offset", tFloat64),
	nf(29, "s_tax_percentage", tFloat64),
)

var Promotion = icebergpkg.NewSchema(0,
	nf(1, "p_promo_sk", tInt64),
	nf(2, "p_promo_id", tString),
	nf(3, "p_start_date_sk", tInt64),
	nf(4, "p_end_date_sk", tInt64),
	nf(5, "p_item_sk", tInt64),
	nf(6, "p_cost", tFloat64),
	nf(7, "p_response_target", tInt32),
	nf(8, "p_promo_name", tString),
	nf(9, "p_channel_dmail", tString),
	nf(10, "p_channel_email", tString),
	nf(11, "p_channel_catalog", tString),
	nf(12, "p_channel_tv", tString),
	nf(13, "p_channel_radio", tString),
	nf(14, "p_channel_press", tString),
	nf(15, "p_channel_event", tString),
	nf(16, "p_channel_demo", tString),
	nf(17, "p_channel_details", tString),
	nf(18, "p_purpose", tString),
	nf(19, "p_discount_active", tString),
)

// === Registry ===

// DimensionTables maps the namespace-relative table name to its schema.
var DimensionTables = map[string]*icebergpkg.Schema{
	"date_dim":                DateDim,
	"time_dim":                TimeDim,
	"item":                    Item,
	"customer":                Customer,
	"customer_address":        CustomerAddress,
	"customer_demographics":   CustomerDemographics,
	"household_demographics":  HouseholdDemographics,
	"store":                   Store,
	"promotion":               Promotion,
}

// FactTables maps the namespace-relative table name to its schema.
var FactTables = map[string]*icebergpkg.Schema{
	"store_sales":   StoreSales,
	"store_returns": StoreReturns,
	"catalog_sales": CatalogSales,
}
