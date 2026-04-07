package tpcds

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

// Generator wraps a deterministic random source and an Arrow allocator
// so the data generators can be tested with a fixed seed and so that
// long-running streamers don't leak Arrow buffers.
type Generator struct {
	rng  *rand.Rand
	pool memory.Allocator
}

// NewGenerator constructs a Generator with a fixed seed for
// reproducibility. Pass any non-zero seed; identical seeds produce
// identical sequences across runs.
func NewGenerator(seed uint64) *Generator {
	return &Generator{
		rng:  rand.New(rand.NewPCG(seed, seed^0xdeadbeef)),
		pool: memory.NewGoAllocator(),
	}
}

// Pool returns the Arrow allocator the generator uses. Useful for
// callers that want to release records under the same pool.
func (g *Generator) Pool() memory.Allocator { return g.pool }

// === Dimension generators ===

// GenDateDim produces the full date_dim table — `n` consecutive
// dates starting at BaseDate. Default n is NumDates.
func (g *Generator) GenDateDim(n int) arrow.Record {
	if n <= 0 {
		n = NumDates
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		d := BaseDate.AddDate(0, 0, i)
		dow := int(d.Weekday())
		weekend := "N"
		if dow == 0 || dow == 6 {
			weekend = "Y"
		}
		rows[i] = map[string]any{
			"d_date_sk":           int64(i + 1),
			"d_date_id":           fmt.Sprintf("AAAAAA%06d", i),
			"d_date":              d,
			"d_month_seq":         int32(i / 30),
			"d_week_seq":          int32(i / 7),
			"d_quarter_seq":       int32(i / 90),
			"d_year":              int32(d.Year()),
			"d_dow":               int32(dow),
			"d_moy":               int32(d.Month()),
			"d_dom":               int32(d.Day()),
			"d_qoy":               int32((int(d.Month())-1)/3 + 1),
			"d_fy_year":           int32(d.Year()),
			"d_fy_quarter_seq":    int32(i / 90),
			"d_fy_week_seq":       int32(i / 7),
			"d_day_name":          d.Weekday().String(),
			"d_quarter_name":      fmt.Sprintf("%dQ%d", d.Year(), (int(d.Month())-1)/3+1),
			"d_holiday":           pick(g.rng, "Y", "N"),
			"d_weekend":           weekend,
			"d_following_holiday": pick(g.rng, "Y", "N"),
			"d_first_dom":         int32(1),
			"d_last_dom":          int32(28 + g.rng.IntN(4)),
			"d_same_day_ly":       int32(maxInt(1, i-365)),
			"d_same_day_lq":       int32(maxInt(1, i-90)),
			"d_current_day":       "N",
			"d_current_week":      "N",
			"d_current_month":     "N",
			"d_current_quarter":   "N",
			"d_current_year":      "N",
		}
	}
	return g.buildRecord(DateDim, rows)
}

// GenTimeDim produces the time_dim table — one row per minute of day,
// 1440 rows total.
func (g *Generator) GenTimeDim() arrow.Record {
	const n = 1440
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		h := i / 60
		m := i % 60
		amPm := "AM"
		if h >= 12 {
			amPm = "PM"
		}
		shift := "first"
		if h >= 8 && h < 16 {
			shift = "second"
		} else if h >= 16 {
			shift = "third"
		}
		subShift := "day"
		if h < 6 {
			subShift = "night"
		}
		mealTime := ""
		switch {
		case h >= 6 && h < 9:
			mealTime = "breakfast"
		case h >= 11 && h < 14:
			mealTime = "lunch"
		case h >= 17 && h < 20:
			mealTime = "dinner"
		}
		rows[i] = map[string]any{
			"t_time_sk":   int64(i + 1),
			"t_time_id":   fmt.Sprintf("TTTT%06d", i),
			"t_time":      int32(i * 60),
			"t_hour":      int32(h),
			"t_minute":    int32(m),
			"t_second":    int32(0),
			"t_am_pm":     amPm,
			"t_shift":     shift,
			"t_sub_shift": subShift,
			"t_meal_time": mealTime,
		}
	}
	return g.buildRecord(TimeDim, rows)
}

// GenItem produces the item table — n distinct items.
func (g *Generator) GenItem(n int) arrow.Record {
	if n <= 0 {
		n = NumItems
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		cat := Categories[g.rng.IntN(len(Categories))]
		catID := indexOf(Categories, cat) + 1
		rows[i] = map[string]any{
			"i_item_sk":        int64(i + 1),
			"i_item_id":        fmt.Sprintf("ITEM%08d", i),
			"i_rec_start_date": g.randDate(),
			"i_rec_end_date":   g.randDate(),
			"i_item_desc":      fmt.Sprintf("Description for item %d", i),
			"i_current_price":  round2(1 + g.rng.Float64()*499),
			"i_wholesale_cost": round2(0.5 + g.rng.Float64()*249.5),
			"i_brand_id":       int32(g.rng.IntN(50) + 1),
			"i_brand":          Brands[g.rng.IntN(len(Brands))],
			"i_class_id":       int32(g.rng.IntN(15) + 1),
			"i_class":          fmt.Sprintf("Class %d", g.rng.IntN(15)+1),
			"i_category_id":    int32(catID),
			"i_category":       cat,
			"i_manufact_id":    int32(g.rng.IntN(100) + 1),
			"i_manufact":       fmt.Sprintf("Manufact %d", g.rng.IntN(100)+1),
			"i_size":           Sizes[g.rng.IntN(len(Sizes))],
			"i_formulation":    randStr(g.rng, 10),
			"i_color":          Colors[g.rng.IntN(len(Colors))],
			"i_units":          pick(g.rng, "Each", "Oz", "Lb", "Gram", "Dram"),
			"i_container":      pick(g.rng, "Unknown", "Wrap", "Box", "Bag"),
			"i_manager_id":     int32(g.rng.IntN(50) + 1),
			"i_product_name":   fmt.Sprintf("product_%d", i),
		}
	}
	return g.buildRecord(Item, rows)
}

// GenCustomer produces n customers.
func (g *Generator) GenCustomer(n int) arrow.Record {
	if n <= 0 {
		n = NumCustomers
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"c_customer_sk":          int64(i + 1),
			"c_customer_id":          fmt.Sprintf("CUST%08d", i),
			"c_current_cdemo_sk":     int64(g.rng.IntN(NumCustDemo) + 1),
			"c_current_hdemo_sk":     int64(g.rng.IntN(NumHouseDemo) + 1),
			"c_current_addr_sk":      int64(g.rng.IntN(NumAddresses) + 1),
			"c_first_shipto_date_sk": int64(g.rng.IntN(NumDates) + 1),
			"c_first_sales_date_sk":  int64(g.rng.IntN(NumDates) + 1),
			"c_salutation":           pick(g.rng, "Mr.", "Mrs.", "Ms.", "Dr.", "Sir"),
			"c_first_name":           fmt.Sprintf("First%d", i),
			"c_last_name":            fmt.Sprintf("Last%d", i),
			"c_preferred_cust_flag":  pick(g.rng, "Y", "N"),
			"c_birth_day":            int32(g.rng.IntN(28) + 1),
			"c_birth_month":          int32(g.rng.IntN(12) + 1),
			"c_birth_year":           int32(1940 + g.rng.IntN(66)),
			"c_birth_country":        "US",
			"c_login":                fmt.Sprintf("user%d", i),
			"c_email_address":        fmt.Sprintf("user%d@example.com", i),
			"c_last_review_date_sk":  int64(g.rng.IntN(NumDates) + 1),
		}
	}
	return g.buildRecord(Customer, rows)
}

// GenCustomerAddress produces n addresses.
func (g *Generator) GenCustomerAddress(n int) arrow.Record {
	if n <= 0 {
		n = NumAddresses
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"ca_address_sk":    int64(i + 1),
			"ca_address_id":    fmt.Sprintf("ADDR%08d", i),
			"ca_street_number": fmt.Sprintf("%d", g.rng.IntN(9999)+1),
			"ca_street_name":   fmt.Sprintf("%s St", pick(g.rng, "Oak", "Elm", "Main", "1st", "2nd")),
			"ca_street_type":   pick(g.rng, "St", "Ave", "Blvd", "Dr", "Ln"),
			"ca_suite_number":  fmt.Sprintf("Suite %d", g.rng.IntN(500)+1),
			"ca_city":          Cities[g.rng.IntN(len(Cities))],
			"ca_county":        fmt.Sprintf("%s County", Cities[g.rng.IntN(len(Cities))]),
			"ca_state":         States[g.rng.IntN(len(States))],
			"ca_zip":           fmt.Sprintf("%d", 10000+g.rng.IntN(89999)),
			"ca_country":       "United States",
			"ca_gmt_offset":    pickF(g.rng, -5.0, -6.0, -7.0, -8.0),
			"ca_location_type": pick(g.rng, "single family", "apartment", "condo"),
		}
	}
	return g.buildRecord(CustomerAddress, rows)
}

// GenCustomerDemographics produces the cartesian product of (gender,
// marital, education, credit, 8 reps) → 1920 rows.
func (g *Generator) GenCustomerDemographics() arrow.Record {
	rows := make([]map[string]any, 0, NumCustDemo)
	sk := 1
	for _, gender := range Genders {
		for _, mar := range Marital {
			for _, edu := range Education {
				for _, cr := range Credit {
					for rep := 0; rep < 8; rep++ {
						rows = append(rows, map[string]any{
							"cd_demo_sk":            int64(sk),
							"cd_gender":             gender,
							"cd_marital_status":     mar,
							"cd_education_status":   edu,
							"cd_purchase_estimate":  int32(g.rng.IntN(9501) + 500),
							"cd_credit_rating":      cr,
							"cd_dep_count":          int32(g.rng.IntN(7)),
							"cd_dep_employed_count": int32(g.rng.IntN(7)),
							"cd_dep_college_count":  int32(g.rng.IntN(7)),
						})
						sk++
					}
				}
			}
		}
	}
	return g.buildRecord(CustomerDemographics, rows)
}

// GenHouseholdDemographics produces 7200 household rows.
func (g *Generator) GenHouseholdDemographics() arrow.Record {
	rows := make([]map[string]any, NumHouseDemo)
	for i := 0; i < NumHouseDemo; i++ {
		rows[i] = map[string]any{
			"hd_demo_sk":        int64(i + 1),
			"hd_income_band_sk": int64(g.rng.IntN(20) + 1),
			"hd_buy_potential":  pick(g.rng, "Unknown", "0-500", "501-1000", "1001-5000", "5001-10000", ">10000"),
			"hd_dep_count":      int32(g.rng.IntN(10)),
			"hd_vehicle_count":  int32(g.rng.IntN(5)),
		}
	}
	return g.buildRecord(HouseholdDemographics, rows)
}

// GenStore produces n stores.
func (g *Generator) GenStore(n int) arrow.Record {
	if n <= 0 {
		n = NumStores
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"s_store_sk":         int64(i + 1),
			"s_store_id":         fmt.Sprintf("STORE%06d", i),
			"s_rec_start_date":   g.randDate(),
			"s_rec_end_date":     g.randDate(),
			"s_closed_date_sk":   int64(0),
			"s_store_name":       fmt.Sprintf("Store %d", i),
			"s_number_employees": int32(50 + g.rng.IntN(451)),
			"s_floor_space":      int32(5000 + g.rng.IntN(95001)),
			"s_hours":            Hours[g.rng.IntN(len(Hours))],
			"s_manager":          fmt.Sprintf("Manager %d", i),
			"s_market_id":        int32(g.rng.IntN(10) + 1),
			"s_geography_class":  "medium",
			"s_market_desc":      fmt.Sprintf("Market desc %d", i),
			"s_market_manager":   fmt.Sprintf("MktMgr %d", i),
			"s_division_id":      int32(g.rng.IntN(5) + 1),
			"s_division_name":    fmt.Sprintf("Div %d", g.rng.IntN(5)+1),
			"s_company_id":       int32(g.rng.IntN(3) + 1),
			"s_company_name":     fmt.Sprintf("Company %d", g.rng.IntN(3)+1),
			"s_street_number":   fmt.Sprintf("%d", g.rng.IntN(999)+1),
			"s_street_name":     fmt.Sprintf("Store St %d", i),
			"s_street_type":     "Ave",
			"s_suite_number":    "",
			"s_city":            Cities[g.rng.IntN(len(Cities))],
			"s_county":          fmt.Sprintf("%s County", Cities[g.rng.IntN(len(Cities))]),
			"s_state":           States[g.rng.IntN(len(States))],
			"s_zip":             fmt.Sprintf("%d", 10000+g.rng.IntN(89999)),
			"s_country":         "United States",
			"s_gmt_offset":      -5.0,
			"s_tax_percentage":  round2(g.rng.Float64() * 0.12),
		}
	}
	return g.buildRecord(Store, rows)
}

// GenPromotion produces n promotions. The Python implementation uses a
// generic small-dim generator; we mirror its row shape.
func (g *Generator) GenPromotion(n int) arrow.Record {
	if n <= 0 {
		n = NumPromotions
	}
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{
			"p_promo_sk":         int64(i + 1),
			"p_promo_id":         fmt.Sprintf("PROM%06d", i),
			"p_start_date_sk":    int64(g.rng.IntN(NumDates) + 1),
			"p_end_date_sk":      int64(g.rng.IntN(NumDates) + 1),
			"p_item_sk":          int64(g.rng.IntN(NumItems) + 1),
			"p_cost":             round2(g.rng.Float64() * 1000),
			"p_response_target":  int32(g.rng.IntN(100) + 1),
			"p_promo_name":       fmt.Sprintf("Promo %d", i),
			"p_channel_dmail":    pick(g.rng, "Y", "N"),
			"p_channel_email":    pick(g.rng, "Y", "N"),
			"p_channel_catalog":  pick(g.rng, "Y", "N"),
			"p_channel_tv":       pick(g.rng, "Y", "N"),
			"p_channel_radio":    pick(g.rng, "Y", "N"),
			"p_channel_press":    pick(g.rng, "Y", "N"),
			"p_channel_event":    pick(g.rng, "Y", "N"),
			"p_channel_demo":     pick(g.rng, "Y", "N"),
			"p_channel_details":  fmt.Sprintf("Channel details %d", i),
			"p_purpose":          "Marketing",
			"p_discount_active":  pick(g.rng, "Y", "N"),
		}
	}
	return g.buildRecord(Promotion, rows)
}

// === Fact-batch generators ===

// GenStoreSalesBatch produces one micro-batch of store_sales rows.
func (g *Generator) GenStoreSalesBatch(batch int) arrow.Record {
	if batch <= 0 {
		batch = 500
	}
	rows := make([]map[string]any, batch)
	for i := 0; i < batch; i++ {
		qty := g.rng.IntN(100) + 1
		price := round2(1 + g.rng.Float64()*499)
		wholesale := round2(price * (0.3 + g.rng.Float64()*0.4))
		discount := round2(price * g.rng.Float64() * 0.3)
		tax := round2(price * float64(qty) * (0.05 + g.rng.Float64()*0.07))
		net := round2((price - discount) * float64(qty))
		rows[i] = map[string]any{
			"ss_sold_date_sk":       int64(g.rng.IntN(NumDates) + 1),
			"ss_sold_time_sk":       int64(g.rng.IntN(1440) + 1),
			"ss_item_sk":            int64(g.rng.IntN(NumItems) + 1),
			"ss_customer_sk":        int64(g.rng.IntN(NumCustomers) + 1),
			"ss_cdemo_sk":           int64(g.rng.IntN(NumCustDemo) + 1),
			"ss_hdemo_sk":           int64(g.rng.IntN(NumHouseDemo) + 1),
			"ss_addr_sk":            int64(g.rng.IntN(NumAddresses) + 1),
			"ss_store_sk":           int64(g.rng.IntN(NumStores) + 1),
			"ss_promo_sk":           int64(g.rng.IntN(NumPromotions) + 1),
			"ss_ticket_number":      int64(g.rng.IntN(1000000) + 1),
			"ss_quantity":           int32(qty),
			"ss_wholesale_cost":     wholesale,
			"ss_list_price":         price,
			"ss_sales_price":        round2(price - discount),
			"ss_ext_discount_amt":   round2(discount * float64(qty)),
			"ss_ext_sales_price":    net,
			"ss_ext_wholesale_cost": round2(wholesale * float64(qty)),
			"ss_ext_list_price":     round2(price * float64(qty)),
			"ss_ext_tax":            tax,
			"ss_coupon_amt":         round2(g.rng.Float64() * 50),
			"ss_net_paid":           net,
			"ss_net_paid_inc_tax":   round2(net + tax),
			"ss_net_profit":         round2((price - discount - wholesale) * float64(qty)),
		}
	}
	return g.buildRecord(StoreSales, rows)
}

// GenStoreReturnsBatch produces one micro-batch of store_returns rows.
func (g *Generator) GenStoreReturnsBatch(batch int) arrow.Record {
	if batch <= 0 {
		batch = 100
	}
	rows := make([]map[string]any, batch)
	for i := 0; i < batch; i++ {
		qty := g.rng.IntN(10) + 1
		amt := round2(5 + g.rng.Float64()*495)
		tax := round2(amt * 0.08)
		rows[i] = map[string]any{
			"sr_returned_date_sk":   int64(g.rng.IntN(NumDates) + 1),
			"sr_return_time_sk":     int64(g.rng.IntN(1440) + 1),
			"sr_item_sk":            int64(g.rng.IntN(NumItems) + 1),
			"sr_customer_sk":        int64(g.rng.IntN(NumCustomers) + 1),
			"sr_cdemo_sk":           int64(g.rng.IntN(NumCustDemo) + 1),
			"sr_hdemo_sk":           int64(g.rng.IntN(NumHouseDemo) + 1),
			"sr_addr_sk":            int64(g.rng.IntN(NumAddresses) + 1),
			"sr_store_sk":           int64(g.rng.IntN(NumStores) + 1),
			"sr_reason_sk":          int64(g.rng.IntN(35) + 1),
			"sr_ticket_number":      int64(g.rng.IntN(1000000) + 1),
			"sr_return_quantity":    int32(qty),
			"sr_return_amt":         amt,
			"sr_return_tax":         tax,
			"sr_return_amt_inc_tax": round2(amt + tax),
			"sr_fee":                round2(g.rng.Float64() * 10),
			"sr_return_ship_cost":   round2(g.rng.Float64() * 25),
			"sr_refunded_cash":      round2(amt * 0.7),
			"sr_reversed_charge":    round2(amt * 0.2),
			"sr_store_credit":       round2(amt * 0.1),
			"sr_net_loss":           round2(amt - amt*0.7),
		}
	}
	return g.buildRecord(StoreReturns, rows)
}

// GenCatalogSalesBatch produces one micro-batch of catalog_sales rows.
func (g *Generator) GenCatalogSalesBatch(batch int) arrow.Record {
	if batch <= 0 {
		batch = 300
	}
	rows := make([]map[string]any, batch)
	for i := 0; i < batch; i++ {
		qty := g.rng.IntN(50) + 1
		price := round2(5 + g.rng.Float64()*995)
		wholesale := round2(price * (0.3 + g.rng.Float64()*0.3))
		discount := round2(price * g.rng.Float64() * 0.25)
		tax := round2(price * float64(qty) * 0.08)
		ship := round2(3 + g.rng.Float64()*22)
		net := round2((price - discount) * float64(qty))
		rows[i] = map[string]any{
			"cs_sold_date_sk":          int64(g.rng.IntN(NumDates) + 1),
			"cs_sold_time_sk":          int64(g.rng.IntN(1440) + 1),
			"cs_ship_date_sk":          int64(g.rng.IntN(NumDates) + 1),
			"cs_bill_customer_sk":      int64(g.rng.IntN(NumCustomers) + 1),
			"cs_bill_cdemo_sk":         int64(g.rng.IntN(NumCustDemo) + 1),
			"cs_bill_hdemo_sk":         int64(g.rng.IntN(NumHouseDemo) + 1),
			"cs_bill_addr_sk":          int64(g.rng.IntN(NumAddresses) + 1),
			"cs_ship_customer_sk":      int64(g.rng.IntN(NumCustomers) + 1),
			"cs_ship_cdemo_sk":         int64(g.rng.IntN(NumCustDemo) + 1),
			"cs_ship_hdemo_sk":         int64(g.rng.IntN(NumHouseDemo) + 1),
			"cs_ship_addr_sk":          int64(g.rng.IntN(NumAddresses) + 1),
			"cs_call_center_sk":        int64(g.rng.IntN(10) + 1),
			"cs_catalog_page_sk":       int64(g.rng.IntN(500) + 1),
			"cs_ship_mode_sk":          int64(g.rng.IntN(5) + 1),
			"cs_warehouse_sk":          int64(g.rng.IntN(10) + 1),
			"cs_item_sk":               int64(g.rng.IntN(NumItems) + 1),
			"cs_promo_sk":              int64(g.rng.IntN(NumPromotions) + 1),
			"cs_order_number":          int64(g.rng.IntN(1000000) + 1),
			"cs_quantity":              int32(qty),
			"cs_wholesale_cost":        wholesale,
			"cs_list_price":            price,
			"cs_sales_price":           round2(price - discount),
			"cs_ext_discount_amt":      round2(discount * float64(qty)),
			"cs_ext_sales_price":       net,
			"cs_ext_wholesale_cost":    round2(wholesale * float64(qty)),
			"cs_ext_list_price":        round2(price * float64(qty)),
			"cs_ext_tax":               tax,
			"cs_coupon_amt":            round2(g.rng.Float64() * 30),
			"cs_ext_ship_cost":         round2(ship * float64(qty)),
			"cs_net_paid":              net,
			"cs_net_paid_inc_tax":      round2(net + tax),
			"cs_net_paid_inc_ship":     round2(net + ship),
			"cs_net_paid_inc_ship_tax": round2(net + ship + tax),
			"cs_net_profit":            round2((price - discount - wholesale) * float64(qty)),
		}
	}
	return g.buildRecord(CatalogSales, rows)
}

// === Helpers ===

// buildRecord materializes a slice of map[string]any into an
// arrow.Record matching the iceberg schema. Field order is taken from
// the iceberg schema; missing keys are emitted as nulls. Type
// conversion is per the schema's primitive types.
func (g *Generator) buildRecord(sc *icebergpkg.Schema, rows []map[string]any) arrow.Record {
	arrowSchema, err := icebergtable.SchemaToArrowSchema(sc, nil, false, false)
	if err != nil {
		panic(fmt.Sprintf("schema conversion: %v", err))
	}
	bldr := array.NewRecordBuilder(g.pool, arrowSchema)
	defer bldr.Release()

	for _, row := range rows {
		for fi, field := range arrowSchema.Fields() {
			val, ok := row[field.Name]
			if !ok || val == nil {
				bldr.Field(fi).AppendNull()
				continue
			}
			switch b := bldr.Field(fi).(type) {
			case *array.Int32Builder:
				b.Append(val.(int32))
			case *array.Int64Builder:
				b.Append(val.(int64))
			case *array.Float64Builder:
				b.Append(val.(float64))
			case *array.StringBuilder:
				b.Append(val.(string))
			case *array.Date32Builder:
				if t, ok := val.(time.Time); ok {
					days := int32(t.Sub(BaseDate.Truncate(24*time.Hour)).Hours() / 24)
					b.Append(arrow.Date32(days + epochDateOffset()))
				} else {
					b.AppendNull()
				}
			default:
				bldr.Field(fi).AppendNull()
			}
		}
	}
	return bldr.NewRecord()
}

// epochDateOffset is the number of days between Unix epoch and the
// generator's BaseDate. Pre-computed once.
func epochDateOffset() int32 {
	return int32(BaseDate.Sub(time.Unix(0, 0)).Hours() / 24)
}

// randDate returns a random date in the date_dim range as a time.Time
// at midnight UTC.
func (g *Generator) randDate() time.Time {
	return BaseDate.AddDate(0, 0, g.rng.IntN(NumDates))
}

func pick(r *rand.Rand, opts ...string) string {
	return opts[r.IntN(len(opts))]
}

func pickF(r *rand.Rand, opts ...float64) float64 {
	return opts[r.IntN(len(opts))]
}

func randStr(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, n)
	for i := range out {
		out[i] = letters[r.IntN(len(letters))]
	}
	return string(out)
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func indexOf(slice []string, want string) int {
	for i, s := range slice {
		if s == want {
			return i
		}
	}
	return 0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
