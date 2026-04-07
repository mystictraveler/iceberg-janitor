package tpcds

import "time"

// TPC-DS scale-related constants. Match the values in
// scripts/tpcds_datagen.py so the Go and Python benchmarks generate
// the same logical row counts.
const (
	NumDates       = 2190 // ~6 years of dates
	NumTimes       = 1440 // one row per minute
	NumItems       = 5000
	NumCustomers   = 20000
	NumAddresses   = 15000
	NumStores      = 50
	NumPromotions  = 200
	NumCustDemo    = 1920 // 2 genders × 5 marital × 6 education × 4 credit × 8 rep = 1920
	NumHouseDemo   = 7200
)

// BaseDate is the day-1 date the date_dim starts from. The Python
// generator uses 2020-01-01.
var BaseDate = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// Pools used by the dimension generators.
var (
	States = []string{
		"CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
		"NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
	}
	Cities = []string{
		"Springfield", "Franklin", "Clinton", "Madison", "Georgetown",
		"Salem", "Bristol", "Fairview", "Riverside", "Greenville",
	}
	Genders   = []string{"M", "F"}
	Marital   = []string{"S", "M", "D", "W", "U"}
	Education = []string{
		"Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree",
		"Advanced Degree",
	}
	Credit = []string{"Low Risk", "Medium Risk", "High Risk", "Unknown"}
	Colors = []string{
		"red", "blue", "green", "yellow", "white", "black", "brown",
		"orange", "purple", "pink",
	}
	Sizes      = []string{"small", "medium", "large", "extra large", "petite", "economy", "N/A"}
	Categories = []string{
		"Electronics", "Books", "Music", "Home", "Sports", "Shoes",
		"Jewelry", "Children", "Men", "Women",
	}
	Brands = func() []string {
		out := make([]string, 50)
		for i := 0; i < 50; i++ {
			out[i] = "Brand #" + itoa(i+1)
		}
		return out
	}()
	Hours = []string{"8AM-4PM", "4PM-12AM", "12AM-8AM"}
)

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}
