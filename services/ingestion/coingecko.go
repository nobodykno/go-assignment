package ingestion

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// coinGeckoMarket represents one entry returned by CoinGecko's
// /api/v3/coins/markets endpoint.
type coinGeckoMarket struct {
	ID           string   `json:"id"`
	Symbol       string   `json:"symbol"`
	Name         string   `json:"name"`
	CurrentPrice *float64 `json:"current_price"`
	LastUpdated  string   `json:"last_updated"`
}

// ParseCoinGecko converts CoinGecko market data into normalized records.
//
// Expected response: an array returned by /coins/markets with
// vs_currency=usd. Each record contains date, coin, price_usd,
// symbol, and name.
func ParseCoinGecko(body []byte) ([]RawRecord, error) {
	var markets []coinGeckoMarket

	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf(
			"decode CoinGecko market response: %w",
			err,
		)
	}

	if len(markets) == 0 {
		return nil, fmt.Errorf(
			"CoinGecko response contains no market records",
		)
	}

	records := make([]RawRecord, 0, len(markets))

	for i, market := range markets {
		recordNumber := i + 1

		coin := strings.TrimSpace(market.ID)
		if coin == "" {
			return nil, fmt.Errorf(
				"CoinGecko record %d has no coin ID",
				recordNumber,
			)
		}

		if market.CurrentPrice == nil {
			return nil, fmt.Errorf(
				"CoinGecko record %d has no current price",
				recordNumber,
			)
		}

		price := *market.CurrentPrice
		if price <= 0 {
			return nil, fmt.Errorf(
				"CoinGecko record %d has an invalid price",
				recordNumber,
			)
		}

		lastUpdated := strings.TrimSpace(market.LastUpdated)
		if lastUpdated == "" {
			return nil, fmt.Errorf(
				"CoinGecko record %d has no last_updated timestamp",
				recordNumber,
			)
		}

		updatedAt, err := time.Parse(time.RFC3339Nano, lastUpdated)
		if err != nil {
			return nil, fmt.Errorf(
				"CoinGecko record %d has an invalid last_updated timestamp: %w",
				recordNumber,
				err,
			)
		}

		records = append(records, RawRecord{
			"date": updatedAt.UTC().Format("2006-01-02"),
			"coin": coin,
			"price_usd": strconv.FormatFloat(
				price,
				'f',
				-1,
				64,
			),
			"symbol": strings.TrimSpace(market.Symbol),
			"name":   strings.TrimSpace(market.Name),
		})
	}

	return records, nil
}
