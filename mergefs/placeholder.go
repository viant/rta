package mergefs

import (
	"context"
	"errors"
	rconfig "github.com/viant/rta/config"
	"github.com/viant/rta/mergefs/config"
	"github.com/viant/sqlx/io/read"
	"sort"
)

type Placeholder struct {
	Name string `sqlx:"NAME"`
}

func ensurePlaceholders(c *config.Config) ([]string, error) {
	placeholders1 := []string{}
	placeholders2 := []string{}

	if c.DestPlaceholders != nil && len(c.DestPlaceholders.Placeholders) > 0 {
		placeholders1 = c.DestPlaceholders.Placeholders
	}

	if c.DestPlaceholders.Connection != nil && c.DestPlaceholders.Query != "" {
		res, err := readPlaceholders(c.DestPlaceholders.Connection, c.DestPlaceholders.Query)
		if err != nil {
			return nil, err
		}
		placeholders2 = res
	}

	result := mergeAndRemoveDuplicatesOrdered(placeholders1, placeholders2)
	return result, nil
}

func readPlaceholders(conn *rconfig.Connection, query string) (result []string, err error) {
	ctx := context.Background()
	db, err := conn.OpenDB(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { err = errors.Join(err, db.Close()) }()

	newFn := func() interface{} { return &Placeholder{} }
	reader, err := read.New(ctx, db, query, newFn)
	if err != nil {
		return nil, err
	}

	result = []string{}
	emitFn := func(row interface{}) error {
		res := row.(*Placeholder)
		result = append(result, res.Name)
		return nil
	}

	err = reader.QueryAll(ctx, emitFn)
	if err != nil {
		return nil, err
	}

	return result, err
}

func mergeAndRemoveDuplicatesOrdered(slice1, slice2 []string) []string {
	seen := make(map[string]bool)
	result := []string{}

	for _, val := range slice1 {
		if !seen[val] {
			seen[val] = true
			result = append(result, val)
		}
	}

	for _, val := range slice2 {
		if !seen[val] {
			seen[val] = true
			result = append(result, val)
		}
	}

	sort.Strings(result)

	return result
}

// difference returns a new slice containing all elements
// from slice1 that are not present in slice2.
func difference(slice1, slice2 []string) []string {
	set2 := make(map[string]bool, len(slice2))

	for _, v := range slice2 {
		set2[v] = true
	}

	var result []string
	for _, v := range slice1 {
		if _, found := set2[v]; !found {
			result = append(result, v)
		}
	}
	return result
}
