package result

import (
	"fmt"
	"strconv"
	"strings"
)

type ResultQuery struct {
	QueryType int
	Result    string
}

func NewResultQuery(queryType int, result string) *ResultQuery {
	return &ResultQuery{
		QueryType: queryType,
		Result:    result,
	}
}

func (r *ResultQuery) AppendResult(result string) {
	r.Result += "\n" + result
}

func (r *ResultQuery) PrintQuery() {
	if r.Result == "" {
		fmt.Println("La query no tiene resultado")
	}
	fmt.Printf("| Query %d |\n %s\n", r.QueryType, r.Result)
}

// ParseToMap parses the result string into a map based on query type
func (r *ResultQuery) ParseToMap() map[string]interface{} {
	switch r.QueryType {
	case 1:
		return r.parseQuery1()
	case 2:
		return r.parseQuery2()
	case 3:
		return r.parseQuery3()
	case 4:
		return r.parseQuery4()
	case 5:
		return r.parseQuery5()
	default:
		return make(map[string]interface{})
	}
}

// parseQuery1 parses movie names and genres
// Example: "La Cienaga","['Comedy', 'Drama']"
func (r *ResultQuery) parseQuery1() map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(r.Result, "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		// Find the position of the first and second quote
		parts := strings.Split(line, "\",\"")
		if len(parts) < 2 {
			continue
		}

		// Clean up the keys and values
		movieName := strings.Trim(parts[0], "\"")
		genresStr := strings.Trim(parts[1], "\"")

		// Parse the genres array
		genresStr = strings.Trim(genresStr, "[]")
		genres := []string{}

		// Split by comma and clean up each genre
		if genresStr != "" {
			genreParts := strings.Split(genresStr, ", ")
			for _, genre := range genreParts {
				// Remove the quotes around each genre
				cleanGenre := strings.Trim(genre, "'")
				genres = append(genres, cleanGenre)
			}
		}

		result[movieName] = genres
	}

	return result
}

// parseQuery2 parses countries and box office
// Example: "United States of America","120153886644"
func (r *ResultQuery) parseQuery2() map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(r.Result, "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\",\"")
		if len(parts) < 2 {
			continue
		}

		country := strings.Trim(parts[0], "\"")
		revenueStr := strings.Trim(parts[1], "\"")

		// Convert revenue to number
		revenue, err := strconv.ParseFloat(revenueStr, 64)
		if err != nil {
			// If parsing fails, store as string
			result[country] = revenueStr
		} else {
			result[country] = revenue
		}
	}

	return result
}

// parseQuery3 parses movies and ratings
// Example: "The forbidden education","4.0"
func (r *ResultQuery) parseQuery3() map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(r.Result, "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\",\"")
		if len(parts) < 2 {
			continue
		}

		movie := strings.Trim(parts[0], "\"")
		ratingStr := strings.Trim(parts[1], "\"")

		// Convert rating to float
		rating, err := strconv.ParseFloat(ratingStr, 64)
		if err != nil {
			// If parsing fails, store as string
			result[movie] = ratingStr
		} else {
			result[movie] = rating
		}
	}

	return result
}

// parseQuery4 parses actors and movie counts
// Example: "Ricardo Darín","17"
func (r *ResultQuery) parseQuery4() map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(r.Result, "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\",\"")
		if len(parts) < 2 {
			continue
		}

		actor := strings.Trim(parts[0], "\"")
		countStr := strings.Trim(parts[1], "\"")

		// Convert count to integer
		count, err := strconv.Atoi(countStr)
		if err != nil {
			// If parsing fails, store as string
			result[actor] = countStr
		} else {
			result[actor] = count
		}
	}

	return result
}

// parseQuery5 parses sentiment and scores
// Example: "POSITIVE","5703.6952437095715"
func (r *ResultQuery) parseQuery5() map[string]interface{} {
	result := make(map[string]interface{})
	lines := strings.Split(r.Result, "\n")

	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\",\"")
		if len(parts) < 2 {
			continue
		}

		sentiment := strings.Trim(parts[0], "\"")
		scoreStr := strings.Trim(parts[1], "\"")

		// Convert score to float
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			// If parsing fails, store as string
			result[sentiment] = scoreStr
		} else {
			result[sentiment] = score
		}
	}

	return result
}
