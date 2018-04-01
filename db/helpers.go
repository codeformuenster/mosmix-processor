package db

import (
	"math"
	"strings"
)

func extractID(key string) string {
	return strings.Split(key, ":")[1]
}

const rEarth = 63728e2 // m
// distance calculates the distance between the given points. Coordinate order
// should be lat,lon
// code from rosettacode.org
func distance(p0, p1 []float64) float64 {
	radianLat0 := p0[0] * math.Pi / 180
	radianLon0 := p0[1] * math.Pi / 180
	radianLat1 := p1[0] * math.Pi / 180
	radianLon1 := p1[1] * math.Pi / 180

	haversineLat := 0.5 * (1 - math.Cos(radianLat1-radianLat0))
	haversineLon := 0.5 * (1 - math.Cos(radianLon1-radianLon0))

	return 2 * rEarth *
		math.Asin(math.Sqrt(haversineLat+math.Cos(radianLat0)*math.Cos(radianLat1)*
			haversineLon))
}
